"""REST client handling, including YandexAppmetricaStream base class."""

from __future__ import annotations

import ctypes
import decimal
import gc
import sys
import csv
from typing import TYPE_CHECKING, Any, ClassVar, Callable, Iterable, Generator

import datetime
import pendulum
import requests
import backoff

from singer_sdk import metrics
from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.helpers._util import utc_now

from tap_yandex_appmetrica import schemas

if sys.version_info >= (3, 12):
    from typing import override
    from cached_property import cached_property
else:
    from typing_extensions import override
    from functools import cached_property

if TYPE_CHECKING:
    from collections.abc import Iterable
    from singer_sdk.helpers.types import Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = SchemaDirectory(schemas)

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

# See https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
csv.field_size_limit(sys.maxsize)

_libc = None
if sys.platform.startswith("linux"):
    try:
        _libc = ctypes.CDLL("libc.so.6")
    except OSError:
        _libc = None


def _release_memory_to_os() -> None:
    """Return freed heap memory to the OS after a chunk finishes.

    Chunked, high-churn CSV parsing (millions of short-lived row dicts per
    date-chunk) tends to leave glibc's allocator holding onto freed memory
    as fragmented arenas rather than releasing it back to the OS. In a
    memory-limited container that shows up as RSS creeping up across many
    chunks in a single run until it gets OOMKilled, even though no Python
    objects are actually leaking. Forcing a GC pass plus malloc_trim at
    each chunk boundary keeps peak RSS closer to a single chunk's size.
    """
    gc.collect()
    if _libc is not None:
        _libc.malloc_trim(0)


def _current_rss_mb() -> float | None:
    """Return this process's *current* resident set size, in MB.

    Unlike ``resource.getrusage().ru_maxrss`` (a high-water mark that only
    ever increases for the life of the process), this reflects memory used
    at the moment it's read, which is what's actually needed to tell a real
    accumulation apart from a large-but-transient per-chunk peak.
    """
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


class YandexAppmetricaStream(RESTStream):
    """Appmetrica stream class."""

    _LOG_REQUEST_METRIC_URLS = True

    url_base = "https://api.appmetrica.yandex.ru"

    extra_retry_statuses = [202] + RESTStream.extra_retry_statuses
    
    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.

        Returns:
            Dictionary of HTTP headers to use as a base for every request.
        """
        return {
            "User-Agent": self.user_agent,
            **self._http_headers,
            "Authorization": f"OAuth {self.config['token']}",
        }

    def backoff_wait_generator(self) -> Generator[float, None, None]:
        return backoff.constant(120)

    def backoff_max_tries(self) -> int:
        return 100

    def _chunk_interval(self) -> datetime.timedelta:
        """Return the date_since/date_until window size for one HTTP request.

        If ``chunk_hours`` is set, it takes precedence over ``chunk_days`` and
        the backlog is walked in windows of that many hours instead — lets a
        deployment tune the tradeoff between per-request memory (smaller
        windows) and request/backoff overhead (larger windows) for streams
        whose daily volume is too large for a full day per request.
        """
        if (chunk_hours := self.config.get("chunk_hours")) is not None:
            return datetime.timedelta(hours=chunk_hours)
        return datetime.timedelta(days=self.config["chunk_days"])

    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
            self._requests_session.stream = True
        return self._requests_session

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            An item for every record in the response.
        """

        page_date = pendulum.parse(self.get_starting_replication_key_value(context))

        if (retro_interval_days := self.config.get("retro_interval_days")) != 0:
            page_date = page_date.subtract(days=retro_interval_days)
            page_date = page_date.set(hour=0, minute=0, second=0, microsecond=0)

        chunk_interval = self._chunk_interval()
        if self.config.get("chunk_hours") is not None:
            # State (and the date_since/date_until windows built from it) must
            # stay hour-truncated so hour-sized windows never drift off clean
            # hour boundaries because of a sub-hour bookmark value carried
            # over from an exact record timestamp.
            page_date = page_date.set(minute=0, second=0, microsecond=0)

        decorated_request = self.request_decorator(self._request)

        now = utc_now()

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while page_date < now:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=page_date,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                row_count = 0
                try:
                    for record in self.parse_response(resp):
                        row_count += 1
                        if row_count % 50_000 == 0:
                            self.logger.info(
                                "'%s': %d rows into chunk starting %s, current RSS=%s MB",
                                self.name,
                                row_count,
                                page_date.isoformat(),
                                _current_rss_mb(),
                            )
                        yield record
                finally:
                    resp.close()

                self.finalize_state_progress_markers()
                self._write_state_message()
                page_date += chunk_interval
                _release_memory_to_os()
                self.logger.info(
                    "Chunk done for '%s': date_until=%s, rows=%d, current RSS=%s MB",
                    self.name,
                    page_date.isoformat(),
                    row_count,
                    _current_rss_mb(),
                )

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        assert next_page_token is not None

        params["application_id"] = self.config["application_id"]

        params["date_dimension"] = "receive"
        params["date_since"] = next_page_token.strftime("%Y-%m-%d %H:%M:%S")
        params["date_until"] = (
            next_page_token + self._chunk_interval()
        ).strftime("%Y-%m-%d %H:%M:%S")

        if (limit := self.config.get("limit")) is not None:
            params["limit"] = limit

        params["fields"] = ",".join(self.fields)

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from reader


class YandexAppmetricaStatStream(RESTStream):
    url_base = "https://api.appmetrica.yandex.ru/stat/v1/data"
    records_jsonpath = "$.data[*]"

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests.

        If an authenticator is also specified, the authenticator's headers will be
        combined with `http_headers` when making HTTP requests.

        Returns:
            Dictionary of HTTP headers to use as a base for every request.
        """
        return {
            "User-Agent": self.user_agent,
            **self._http_headers,
            "Authorization": f"OAuth {self.config['token']}",
        }

    @property
    def get_metrics(self) -> str:
        return ""

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """

        if (
            replication_key_value := self.get_starting_replication_key_value(
                context=context
            )
        ) is not None:
            replication_key_value = pendulum.parse(replication_key_value.split()[0])

            if (retro_interval_days := self.config.get("retro_interval_days")) != 0:
                replication_key_value = replication_key_value.subtract(
                    days=retro_interval_days
                )

            start_date = self.compare_start_date(
                value=replication_key_value.strftime("%Y-%m-%d"),
                start_date_value=self.config["start_date"].split()[0],
            )
        else:
            start_date = self.config["start_date"].split()[0]

        params: dict = {
            "id": self.config["application_id"],
            "metrics": self.get_metrics,
            "dimensions": "ym:i:date",
            "date1": start_date,
            "group": "day",
        }
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
