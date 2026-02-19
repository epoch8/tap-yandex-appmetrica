"""YandexAppmetrica tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_yandex_appmetrica import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapYandexAppmetrica(Tap):
    """Singer tap for YandexAppmetrica."""

    name = "tap-yandex-appmetrica"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "application_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "chunk_days",
            th.IntegerType,
            default=30,
        ),
        th.Property(
            "limit",
            th.StringType,
        ),
        th.Property(
            "retro_interval_days",
            th.IntegerType,
            default=0
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.YandexAppmetricaStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EventsStream(self),
            streams.InstallationsStream(self),
            streams.InstallDevicesStream(self),
        ]


if __name__ == "__main__":
    TapYandexAppmetrica.cli()
