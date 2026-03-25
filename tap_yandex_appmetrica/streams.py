"""Stream type classes for tap-yandex-appmetrica."""

from __future__ import annotations

import csv
import requests
import pendulum
from pendulum.exceptions import ParserError
from typing import Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_yandex_appmetrica.client import YandexAppmetricaStream
from tap_yandex_appmetrica.client import YandexAppmetricaStatStream


def is_valid_datetime(date_string: str):
    try:
        pendulum.parse(date_string)
        return True
    except ParserError:
        return False
    

class EventsStream(YandexAppmetricaStream):
    name = "events"
    path = "/logs/v1/export/events.csv"

    primary_keys = None
    replication_key = "event_receive_datetime"

    fields = (
        "event_datetime",
        "event_json",
        "event_name",
        "event_receive_datetime",
        "event_receive_timestamp",
        "event_timestamp",
        "session_id",
        "installation_id",
        "appmetrica_device_id",
        "city",
        "connection_type",
        "country_iso_code",
        "device_ipv6",
        "device_locale",
        "device_manufacturer",
        "device_model",
        "device_type",
        "google_aid",
        "ios_ifa",
        "ios_ifv",
        "mcc",
        "mnc",
        "operator_name",
        "original_device_model",
        "os_name",
        "os_version",
        "profile_id",
        "windows_aid",
        "app_build_number",
        "app_package_name",
        "app_version_name",
        "application_id",
    )

    schema = th.PropertiesList(
        *[th.Property(i, th.StringType) for i in fields]
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        response.encoding = 'utf-8'
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from (
            obj
            for obj in reader
            if obj.get("event_receive_datetime")
            and is_valid_datetime(obj.get("event_receive_datetime"))
        )


class InstallationsStream(YandexAppmetricaStream):
    name = "installations"
    path = "/logs/v1/export/installations.csv"

    primary_keys = None
    replication_key = "install_receive_datetime"

    fields = [
        "application_id",
        "click_datetime",
        "click_id",
        "click_ipv6",
        "click_timestamp",
        "click_url_parameters",
        "click_user_agent",
        "profile_id",
        "publisher_id",
        "publisher_name",
        "tracker_name",
        "tracking_id",
        "install_datetime",
        "install_ipv6",
        "install_receive_datetime",
        "install_receive_timestamp",
        "install_timestamp",
        "is_reattribution",
        "is_reinstallation",
        "match_type",
        "appmetrica_device_id",
        "city",
        "connection_type",
        "country_iso_code",
        "device_locale",
        "device_manufacturer",
        "device_model",
        "device_type",
        "google_aid",
        "ios_ifa",
        "ios_ifv",
        "mcc",
        "mnc",
        "operator_name",
        "os_name",
        "os_version",
        "windows_aid",
        "app_package_name",
        "app_version_name",
    ]

    schema = th.PropertiesList(
        *[th.Property(i, th.StringType) for i in fields]
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        response.encoding = 'utf-8'
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from (
            obj
            for obj in reader
            if obj.get("install_receive_datetime")
            and is_valid_datetime(obj.get("install_receive_datetime"))
        )


class InstallDevicesStream(YandexAppmetricaStatStream):
    name = "install_devices"
    path = ""

    primary_keys = ["date"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("install_devices", th.NumberType)
    ).to_dict()

    @property
    def get_metrics(self) -> str:
        return 'ym:i:installDevices'

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
        row: dict = {
            "date": row["dimensions"][0]["name"],
            "install_devices": row["metrics"][0]
        }
        return row


class DeeplinkStream(YandexAppmetricaStream):
    name = "deeplink"
    # path = "/logs/v1/export/deeplinks.json"
    path = "/logs/v1/export/deeplinks.csv"
    # records_jsonpath = "$.data[*]"

    primary_keys = ["session_id"]
    replication_key = "event_datetime"

    fields = [
        "deeplink_url_host",
        "deeplink_url_parameters",
        "deeplink_url_path",
        "deeplink_url_scheme",
        "event_datetime",
        "event_receive_datetime",
        "event_receive_timestamp",
        "event_timestamp",
        "is_reengagement",
        "profile_id",
        "publisher_id",
        "publisher_name",
        "session_id",
        "tracker_name",
        "tracking_id",
        "android_id",
        "appmetrica_device_id",
        "appmetrica_sdk_version",
        "city",
        "connection_type",
        "country_iso_code",
        "device_ipv6",
        "device_locale",
        "device_manufacturer",
        "device_model",
        "device_type",
        "google_aid",
        "ios_ifa",
        "ios_ifv",
        "mcc",
        "mnc",
        "original_device_model",
        "os_version",
        "windows_aid",
        "app_build_number",
        "app_package_name",
        "app_version_name",
    ]

    schema = th.PropertiesList(
        th.Property("deeplink_url_host", th.StringType),
        th.Property("deeplink_url_parameters", th.StringType),
        th.Property("deeplink_url_path", th.StringType),
        th.Property("deeplink_url_scheme", th.StringType),
        th.Property("event_datetime", th.StringType),
        th.Property("event_receive_datetime", th.StringType),
        th.Property("event_receive_timestamp", th.IntegerType),
        th.Property("event_timestamp", th.IntegerType),
        th.Property("is_reengagement", th.BooleanType),
        th.Property("profile_id", th.StringType),
        th.Property("publisher_id", th.IntegerType),
        th.Property("publisher_name", th.StringType),
        th.Property("session_id", th.IntegerType),
        th.Property("tracker_name", th.StringType),
        th.Property("tracking_id", th.IntegerType),
        th.Property("android_id", th.StringType),
        th.Property("appmetrica_device_id", th.IntegerType),
        th.Property("appmetrica_sdk_version", th.IntegerType),
        th.Property("city", th.StringType),
        th.Property("connection_type", th.StringType),
        th.Property("country_iso_code", th.StringType),
        th.Property("device_ipv6", th.StringType),
        th.Property("device_locale", th.StringType),
        th.Property("device_manufacturer", th.StringType),
        th.Property("device_model", th.StringType),
        th.Property("device_type", th.StringType),
        th.Property("google_aid", th.StringType),
        th.Property("ios_ifa", th.StringType),
        th.Property("ios_ifv", th.StringType),
        th.Property("mcc", th.IntegerType),
        th.Property("mnc", th.IntegerType),
        th.Property("original_device_model", th.StringType),
        th.Property("os_version", th.StringType),
        th.Property("windows_aid", th.StringType),
        th.Property("app_build_number", th.IntegerType),
        th.Property("app_package_name", th.StringType),
        th.Property("app_version_name", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        response.encoding = 'utf-8'
        reader = csv.DictReader(response.iter_lines(decode_unicode=True))
        yield from (
            obj
            for obj in reader
            if obj.get("event_datetime")
            and is_valid_datetime(obj.get("event_datetime"))
        )

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        row["is_reengagement"] = row["is_reengagement"] == "true"
        row["event_receive_timestamp"] = int(row["event_receive_timestamp"])
        row["event_timestamp"] = int(row["event_timestamp"])
        row["publisher_id"] = int(row["publisher_id"])
        row["session_id"] = int(row["session_id"])
        row["tracking_id"] = int(row["tracking_id"])
        row["appmetrica_device_id"] = int(row["appmetrica_device_id"])
        row["appmetrica_sdk_version"] = int(row["appmetrica_sdk_version"])
        row["mcc"] = int(row["mcc"])
        row["mnc"] = int(row["mnc"])
        row["app_build_number"] = int(row["app_build_number"])
        return row
