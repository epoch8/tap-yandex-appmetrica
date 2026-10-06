"""The bookmark must never move backwards, even when retro data is re-read."""

import pytest

from tap_yandex_appmetrica.tap import TapYandexAppmetrica

CONFIG = {
    "token": "x",
    "application_id": "1",
    "start_date": "2026-01-01",
    "chunk_days": 1,
    "retro_interval_days": 3,
}


def _finalize(stream_name, bookmark, marker):
    tap = TapYandexAppmetrica(config=CONFIG, parse_env_config=False)
    stream = tap.streams[stream_name]
    state = stream.get_context_state(None)
    rk = stream.replication_key
    if bookmark is not None:
        state["replication_key"] = rk
        state["replication_key_value"] = bookmark
    if marker is not None:
        state["progress_markers"] = {
            "Note": "Progress is not resumable if interrupted.",
            "replication_key": rk,
            "replication_key_value": marker,
        }
    stream.finalize_state_progress_markers()
    return stream.get_context_state(None).get("replication_key_value")


@pytest.mark.parametrize("stream_name", ["events", "installations", "install_devices"])
def test_retro_chunk_does_not_regress_bookmark(stream_name):
    assert _finalize(stream_name, "2026-10-06 12:00:00", "2026-10-04 23:59:59") == (
        "2026-10-06 12:00:00"
    )


def test_bookmark_still_advances():
    assert _finalize("events", "2026-10-06 12:00:00", "2026-10-07 01:00:00") == (
        "2026-10-07 01:00:00"
    )


def test_mixed_timestamp_formats_compare_as_datetimes():
    # 'T' sorts after ' ' as a string, which would wrongly look "newer".
    assert _finalize("events", "2026-10-06 12:00:00", "2026-10-06T08:00:00+00:00") == (
        "2026-10-06 12:00:00"
    )


def test_first_run_without_bookmark():
    assert _finalize("events", None, "2026-10-04 10:00:00") == "2026-10-04 10:00:00"


def test_no_records_keeps_bookmark():
    assert _finalize("events", "2026-10-06 12:00:00", None) == "2026-10-06 12:00:00"
