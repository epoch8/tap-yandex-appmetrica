import os
import time
import requests


APPMETRICA_TOKEN = os.getenv("TAP_APPMETRICA_TOKEN")
APPLICATION_ID = os.getenv("TAP_APPMETRICA_APPLICATION_ID")

url = "https://api.appmetrica.yandex.ru/logs/v1/export/deeplinks.csv"
params = {
    "application_id": APPLICATION_ID,
    "date_since": "2026-03-16 00:00:00",
    "date_until": "2026-03-16 23:59:59",
    "fields": ",".join([
        "event_datetime",
        "event_receive_datetime",
        "session_id",
        "appmetrica_device_id",
        "profile_id",
        "tracker_name",
        "tracking_id",
        "is_reengagement",
        "deeplink_url_scheme",
        "deeplink_url_host",
        "deeplink_url_path",
        "deeplink_url_parameters",
        "app_package_name",
        "app_version_name",
    ]),
    "limit": 10,
    "skip_unavailable_shards": "true",
}

headers = {
    "Authorization": f"OAuth {APPMETRICA_TOKEN}",
    "Accept-Encoding": "gzip",
}

for attempt in range(120):
    r = requests.get(url, params=params, headers=headers, timeout=120)
    print("status:", r.status_code)

    if r.status_code == 200:
        with open("deeplinks_2026-03-16_2026-03-16.csv", "wb") as f:
            f.write(r.content)
        print("downloaded:", len(r.content), "bytes")
        break

    if r.status_code == 202:
        time.sleep(5)
        continue

    print(r.text[:2000])
    r.raise_for_status()
else:
    raise RuntimeError("Не дождались готовности файла.")
