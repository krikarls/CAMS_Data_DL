#!/usr/bin/env python3

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator

import cdsapi

MODELS: list[str] = [
    "chimere",
    "dehm",
    "emep",
    "ensemble",
    "euradim",
    "gemaq",
    "lotos",
    "match",
    "mocage",
    "silam",
]

VARIABLES: list[str] = [
    "nitrogen_dioxide",
    "ozone",
    "particulate_matter_10um",
    "particulate_matter_2.5um",
    "sulphur_dioxide",
    "carbon_monoxide"
]


client = cdsapi.Client()


def download_data(date: datetime, model: str, *, tries: int = 5):
    path = Path(f"{date:%F}-{model}.nc")
    if path.exists():
        print(f"found {path.name}, skip")
        return

    request = dict(
        model=model,
        date=f"{date:%F}",
        format="netcdf",
        variable=VARIABLES,
        level="0",
        type="forecast",
        time="00:00",
        leadtime_hour=[str(h) for h in range(97)],
    )

    print(f"download {path.name}")
    path.parent.mkdir(exist_ok=True, parents=True)
    tmp = path.with_suffix(".tmp")
    for retry in range(1, tries + 1):
        try:
            client.retrieve("cams-europe-air-quality-forecasts", request, tmp)
        except Exception as e:
            print(f"{date} try #{retry} raised {e}")
        else:
            tmp.rename(path)
            break

    if not path.exists():
        raise RuntimeError(f"failed to download {path.name}")


def date_range(
    start_date: str | datetime, end_date: str | datetime
) -> Generator[datetime, None, None]:
    """dates from start_date to end_date"""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while (date:=start_date) <= end_date:
        yield date
        date += timedelta(days=1)

def main():
    for date in date_range("2021-06-01", "2021-08-31"):
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(download_data, date, model) for model in MODELS]
        for future in as_completed(futures):
            if (exception:=future.exception()) is not None:
                raise exception


if __name__ == "__main__":
    main()
