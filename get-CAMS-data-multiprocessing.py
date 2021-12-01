#!/usr/bin/env python3

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Generator

import cdsapi


class ModelName(str, Enum):
    EMEP = "emep"
    DELH = "dehm"
    EURAD = "euradim"
    GEMAQ = "gemaq"
    LOTOS = "lotos"
    MATCH = "match"
    SILAM = "silam"
    MOCAGE = "mocage"
    CHIMERE = "chimere"
    ENSEMBLE = "ensemble"

    def __str__(self) -> str:
        return self.value


class PollutantName(str, Enum):
    NO2 = "nitrogen_dioxide"
    O3 = "ozone"
    PM10 = "particulate_matter_10um"
    PM25 = "particulate_matter_2.5um"
    SO2 = "sulphur_dioxide"
    CO = "carbon_monoxide"

    def __str__(self) -> str:
        return self.value


def download_data(client: cdsapi.Client, date: datetime, model: ModelName, *, tries: int = 5):
    path = Path(f"{date:%F}-{model}.nc")
    if path.exists():
        print(f"found {path.name}, skip")
        return

    request = dict(
        model=model,
        date=f"{date:%F}",
        format="netcdf",
        variable=[str(poll) for poll in PollutantName],
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

    while (date := start_date) <= end_date:
        yield date
        date += timedelta(days=1)


def main():
    client = cdsapi.Client()
    for date in date_range("2021-06-01", "2021-08-31"):
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(download_data, client, date, model) for model in ModelName
            ]
        for future in as_completed(futures):
            if (exception := future.exception()) is not None:
                raise exception


if __name__ == "__main__":
    main()
