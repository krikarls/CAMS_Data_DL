from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from time import perf_counter
from typing import Generator

from cdsapi import Client


class ModelName(str, Enum):
    EMEP = "emep"
    DEHM = "dehm"
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


@contextmanager
def perf_timer(message: str = "took"):
    try:
        print(message)
        seconds = -perf_counter()
        yield
    finally:
        seconds += perf_counter()
        delta = timedelta(seconds=int(seconds))
        print(f"{message} {delta}")


def download_data(
    client: cdsapi.Client, date: datetime, model: ModelName, *, tries: int = 5
):
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

    with perf_timer(f"downloading {path.name}"):
        path.parent.mkdir(exist_ok=True, parents=True)
        tmp = path.with_suffix(".tmp")
        for retry in range(1, tries + 1):
            try:
                client.retrieve("cams-europe-air-quality-forecasts", request, tmp)
            except Exception as e:
                print(f"{path.name} try #{retry} raised {e}")
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

    days = (end_date - start_date) // timedelta(days=1)
    for day in range(days):
        yield start_date + timedelta(days=day)
