#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from cdsapi import Client

from .CAMS2_83 import date_range, download_data, ModelName

def main():
    client = Client()
    for date in date_range("2021-06-01", "2021-08-31"):
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(download_data, client, date, model)
                for model in ModelName
            ]
            for future in as_completed(futures):
                if (exception := future.exception()) is not None:
                    raise exception


if __name__ == "__main__":
    main()
