#!/usr/bin/env python3
from concurrent.futures import ProcessPoolExecutor, as_completed

from cdsapi import Client

from CAMS2_83 import ModelName, date_range, download_data


def main():
    client = Client()
    for date in date_range("2021-06-01", "2021-08-31"):
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(download_data, client, date, model)
                for model in ModelName
            ]
            for future in as_completed(futures):
                if (exception := future.exception()) is not None:
                    raise exception


if __name__ == "__main__":
    main()
