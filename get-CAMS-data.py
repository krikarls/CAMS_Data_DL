#!/usr/bin/env python3
from __future__ import annotations

from cdsapi import Client

from CAMS2_83 import date_range, download_data, ModelName

def main():
    client = Client()
    for date in date_range("2021-06-01", "2021-08-31"):
        for model in ModelName:
            download_data(client, date, model)

if __name__ == "__main__":
    main()
