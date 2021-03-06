#!/usr/bin/env python

import cdsapi
import pandas as pd
import os
import time

c = cdsapi.Client()


def DownloadData(date,variable):

    c.retrieve(
        'cams-europe-air-quality-forecasts',
        {
            'model': [
                'chimere', 'dehm', 'emep',
                'ensemble', 'euradim', 'gemaq',
                'lotos', 'match', 'mocage',
                'silam',
            ],
            'date': date,
            'format': 'netcdf',
            'variable': variable,
            'level': '0',
            'type': 'forecast',
            'time': '00:00',
            'leadtime_hour': [
                '0', '1', '10',
                '11', '12', '13',
                '14', '15', '16',
                '17', '18', '19',
                '2', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '3',
                '30', '31', '32',
                '33', '34', '35',
                '36', '37', '38',
                '39', '4', '40',
                '41', '42', '43',
                '44', '45', '46',
                '47', '48', '49',
                '5', '50', '51',
                '52', '53', '54',
                '55', '56', '57',
                '58', '59', '6',
                '60', '61', '62',
                '63', '64', '65',
                '66', '67', '68',
                '69', '7', '70',
                '71', '72', '73',
                '74', '75', '76',
                '77', '78', '79',
                '8', '80', '81',
                '82', '83', '84',
                '85', '86', '87',
                '88', '89', '9',
                '90', '91', '92',
                '93', '94', '95',
                '96',
            ],
        },
        date+"-all-models-"+variable+".zip")



def DateRange2String(start_date,end_date):
    """Returns a string of dates from start_date to end_date"""
    dates = pd.date_range(start=start_date, end=end_date)

    dates_string = []
    for date in dates:      
        date_str = str(date.date())
        dates_string.append(date_str)


    return dates_string



# Model names
models = ['chimere','dehm','emep','ensemble','euradim','gemaq','lotos', 'match', 'mocage','silam']


# Variables
variables = ['nitrogen_dioxide','ozone','particulate_matter_10um','particulate_matter_2.5um']


# Dates
start_date = '2021-06-01'
end_date = '2021-08-31'

dates = DateRange2String(start_date,end_date)


# Loop over the combination of dates and variables 
# Note that all models have been selected manually by "hard coding"
for date in dates:
    for variable in variables:

        file_name = date+"-all-models-"+variable+".zip"
        print("Attempting to look for: ",file_name)
  
        # If file does not exist: download
        counter = 0     
        while os.path.isfile("complete-files/"+file_name) == False:
            counter += 1

            try:
                DownloadData(date,variable)
                print("Download complete: "+file_name)
                os.system("mv "+file_name+" complete-files")

            except Exception as ex:
                # Print exeption error msg
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                error_message = template.format(type(ex).__name__, ex.args)
                print(error_message)

                print("Now let's wait 2 min before re-try...")
                time.sleep(120)

            # Limit the number of attempts to get data
            if counter > 5:
                break
      
