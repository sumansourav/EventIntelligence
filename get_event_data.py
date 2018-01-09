import requests
import urllib
import csv
import json
import argparse

__author__ = 'sumansourav'

''' This code can be used to get a list of events at a specific area within a given date range.
    The code uses a http get based query from PredictHQ's available APIs. The APIs have a free-mium model and might
    require subscription.

    Steps to get the code working:
    1. Get access_token from https://app.predicthq.com/signup
    2. Get the Country, Area(lat long based), radius of the area, and date-range(from, to) information ready
    3. Run the python file with the following commands:
    $ python get_event_data.py <access_token> <country_code> -a <radius> <unit> <lat> <long> -d <fromyyyy-mm-dd> <toyyyy-mm-dd>
    4. If there are no issues, a "event_data.csv" is created with the following event details in it.
        Data columns (total 18 columns):
        relevance      100 non-null float64
        id             100 non-null object
        title          100 non-null object
        description    47 non-null object
        category       100 non-null object
        labels         100 non-null object
        rank           100 non-null float64
        duration       100 non-null float64
        start          100 non-null object
        end            100 non-null object
        updated        100 non-null object
        timezone       99 non-null object
        location       100 non-null object
        scope          100 non-null object
        country        100 non-null object
        state          100 non-null object
        entities       70 non-null object
    Happy Analyzing!
'''

parser = argparse.ArgumentParser(description='Get event data for analysis')
parser.add_argument('access_token',
                    help='Enter the access token to interact with PredictHQ APIs.'
                         'Obtain one by signing up with https://app.predicthq.com/dev/applications')

parser.add_argument('country',
                    help='Enter the country. '
                         'Find a list of country codes here: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2')

parser.add_argument('-a', '--area', required=True, nargs='+',
                    help='Enter the area in the specific format: {radius} {unit} {latitude} {longitude}. '
                         'Ex: 10 km -36.844480 174.768368. Valid units are m, km, ft, mi')

parser.add_argument('-d', '--daterange', required=True, nargs='+',
                    help='The date from and to with from date first and to date second. Use yyyy-mm-dd format only')

args = parser.parse_args()

offset = 0
results = []

while True:

    response = requests.get(
        url="https://api.predicthq.com/v1/events/",
        headers={
          "Authorization": "Bearer {0}".format(args.access_token),
          "Accept": "application/json"
        },
        params={
            "country": args.country,
            "offset": offset,
            "start.gte": args.daterange[0],
            "start.lte": args.daterange[1],
            "within": "{0}{1}@{2},{3}".format(args.area[0], args.area[1], args.area[2], args.area[3])
        }
    )
    json_response = response.json()
    results += json_response['results']

    # parse for the next url.
    # This step is required as all data can not be obtained by querying a single time
    # (not atleast with the free access token). Only 10 event details are obtained in a single query with the free-trial
    # access token.
    try:
        next_url = json_response['next']
        if next_url is not None and next_url is not 'null':
            offset = urllib.parse.parse_qs(next_url)['offset'][0]
        else:
            break
    except Exception as e:
        print(e)
        break

print(results)

outputFile = open("event_data.csv", 'w', encoding='UTF-8')  # load csv file
data = json.loads(json.dumps(results),)  # load json content
output = csv.writer(outputFile)  # create a csv.write
output.writerow(data[0].keys())  # header row
for row in data:
    output.writerow(row.values())  # values row
