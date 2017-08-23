from cryptocurrency.utils import csv
from datetime import datetime, timedelta
import time
from urllib import request, parse
import http.client
import json
import sys
import signal

csvfile = None
dateFormat = '%Y-%m-%d %H:%M:%S'

# valid periods (in seconds) are 300, 900, 1800, 7200, 14400, and 86400
def getAllChartData(filename = 'chartData_5mins.csv', currencyPair = 'BTC_NXT', period = 300):
    # get the field names for the csv and create the csv file
    # NOTE: startDate is 100000 instead of 1 or 0 because datetime appears to raise an error for timestamps less than 6 digits in python 3.6+
    startDate = 100000
    startDateString = datetime.utcfromtimestamp(startDate).strftime(dateFormat)
    chartData = getChartDataFrom(start = startDateString, end = startDateString, currencyPair = currencyPair, period = period)

    fieldnames = []
    for fieldname in chartData[0]:
        if (fieldname.lower() == 'date'):
            fieldnames.extend(['timestamp', fieldname])
        else:
            fieldnames.append(fieldname)

    (csvfile, csvwriter) = csv.create(filename, fieldnames)

    print('Fetching all chart data ({0} seconds interval) ... '.format(period), end = '', flush = True)
    while (startDate > 0):
        chartData = getChartDataFrom(start = startDateString, currencyPair = currencyPair, period = period)

        if (len(chartData) > 0):
            # To avoid appending invalid data (ex. timestamp is the start of POSIX time), we will skip any data that has the date field as 0
            if (chartData[0]['date'] > 0):
                # write chart data to csv
                for datum in chartData:
                    record = []
                    for fieldname, value in datum.items():
                        if (fieldname.lower() == 'date'):
                            timestamp = value
                            readableDate = datetime.utcfromtimestamp(timestamp).strftime(dateFormat)
                            record.extend([timestamp, readableDate])
                        else:
                            record.append(value)
                    csvwriter.writerow(record)

            startDate = int(chartData[-1]['date'])
            date = startDate + 1
        else:
            startDate += 1
            date = startDate

        startDateString = datetime.utcfromtimestamp(date).strftime(dateFormat)

        # Wait a bit to stay within the API call restrictions
        time.sleep(0.25)

    csvfile.close()
    print('complete!')


# valid periods (in seconds) are 300, 900, 1800, 7200, 14400, and 86400
def getChartDataFrom(start, end = None, currencyPair = 'BTC_NXT', period = 300):
    try:
        baseApi = 'https://poloniex.com/public'
        data = {
            'command': 'returnChartData',
            'currencyPair': currencyPair,
            'period': period,
            'start': datetime.strptime(start, dateFormat).timestamp()
        }

        if (end is not None):
            data['end'] = datetime.strptime(end, dateFormat).timestamp()

        queryParameters = parse.urlencode(data)
        apiCall = baseApi + '?' + queryParameters

        # try calling the Poloniex API with a maximum number of retries if it fails
        result = {}
        MAX_RETRIES = 5;
        numberOfRetries = 0;
        while (numberOfRetries < MAX_RETRIES):
            try:
                response = request.urlopen(apiCall)
                response = response.read()
                result = json.loads(response)
            except (IOError, http.client.IncompleteRead):
                numberOfRetries += 1
                time.sleep(0.2)
            else:
                break

        return result
    except Exception as error:
        print("Something went wrong with getting the data")
        print(error)
        raise

def quitProgram(signal, frame):
    if (csvfile is not None):
        csvfile.close()
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, quitProgram)

    coinPair = 'BTC_NXT'
    getAllChartData('chartData_5mins.csv', coinPair, 300)
    getAllChartData('chartData_15mins.csv', coinPair, 900)
    getAllChartData('chartData_30mins.csv', coinPair, 1800)
    getAllChartData('chartData_2hrs.csv', coinPair, 7200)
    getAllChartData('chartData_4hrs.csv', coinPair, 14400)
    getAllChartData('chartData_1day.csv', coinPair, 86400)