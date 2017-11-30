from cryptocurrency.utils import csvHelpers
from datetime import datetime, timedelta
import time
from urllib import request, parse
import urllib.error
import http.client
import json
import sys
import signal
import timeit

csvfile = None
dateFormat = '%Y-%m-%d %H:%M:%S'
ONE_YEAR_PERIOD = timedelta(weeks = 52)

def getAllTradeHistory(filename = 'tradeHistory.csv', currencyPair = 'BTC_NXT', end = None):
    fieldnames = ['date', 'tradeID', 'globalTradeID', 'type', 'rate', 'amount', 'total']
    (csvfile, csvwriter) = csvHelpers.createNewCSV(filename, fieldnames)

    lastTradeId = None

    # date range for the first api call
    timeWindow = timedelta(weeks = 4)

    if (end is None):
        endDate = datetime.utcnow()
    else:
        endDate = end
    startDate = endDate - timeWindow

    lastActiveTradeDate = endDate

    baseProgressMessage = 'Saving {0} trade histories to {1}'.format(currencyPair, filename)
    # numberOfRepeats = 3

    tt0 = timeit.default_timer()
    while (((lastTradeId is None) or (lastTradeId > 0)) and ((lastActiveTradeDate - startDate) <= ONE_YEAR_PERIOD)):
        # print('Time window size: {0}'.format(endDate - startDate))
        print('Latest trade ID: {0}'.format(lastTradeId), flush = True)
        # t0 = timeit.default_timer()
        # t2 = t0
        # t3 = t0
        # t4 = t0
        # t5 = t0

        # progress message
        # numberOfRepeats = (numberOfRepeats + 1) % 5
        # progressBar = '.' * (numberOfRepeats + 1)
        # print('{0} ... {1}'.format(baseProgressMessage, (lastActiveTradeDate - startDate)), flush = True)

        # format the dates and call the API
        startDateString = startDate.strftime(dateFormat)
        endDateString = endDate.strftime(dateFormat)
        tradeHistories = getTradeHistoryFrom(startDateString, endDateString, currencyPair)

        # t1 = timeit.default_timer()


        # check for any error responses and act accordingly
        if (isinstance(tradeHistories, dict) and ('error' in tradeHistories)):
            print('Trade histories retrieved: 0\n', flush = True)
            # try again with half the time window
            if (tradeHistories['error'] == 'Please specify a time window of no more than 1 month.'):
                timeWindow = (endDate - startDate) / 2
                startDate = endDate - timeWindow

        elif (isinstance(tradeHistories, list)):
            print('Trade histories retrieved: {0}\n'.format(len(tradeHistories)), flush = True)
            if (lastTradeId is not None):
                # t2 = timeit.default_timer()

                # avoid duplicate trade history data by comparing the trade ID, which is assumed to be monotonically increasing and unique
                tradeHistories = [tradeData for tradeData in tradeHistories if (int(tradeData['tradeID']) < lastTradeId)]

                # t3 = timeit.default_timer()

            # keep track of the current oldest trade ID to use for filtering duplicate trade history data in the next set
            if (len(tradeHistories) > 0):
                # NOTE: the api will only return back up to 50,000 data points despite the requested time range.
                # However, data points within the range of the end date will always be included in that 50,000 entry limit.

                oldestTradeHistory = tradeHistories[-1]
                lastTradeId = int(oldestTradeHistory['tradeID'])

                # update the date range for the next api call
                endDate = datetime.strptime(oldestTradeHistory['date'], dateFormat)
                startDate = endDate - timeWindow

                lastActiveTradeDate = endDate

            else:
                # there's no trade history data from the current time range, so search in the next time range
                endDate = startDate
                startDate = endDate - timeWindow

            # t4 = timeit.default_timer()

            # save trade history data to csv
            for transaction in tradeHistories:
                record = [transaction[fieldname] for fieldname in fieldnames]
                csvwriter.writerow(record)

            # t5 = timeit.default_timer()

        # t6 = timeit.default_timer()

        # Wait a bit to stay within the API call restrictions
        time.sleep(0.2)

        # testing performance
        # apiTime = t1 - t0
        # filterTime = t3 - t2
        # csvTime = t5 - t4
        # totalTime = t6 - t0
        # print('Time Measurements')
        # print('api call: {0:.3}s, csv: {1:.3}s, filtering: {2:.3}s, overall: {3:.3}s'.format(apiTime, csvTime, filterTime, totalTime))
        # print('api call: {0:.2%}, csv: {1:.2%}, filtering: {2:.2%}'.format(apiTime/totalTime, csvTime/totalTime, filterTime/totalTime), flush = True, end = '\n\n')

    tt1 = timeit.default_timer()

    csvfile.close()

    print('{0} done!'.format(baseProgressMessage), flush = True)
    print('\nTotal time: {0:.3}s'.format(tt1 - tt0), flush = True)




# Normally, trade history is an array of trade history data points or
# an empty array if no data is available at the specific time range.
# However, the API can respond back with an error and the format is a JSON.
# In this error case, the JSON looks like {"error": "the error message"}.
# If the time range is too big, it will give an error response in this format.
def getTradeHistoryFrom(start, end = None, currencyPair = 'BTC_NXT'):
    try:
        start = datetime.strptime(start, dateFormat).timestamp()

        if (end is None):
            end = datetime.utcnow().timestamp()
        else:
            end = datetime.strptime(end, dateFormat).timestamp()

        # creating the API call URL with the given parameters
        baseApi = 'https://poloniex.com/public'
        data = {
            'command': 'returnTradeHistory',
            'currencyPair': currencyPair,
            'start': start,
            'end': end
        }

        queryParameters = parse.urlencode(data)
        apiCall = baseApi + '?' + queryParameters

        # try calling the Poloniex API with a maximum number of retries if it fails
        result = []
        MAX_RETRIES = 5;
        numberOfRetries = 0;
        while (numberOfRetries < MAX_RETRIES):
            try:
                # Future TODO: use "with request.urlopen(apiCall) as response:" syntax maybe?
                response = request.urlopen(apiCall)
                response = response.read()
                result = json.loads(response)
            except (IOError, urllib.error.URLError, http.client.IncompleteRead):
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

    currencyPairs = [
        'USDT_BTC',
        'USDT_DASH',
        'USDT_LTC',
        'USDT_NXT',
        'USDT_STR',
        'USDT_XMR',
        'USDT_XRP'
    ]

    # will retrieve all trade history data from the beginning to now
    now = datetime.utcnow()

    # will retrieve all USDT currency pairs
    for currencyPair in currencyPairs:
        filename = 'tradeHistory-{0}.csv'.format(currencyPair)
        getAllTradeHistory(filename, currencyPair, now)
