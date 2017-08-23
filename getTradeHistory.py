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
ONE_YEAR_PERIOD = timedelta(weeks = 52)

def getAllTradeHistory(filename = 'tradeHistory.csv', currencyPair = 'BTC_NXT'):
    fieldnames = ['date', 'tradeID', 'globalTradeID', 'type', 'rate', 'amount', 'total']
    (csvfile, csvwriter) = csv.create(filename, fieldnames)

    totalTransactions = 0
    numberOfTransactionSaved = 0
    lastTradeId = None

    # date range for the first api call
    endDate = datetime.utcnow()
    startDate = endDate - timedelta(weeks = 10)

    print('Fetching all trade history data ...')
    while ((lastTradeId is None) or ((endDate - startDate) <= ONE_YEAR_PERIOD)):
        startDateString = startDate.strftime(dateFormat)
        endDateString = endDate.strftime(dateFormat)

        tradeHistory = getTradeHistoryFrom(startDateString, endDateString, currencyPair)

        if (len(tradeHistory) > 0):
            # correct any trade ID overlap due to date resolution
            entry = 0
            while ((lastTradeId is not None) and (entry < len(tradeHistory)) and (tradeHistory[entry]['tradeID'] >= lastTradeId)):
                entry += 1

            uniqueTradeHistory = tradeHistory[entry:]
            if (len(uniqueTradeHistory) > 0):

                for transaction in uniqueTradeHistory:
                    record = []
                    for fieldname in fieldnames:
                        record.append(transaction[fieldname])
                    csvwriter.writerow(record)

                # We'll assume that the trade id is never a negative integer. We can use the trade id to exit the loop
                if ((lastTradeId is None) or (uniqueTradeHistory[-1]['tradeID'] < lastTradeId)):
                    lastTradeId = uniqueTradeHistory[-1]['tradeID']

                numberOfTransactionSaved += len(uniqueTradeHistory)

                if (totalTransactions == 0):
                    totalTransactions = (uniqueTradeHistory[0]['tradeID'] + 1)

                # update the date range for the next api call
                endDate = datetime.strptime(uniqueTradeHistory[-1]['date'], dateFormat)
                startDate = endDate - timedelta(weeks = 10)

                print('Trade history from {0} to {1}'.format(startDateString, endDateString), end = ' - ', flush = True)
                print('{2:.2F}% Completed ({0}/{1} records)'.format(numberOfTransactionSaved, totalTransactions, (numberOfTransactionSaved / totalTransactions) * 100))
            else:
                endDate = startDate
                startDate = endDate - timedelta(weeks = 10)

        # update the date range for the next api call
        else:
            startDate = startDate - timedelta(weeks = 10)

        # Wait a bit to stay within the API call restrictions
        time.sleep(0.25)

    csvfile.close()
    print('Fetching trade history data complete!')




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
    getAllTradeHistory('tradeHistory.csv', 'BTC_NXT')
