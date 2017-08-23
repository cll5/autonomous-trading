import csv

# creates a csv tuple of (csv file identifier, csv writer)
def createNewCSV(filename, fieldnames = []):
    if (not filename.lower().endswith('.csv')):
        filename += '.csv'

    csvfile = open(filename, 'w', newline = '\n')

    # special characters are escaped with forward slash (\)
    writer = csv.writer(csvfile, doublequote = False, escapechar = '\\')
    if (fieldnames):
        writer.writerow(fieldnames)

    return (csvfile, writer)