import pandas as pd
import os
from datetime import datetime

FILE_PATH = 'a.csv'
CHUNK_SIZE = 10000

def clean_path(path):
    return path.replace('%3D', '=')

def days_between_dates(date1, date2):
    return (date2 - date1).days

def is_day_1_of_month(date):
    return date.day == 1

def get_date_from_path(path):
    year = path.split('year=')[1].split('/')[0]
    month = path.split('month=')[1].split('/')[0]
    day = path.split('day=')[1].split('/')[0]

    return datetime(int(year), int(month), int(day))

def filter_path(path):
    notification = {}
    notification['reason'] = ''
    notification['path'] = path
    notification['error'] = None
    valid = True

    try:
        date = get_date_from_path(path)
    except Exception as e:
        notification['error'] = e
        notification['reason'] = 'Invalid date'
        valid = False
        return notification, valid

    if is_day_1_of_month(date):
        notification['reason'] = 'Date is not the first day of the month'
        valid = False
        return notification, valid

    if days_between_dates(date, datetime.now()) < 90:
        notification['reason'] = 'Date is too recent'
        valid = False
        return notification, valid

    return notification, valid

def main():
    os.remove('valid.csv')
    os.remove('invalid.csv')

    df = pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE)

    for chunk in df:
        for _, row in chunk.iterrows():
            bucket, path = row.iloc[0], row.iloc[1]

            notification, valid = filter_path(clean_path(path))
            if valid:
                with open('valid.csv', 'a') as f:
                    f.write(f"{bucket},{path}\n")
            else:
                with open('invalid.csv', 'a') as f:
                    f.write(f"{bucket},{path},{notification['reason']}\n")


if __name__ == "__main__":
    main()
