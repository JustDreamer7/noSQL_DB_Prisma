import datetime

import pandas as pd
import pymongo

from config_info.config import *

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def prisma_7d_past_data_copier(date, cluster):
    if cluster == 1:
        n7_file_template = f"n7_{date.month:02}-{date.day:02}.{date.year - 2000:02}"
        n7_file = pd.read_csv(PATH_TO_PRISMA_1_7d_DATA + n7_file_template, sep=' ', skipinitialspace=True, header=None)
        n7_file = n7_file.dropna(axis=1, how='all')
        for i in range(len(n7_file[0])):
            if type(n7_file[0][i]) is str:
                n7_file.loc[i,0] = float('.'.join(n7_file.loc[i,0].split(',')))
        print("Data file: {}".format(PATH_TO_PRISMA_1_7d_DATA + n7_file_template))
        fix_end_time_series = n7_file[0].lt(n7_file[0].shift())
        bad_end_time_index = fix_end_time_series[fix_end_time_series == True].index
    else:
        n7_file_template = f"2n7_{date.month:02}-{date.day:02}.{date.year - 2000:02}"
        n7_file = pd.read_csv(PATH_TO_PRISMA_2_7d_DATA + n7_file_template, sep=' ', skipinitialspace=True, header=None)
        n7_file = n7_file.dropna(axis=1, how='all')
        for i in range(len(n7_file[0])):
            if type(n7_file[0][i]) is str:
                n7_file.loc[i,0] = float('.'.join(n7_file.loc[i,0].split(',')))
        print("Data file: {}".format(PATH_TO_PRISMA_2_7d_DATA + n7_file_template))
        fix_end_time_series = n7_file[0].lt(n7_file[0].shift())
        bad_end_time_index = fix_end_time_series[fix_end_time_series == True].index
    for index in range(len(n7_file.index)):
        params = list(n7_file.iloc[index])
        # if type(params[0]) is str:
        #     params[0] = float('.'.join(params[0].split(',')))
        event_time = str(datetime.timedelta(seconds=params[0]))  # перевод в utc-формат
        # event_date = (datetime.timedelta(seconds=params[0]) + datetime.timedelta(hours=3)).date()
        event_datetime = datetime.datetime(date.year, date.month, date.day, int(event_time.split(':')[0]),
                                           int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
                                           int(round(
                                               float(event_time.split(':')[2]) - int(float(event_time.split(':')[2])),
                                               2) * 10 ** 6)) - datetime.timedelta(hours=4)
        if index >= bad_end_time_index:
            new_date = date + datetime.timedelta(days=1)
            event_datetime = datetime.datetime(new_date.year, new_date.month, new_date.day,
                                               int(event_time.split(':')[0]),
                                               int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
                                               int(round(
                                                   float(event_time.split(':')[2]) - int(
                                                       float(event_time.split(':')[2])),
                                                   2) * 10 ** 6)) - datetime.timedelta(hours=4)
        trigger = params[2]
        amp = [int(params[j]) for j in range(3, 19)]

        det_params = {}

        for i in range(1, 17):
            det_params[f'det_{i:02}'] = {
                'amplitude': amp[i - 1]
            }

        try:
            new_record = {
                '_id': f'{event_datetime.date()}_{cluster:02}_07d_{int(event_datetime.hour):02}:' +
                       f'{int(event_datetime.minute):02}:{int(event_datetime.second):02}.' +
                       f'{str(event_datetime.microsecond)[:3]}.000.000',
                'time_ns': int((int(event_datetime.hour) * 1440 + int(event_datetime.minute) * 60 + int(
                    event_datetime.second)) * 10e8 + int(event_datetime.microsecond) * 1000),
                'cluster': cluster,
                'trigger': int(trigger),
                'detectors': det_params
            }
            collection_prisma = prisma_db[f'{str(event_datetime.date())}_7d']
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {event_datetime.date()}-{event_time}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cluster_1 = 1
    cluster_2 = 2
    date_time_start = datetime.date(2021, 12, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 12, 31)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        try:
            prisma_7d_past_data_copier(date, cluster_1)
        except FileNotFoundError:
            print(f'файла {cluster_1}-го кластера от {date} не существует')
        try:
            prisma_7d_past_data_copier(date, cluster_2)
        except FileNotFoundError:
            print(f'файла {cluster_2}-го кластера от {date} не существует')
    print('test')
