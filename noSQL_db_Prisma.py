import datetime

import pymongo

from config_info.config import *
from file_reader.file_reader import FileReader


# from pathlib import Path


# noinspection DuplicatedCode
class NoSQLPrisma:
    __DB_URL = DB_URL
    __db_client = pymongo.MongoClient(__DB_URL)
    __prisma_db = __db_client["prisma-32_db"]

    def __init__(self, cluster, single_date):
        self.cluster = cluster
        self.single_date = single_date
        self.file_reader = FileReader(cluster=self.cluster, single_date=self.single_date,
                                      path_to_files=f'z:\\PRISMA-32\\DataArchive\\DATA P{self.cluster} archive\\data{self.single_date.year}')

    # def __del__(self):
    #     pass

    def dinods_data_copier(self, event_datetime, trigger, det_params, dinode):
        try:
            new_record = {
                '_id': f'{event_datetime.date()}_{self.cluster:02}_{dinode:02}d_{int(event_datetime.hour):02}:' +
                       f'{int(event_datetime.minute):02}:{int(event_datetime.second):02}.' +
                       f'{str(event_datetime.microsecond)[:3]}.000.000',
                'time_ns': int((int(event_datetime.hour) * 3600 + int(event_datetime.minute) * 60 + int(
                    event_datetime.second)) * 10e8 + int(event_datetime.microsecond) * 1000),
                'cluster': self.cluster,
                'trigger': int(trigger),
                'detectors': det_params
            }
            collection_prisma = NoSQLPrisma.__prisma_db[f'{str(event_datetime.date())}_{dinode}d']
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {event_datetime.date()}-{event_datetime.time()}')

    def prisma_12d_past_data_copier(self):
        n_file_today, n_file_day_after = self.file_reader.reading_n_file()
        try:
            t_file = self.file_reader.reading_t_file()
            n_file_today = n_file_today.merge(t_file)
            self.make_params_from_df_12_d(n_file_today, self.single_date)
            if any(n_file_day_after):
                n_file_day_after = n_file_day_after.merge(t_file)
                self.make_params_from_df_12_d(n_file_day_after,
                                              self.single_date + datetime.timedelta(
                                                  days=1))
        except FileNotFoundError:
            with open(f't_{self.cluster}cl_files_not_found.txt', 'a+') as f:
                f.write(f't-файла {self.cluster}-го кластера от {self.single_date} не существует\n')
            self.make_params_from_df_12_d_no_t(n_file_today, self.single_date)
            if any(n_file_day_after):
                self.make_params_from_df_12_d_no_t(n_file_day_after,
                                                   self.single_date + datetime.timedelta(
                                                       days=1))

    def prisma_7d_past_data_copier(self):
        n7_file_today, n7_file_day_after = self.file_reader.reading_n7_file()
        self.make_params_from_df_7_d(n7_file_today, self.single_date)
        if any(n7_file_day_after):
            self.make_params_from_df_7_d(n7_file_day_after,
                                         self.single_date + datetime.timedelta(days=1))

    def make_params_from_df_12_d(self, df, date):
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))
            event_datetime = datetime.datetime(date.year, date.month, date.day,
                                               int(event_time.split(':')[0]),
                                               int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
                                               int(round(
                                                   float(event_time.split(':')[2]) - int(
                                                       float(event_time.split(':')[2])),
                                                   2) * 10 ** 6)) - datetime.timedelta(hours=4)
            trigger = params[3]
            amp = [int(params[j]) for j in range(4, 36, 2)]
            n = [int(params[j]) for j in range(5, 37, 2)]

            n_time_delay = params[36]
            detector = params[37]
            n_in_step = params[38]

            det_params = {}
            for i in range(1, 17):
                n_time_delay_by_det = []
                detector_index = [ind for ind, v in enumerate(detector) if v == i]
                for j in detector_index:
                    n_time_delay_by_det.extend([n_time_delay[j]] * int(n_in_step[j]))
                #  В БД будут оставаться пустые списки при нуле нейтронов, надо ли это фиксить?
                det_params[f'det_{i:02}'] = {
                    'amplitude': amp[i - 1],
                    'neutrons': n[i - 1],
                    'time_delay': n_time_delay_by_det
                }
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=12)
        return None

    def make_params_from_df_7_d(self, df, date):
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))  # перевод в utc-формат
            event_datetime = datetime.datetime(date.year, date.month, date.day,
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
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=7)
        return None

    def make_params_from_df_12_d_no_t(self, df, date):
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))
            event_datetime = datetime.datetime(date.year, date.month, date.day,
                                               int(event_time.split(':')[0]),
                                               int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
                                               int(round(
                                                   float(event_time.split(':')[2]) - int(
                                                       float(event_time.split(':')[2])),
                                                   2) * 10 ** 6)) - datetime.timedelta(hours=4)
            trigger = params[3]
            amp = [int(params[j]) for j in range(4, 36, 2)]
            n = [int(params[j]) for j in range(5, 37, 2)]

            det_params = {}
            for i in range(1, 17):
                #  В БД будут оставаться пустые списки при нуле нейтронов, надо ли это фиксить?
                det_params[f'det_{i:02}'] = {
                    'amplitude': amp[i - 1],
                    'neutrons': n[i - 1],
                    'time_delay': False
                }
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=12)
        return None
