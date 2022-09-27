import datetime

import pandas as pd
import pymongo

from config_info.config import *
from file_reader.file_reader import FileReader


# noinspection DuplicatedCode
class NoSQLPrisma:
    __DB_URL = DB_URL
    __db_client = pymongo.MongoClient(__DB_URL)
    __prisma_db = __db_client["prisma-32_db"]

    def __init__(self, cluster, single_date):
        self.cluster = cluster
        if self.cluster == 1:
            self.cluster_n = ""
        else:
            self.cluster_n = '2'
        self.single_date = single_date
        self.__PATH_TO_PRISMA_N_DATA = f"D:\\PRISMA20\\P{self.cluster}"
        self.__PATH_TO_PRISMA_7d_DATA = f"D:\\PRISMA20\\P{self.cluster}\\n7"
        self.__PATH_TO_PRISMA_T_DATA = f"D:\\PRISMA20\\P{self.cluster}\\t\\"
        self.file_reader = FileReader(cluster=self.cluster, single_date=self.single_date,
                                      path_to_files=self.__PATH_TO_PRISMA_N_DATA,
                                      path_to_files_7d=self.__PATH_TO_PRISMA_7d_DATA)

    def __del__(self):
        pass

    def t_file_converter(self, path_to_t_file):
        """Converter for PRISMA t-files"""
        with open(
                f'{path_to_t_file}{self.cluster_n}t_{self.single_date.month:02}-{self.single_date.day:02}.{self.single_date.year - 2000:02}') as f:
            raw_data = f.readlines()
        raw_data = [line.rstrip() for line in raw_data]
        # Убираем переводы строки
        event_list = []
        main_list = []
        sep = 0
        for i in range(len(raw_data)):
            if raw_data[i] == '*#*':
                main_list.append(raw_data[sep].split(' '))
                event_list.append(raw_data[sep + 1:i])
                sep = i + 1
        unit_delay = []
        for item in event_list:
            delay_per_event = []
            for line in item:
                step = line.split(' ')
                for i in range(1, 17):
                    if int(step[i]) != 0:
                        delay_per_event.append([round(int(step[0]) * (10 ** (-4)), 4), i, int(step[i])])
            unit_delay.append(delay_per_event)
        plural_data_list = []
        for i in unit_delay:
            time_list = []
            detector_list = []
            neut_quantity_list = []
            for j in i:
                time_list.append(j[0])
                detector_list.append(j[1])
                neut_quantity_list.append(j[2])
            plural_data_list.append([time_list, detector_list, neut_quantity_list])
        for i in range(len(main_list)):
            main_list[i].extend(plural_data_list[i])
        t_file_df = pd.DataFrame(main_list,
                                 columns=['time', 'number', 'sum_n', 'trigger', 'time_delay', 'detectors',
                                          'n_per_step'])
        t_file_df = t_file_df.astype({"time": float, "number": int, "sum_n": int, "trigger": int})
        return t_file_df

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
        t_file = self.t_file_converter(path_to_t_file=self.__PATH_TO_PRISMA_T_DATA)
        n_file_today = self.file_reader.n_file_today.merge(t_file)
        self.make_parameters_from_df_12_d(n_file_today, self.single_date)
        if any(self.file_reader.n_file_day_after):
            n_file_day_after = self.file_reader.n_file_day_after.merge(t_file)
            self.make_parameters_from_df_12_d(n_file_day_after,
                                              self.single_date + datetime.timedelta(
                                                  days=1))

    def prisma_7d_past_data_copier(self):
        self.make_parameters_from_df_7_d(self.file_reader.n7_file_today, self.single_date)
        if any(self.file_reader.n7_file_day_after):
            self.make_parameters_from_df_7_d(self.file_reader.n7_file_day_after,
                                             self.single_date + datetime.timedelta(days=1))

    def make_parameters_from_df_12_d(self, df, date):
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

    def make_parameters_from_df_7_d(self, df, date):
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
# for index in range(len(n_file_today.index)):
#     params = list(n_file_today.iloc[index])
#     event_time = str(datetime.timedelta(seconds=params[0]))
#     event_datetime = datetime.datetime(self.single_date.year, self.single_date.month, self.single_date.day,
#                                        int(event_time.split(':')[0]),
#                                        int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
#                                        int(round(
#                                            float(event_time.split(':')[2]) - int(
#                                                float(event_time.split(':')[2])),
#                                            2) * 10 ** 6)) - datetime.timedelta(hours=4)
#     if index >= bad_end_time_index:
#         new_date = self.single_date + datetime.timedelta(days=1)
#         event_datetime = datetime.datetime(new_date.year, new_date.month, new_date.day,
#                                            int(event_time.split(':')[0]),
#                                            int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
#                                            int(round(
#                                                float(event_time.split(':')[2]) - int(
#                                                    float(event_time.split(':')[2])),
#                                                2) * 10 ** 6)) - datetime.timedelta(hours=4)
#     trigger = params[3]
#     amp = [int(params[j]) for j in range(4, 36, 2)]
#     n = [int(params[j]) for j in range(5, 37, 2)]
#
#     n_time_delay = params[36]
#     detector = params[37]
#     n_in_step = params[38]
#
#     det_params = {}
#     for i in range(1, 17):
#         n_time_delay_by_det = []
#         detector_index = [ind for ind, v in enumerate(detector) if v == i]
#         for j in detector_index:
#             n_time_delay_by_det.extend([n_time_delay[j]] * int(n_in_step[j]))
#         #  В БД будут оставаться пустые списки при нуле нейтронов, надо ли это фиксить?
#         det_params[f'det_{i:02}'] = {
#             'amplitude': amp[i - 1],
#             'neutrons': n[i - 1],
#             'time_delay': n_time_delay_by_det
#         }
