import datetime
import pathlib
import pymongo

import config_info.config
from file_reader.file_reader import FileReader


# noinspection DuplicatedCode
class NoSQLPrisma:
    """Класс записи n, n7, t - файлов в MongoDB базу данных PRISMA-32"""
    __DB_URL = config_info.config.DB_URL
    __db_client = pymongo.MongoClient(__DB_URL)
    __prisma_db = __db_client["prisma-32_db"]
    __slots__ = ["cluster", "single_date", "file_reader"]

    def __init__(self, cluster, single_date):
        self.cluster = cluster
        self.single_date = single_date
        self.file_reader = FileReader(cluster=self.cluster, single_date=self.single_date,
                                      path_to_files=pathlib.PurePath('D:\\PRISMA20', f'P{self.cluster}'))

    def dinods_data_copier(self, event_datetime, trigger, det_params, dinode):
        """Метод, создающий запись в базе данных, пользуясь информацией из его аргументов,
        если запись в БД уже есть, то кидает ошибку"""
        try:
            new_record = {
                '_id': f'{event_datetime.date()}_{self.cluster:02}_{dinode:02}d' +
                       f'_{int(event_datetime.hour):02}:' +
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
            # with open('log.txt', 'a+') as log_file:
            #     log_file.write(f'Copied - {ins_result.inserted_id}\n')
        except pymongo.errors.DuplicateKeyError:
            print(f'Запись существует - {self.cluster}cl - {dinode:02}d' +
                  f' - {event_datetime.date()}-{event_datetime.time()}')
            # with open('log.txt', 'a+') as log_file:
            #     log_file.write(f'Ошибка - {event_datetime.date()}-{event_datetime.time()}\n')

    def prisma_12d_past_data_copier(self):
        """Главный метод для данных 12-динода, в котором происходит чтение n
        и t-файлов и их соединение, определение их параметров и запись в БД"""
        n_file_today, n_file_day_after = self.file_reader.reading_n_file()  # Если n-файла, нет то
        # проброс исключение идет в runner.py
        try:
            t_file = self.file_reader.t_file_conventer()
            n_file_today = n_file_today.merge(t_file)  # А что делать, если не все строки в t-файле совпадают
            self.make_params_from_df_12_d(n_file_today, self.single_date)
            if any(n_file_day_after):
                n_file_day_after = n_file_day_after.merge(t_file)
                self.make_params_from_df_12_d(n_file_day_after,
                                              self.single_date + datetime.timedelta(
                                                  days=1))
        # Идет проброс исключение на существование t-файла и вызывается метод make_params_from_df_12_d_no_t
        except FileNotFoundError:
            with open(f'files_not_found/t_{self.cluster}cl_files_not_found.txt', 'a+', encoding='utf-8') as t_log_file:
                t_log_file.write(f't-файла {self.cluster}-го кластера от {self.single_date} не существует\n')
            self.make_params_from_df_12_d_no_t(n_file_today, self.single_date)
            if any(n_file_day_after):
                self.make_params_from_df_12_d_no_t(n_file_day_after,
                                                   self.single_date + datetime.timedelta(
                                                       days=1))

    def prisma_7d_past_data_copier(self):
        """Главный метод для данных 7-динода, в котором происходит чтение n7,
                определение их параметров и запись в БД"""
        n7_file_today, n7_file_day_after = self.file_reader.reading_n7_file()
        self.make_params_from_df_7_d(n7_file_today, self.single_date)
        if any(n7_file_day_after):
            self.make_params_from_df_7_d(n7_file_day_after,
                                         self.single_date + datetime.timedelta(days=1))

    def make_params_from_df_12_d(self, df, date):
        """Метод для данных 12-динода, парсящий построчно n+t-dataframe, определяющий все нужные для БД
        параметры события: время регистрации, амплитуды и кол-во нейтронов по детекторно, триггер,
        время запаздывания нейтронов."""
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))
            event_time_split = event_time.split(':')
            event_datetime = datetime.datetime(date.year, date.month, date.day,
                                               int(event_time_split[0]),
                                               int(event_time_split[1]),
                                               int(float(event_time_split[2])),
                                               int(round(
                                                   float(event_time_split[2]) - int(
                                                       float(event_time_split[2])),
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
            # Вызывает в конце dinods_data_copier для записи в БД всю информацию о событии
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=12)

    def make_params_from_df_7_d(self, df, date):
        """Метод для данных 7-динода, парсящий построчно n7-dataframe, определяющий все нужные для БД
                параметры события: время регистрации, амплитуды по детекторно, триггер"""
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))  # перевод в utc-формат
            event_time_split = event_time.split(':')
            event_datetime = datetime.datetime(date.year, date.month, date.day,
                                               int(event_time_split[0]),
                                               int(event_time_split[1]),
                                               int(float(event_time_split[2])),
                                               int(round(
                                                   float(event_time_split[2]) - int(
                                                       float(event_time_split[2])),
                                                   2) * 10 ** 6)) - datetime.timedelta(hours=4)
            trigger = params[2]
            amp = [int(params[j]) for j in range(3, 19)]

            det_params = {}

            for i in range(1, 17):
                det_params[f'det_{i:02}'] = {
                    'amplitude': amp[i - 1]
                }
            # Вызывает в конце dinods_data_copier для записи в БД всю информацию о событии
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=7)

    def make_params_from_df_12_d_no_t(self, df, date):
        """Метод для данных 12-динода, парсящий построчно n-dataframe, так t-файла не существует в эту дату,
           определяющий все нужные для БД параметры события: время регистрации,
           амплитуды и кол-во нейтронов по детекторно, триггер."""
        for index in range(len(df.index)):
            params = list(df.iloc[index])
            event_time = str(datetime.timedelta(seconds=params[0]))
            event_time_split = event_time.split(':')
            event_datetime = datetime.datetime(date.year, date.month, date.day,
                                               int(event_time_split[0]),
                                               int(event_time_split[1]),
                                               int(float(event_time_split[2])),
                                               int(round(
                                                   float(event_time_split[2]) - int(
                                                       float(event_time_split[2])),
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
            # Вызывает в конце dinods_data_copier для записи в БД всю информацию о событии
            self.dinods_data_copier(event_datetime=event_datetime, trigger=trigger,
                                    det_params=det_params, dinode=12)
