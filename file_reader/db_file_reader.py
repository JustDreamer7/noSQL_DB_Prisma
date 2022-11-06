import datetime
from collections import defaultdict

import pandas as pd
import pymongo

from file_reader.file_reader import FileReader


class DbFileReader(FileReader):
    """Класс для чтения данных ПРИЗМА-32 из MongoDB базы данных, должен быть один для
    всех модулей"""
    # __DB_URL = "mongodb://localhost:27017/"
    __amp_n_cols = []
    for i in range(1, 17):
        __amp_n_cols.append(f'amp{i}')
        __amp_n_cols.append(f'n{i}')

    __slots__ = ["__db_url"]

    def __init__(self, cluster, single_date, db_url):
        super().__init__(cluster, single_date)
        self.__db_url = db_url

    def reading_db(self) -> pd.DataFrame():
        """Метод, прочитывающий noSQL БД ПРИЗМА-32 с помощью DB_URL"""

        data_cl = pd.DataFrame.from_records(
            pymongo.MongoClient(self.__db_url)["prisma-32_db"][f'{str(self.single_date.date())}_12d'].find(
                {'cluster': self.cluster}))
        if data_cl.empty:
            raise FileNotFoundError
        amp_dict = defaultdict(list)
        n_dict = defaultdict(list)
        for item in data_cl['detectors']:
            for j in [f'det_{i:02}' for i in range(1, 17)]:
                amp_dict[j].append(item[j]['amplitude'])
                n_dict[j].append(item[j]['neutrons'])

        for i in range(1, 17):
            data_cl[f'amp{i}'] = amp_dict[f'det_{i:02}']
            data_cl[f'n{i}'] = n_dict[f'det_{i:02}']
        data_cl['time'] = [round(item / 1e9, 2) for item in data_cl['time_ns']]
        data_cl['Date'] = [datetime.date(int(item[0:4]), int(item[5:7]), int(item[8:10])) for item in data_cl['_id']]

        return data_cl

    def concat_n_data(self, concat_n_df):
        """Статический метод соединения датафреймов файлов,
        полученных на выходе в один с добавления колонки с датой"""
        data_cl = self.reading_db()
        # noinspection PyUnresolvedReferences
        concat_n_df = pd.concat([concat_n_df, data_cl[['Date', 'time', 'trigger'] + self.__class__.__amp_n_cols]],
                                ignore_index=True)
        return concat_n_df

    @staticmethod
    def db_preparing_data(start_date, end_date, path_to_db, cluster):
        """Статический метод подготовки информации для обработки из
        базы данных ПРИЗМА-32 за определенный
        период для определенного кластера"""
        concat_n_df = pd.DataFrame(columns=['Date', 'time', 'trigger'] + DbFileReader.__amp_n_cols)
        for single_date in pd.date_range(start_date, end_date):
            try:
                db_file_reader = DbFileReader(cluster=cluster, single_date=single_date, db_url=path_to_db)
                concat_n_df = db_file_reader.concat_n_data(concat_n_df=concat_n_df)
            except FileNotFoundError:
                print(
                    f"File {cluster}n_{single_date.month:02}-" +
                    f"{single_date.day:02}.{single_date.year - 2000:02}', does not exist")
                # Переделать n на коллекцию из БД
        return concat_n_df
