import datetime
from collections import defaultdict
from file_reader.file_reader import FileReader

import pandas as pd
import pymongo


class DbFileReader(FileReader):
    # __DB_URL = "mongodb://localhost:27017/"

    def __init__(self, cluster, single_date, db_url):
        self.cluster = cluster
        self.single_date = single_date
        self.__db_url = db_url

    def reading_db(self) -> pd.DataFrame():
        """Метод, прочитывающий noSQL БД ПРИЗМА-32 с помощью DB_URL"""

        data_cl = pd.DataFrame.from_records(
            pymongo.MongoClient(self.__db_url)["prisma-32_db"][f'{str(self.single_date)}_12d'].find(
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
        data_cl['Date'] = [datetime.date(item[0:3], item[5:6], item[8:9]) for item in data_cl['time_ns']]

        return data_cl

    def concat_n_data(self, concat_n_df):
        data_cl = self.reading_db
        # noinspection PyUnresolvedReferences
        concat_n_df = pd.concat([concat_n_df, data_cl[['Date', 'time', 'trigger'] + DbFileReader.__amp_n_cols]],
                                ignore_index=True)
        return concat_n_df


def db_preparing_data(start_date, end_date, path_to_db):
    concat_n_df_1 = pd.DataFrame(columns=['Date', 'time', 'trigger'] + DbFileReader.__amp_n_cols)
    concat_n_df_2 = pd.DataFrame(columns=['Date', 'time', 'trigger'] + DbFileReader.__amp_n_cols)
    for single_date in pd.date_range(start_date, end_date):
        try:
            db_file_reader_1 = DbFileReader(cluster=1, single_date=single_date, db_url=path_to_db)
            concat_n_df_1 = db_file_reader_1.concat_n_data(concat_n_df=concat_n_df_1)
        except FileNotFoundError:
            print(
                f"File n_{single_date.month:02}-" +
                f"{single_date.day:02}.{single_date.year - 2000:02}', does not exist")
        try:
            db_file_reader_1 = DbFileReader(cluster=2, single_date=single_date, db_url=path_to_db)
            concat_n_df_2 = db_file_reader_1.concat_n_data(concat_n_df=concat_n_df_2)
        except FileNotFoundError:
            print(
                f"File 2n_{single_date.month:02}-" +
                f"{single_date.day:02}.{single_date.year - 2000:02}', does not exist")

    return concat_n_df_1, concat_n_df_2

# if __name__ == '__main__':
#     date_time_start = datetime.date(2021, 11, 1)  # посмотреть почему не собирается конец дня 2018-04-22
#     date_time_stop = datetime.date(2021, 11, 1)
#     LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
#                      range((date_time_stop - date_time_start).days + 1)]
#     for date in LIST_OF_DATES:
#         print(DbFileReader(cluster=1, single_date=date, db_url="mongodb://localhost:27017/").reading_db())
