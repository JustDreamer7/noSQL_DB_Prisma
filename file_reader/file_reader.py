import datetime
from collections import defaultdict

import pandas as pd
import pymongo


class FileReader:
    __amp_n_cols = []
    for i in range(1, 17):
        __amp_n_cols.append(f'amp{i}')
        __amp_n_cols.append(f'n{i}')

    def __init__(self, cluster, single_date, path_to_files='', path_to_files_7d=''):
        self.cluster = cluster
        if cluster == 1:
            self.cluster_n = ''
        else:
            self.cluster_n = '2'
        self.path_to_files = path_to_files
        self.path_to_files_7d = path_to_files_7d
        self.single_date = single_date
        self.n_file_today, self.n_file_day_after = self._reading_n_file()
        self.n7_file_today, self.n7_file_day_after = self._reading_n7_file()

    def __del__(self):
        pass

    def reading_file(self, file_type) -> pd.DataFrame:
        file = pd.read_csv(
            f'{self.path_to_files}\\{file_type}\\{self.cluster_n}n_{self.single_date.month:02}' +
            f'-{self.single_date.day:02}.{self.single_date.year - 2000:02}',
            sep=r'\s[-]*\s*', header=None, skipinitialspace=True, index_col=False)
        file.dropna(axis=1, how='all', inplace=True)
        return file

    def preparing_n_file(self):
        n_file = self.reading_file(file_type='n')
        n_file.columns = ['time', 'number', 'sum_n', 'trigger'] + FileReader.__amp_n_cols

    def _reading_n_file(self):
        """Метод, прочитывающий n-файлы, возвращающий датафрейм дня на выходе. Или возвращающий filenotfounderror, если
        файла нет"""
        n_file = pd.read_csv(
            f'{self.path_to_files}\\{self.cluster_n}n_{self.single_date.month:02}' +
            f'-{self.single_date.day:02}.{self.single_date.year - 2000:02}',
            sep=' ', header=None, skipinitialspace=True, index_col=False,
            names=['time', 'number', 'sum_n', 'trigger'] + FileReader.__amp_n_cols)
        n_file.dropna(axis=1, how='all', inplace=True)
        time_difference = n_file['time'].diff()
        bad_end_time_index = time_difference[time_difference < -10000].index
        if any(bad_end_time_index):
            n_file_today = n_file[n_file.index < bad_end_time_index[0]]
            n_file_day_after = n_file[n_file.index >= bad_end_time_index[0]]
            return n_file_today, n_file_day_after
        return n_file, []

    def _reading_n7_file(self):
        n7_file = pd.read_csv(
            f'{self.path_to_files_7d}\\{self.cluster_n}n7_{self.single_date.month:02}' +
            f'-{self.single_date.day:02}.{self.single_date.year - 2000:02}',
            sep=' ', header=None, skipinitialspace=True, index_col=False)
        n7_file.dropna(axis=1, how='all', inplace=True)
        for i in range(len(n7_file[0])):
            if type(n7_file[0][i]) is str:
                n7_file.loc[i, 0] = float('.'.join(n7_file.loc[i, 0].split(',')))
        time_difference = n7_file[0].diff()
        bad_end_time_index = time_difference[time_difference < -10000].index
        if any(bad_end_time_index):
            n7_file_today = n7_file[n7_file.index < bad_end_time_index[0]]
            n7_file_day_after = n7_file[n7_file.index >= bad_end_time_index[0]]
            return n7_file_today, n7_file_day_after
        return n7_file, []

    @staticmethod
    def concat_n_data(cls_object, concat_n_df):
        cls_object.n_file_today['Date'] = [cls_object.single_date.date()] * len(cls_object.n_file_today.index)
        concat_n_df = pd.concat([concat_n_df, cls_object.n_file_today],
                                ignore_index=True)
        if any(cls_object.n_file_day_after):
            cls_object.n_file_day_after['Date'] = [(cls_object.single_date + datetime.timedelta(
                days=1)).date()] * len(cls_object.n_file_day_after.index)
            concat_n_df = pd.concat([concat_n_df, cls_object.n_file_day_after],
                                    ignore_index=True)
        return concat_n_df

    def reading_p_file(self):
        """Метод, прочитывающий p-файлы, возвращающий датафрейм дня на выходе. Или возвращающий filenotfounderror, если
        файла нет"""
        try:
            p_file = pd.read_csv(
                f'{self.path_to_files}\\nv\\{self.cluster}p{self.single_date.date().month:02}' +
                f'-{self.single_date.date().day:02}.{self.single_date.date().year - 2000:02}',
                sep=r'\s[-]*\s*', header=None, skipinitialspace=True, engine='python')
            p_file.dropna(axis=1, how='all', inplace=True)
            corr_p_file = self.correcting_p_file(p_file)
            return corr_p_file
        except FileNotFoundError as error:
            print(f"File {self.path_to_files}\\nv\\{self.cluster}p{self.single_date.date().month:02}-" +
                  f"{self.single_date.date().day:02}.{self.single_date.date().year - 2000:02} does not exist")
            return error.strerror

    @staticmethod
    def correcting_p_file(p_file):
        """Метод, корректирующий старые файлы ПРИЗМА-32, возвращающий скорректированный датафрейм"""
        p_file['time'] = p_file[0]
        del p_file[0]
        p_file = p_file.sort_values(by='time')
        if len(p_file['time']) > len(p_file['time'].unique()):
            """Данный костыль нужен для старых p-файлов ПРИЗМА-32(до 14-15 гг.), в которых индексы строк, 
            по сути обозначающие 5 минут реального времени между ранами, могут повторяться. """
            p_file.drop_duplicates(keep=False, inplace=True)
            """После удаления полных дубликатов ищем повторяющиеся индексы. Сначала удаляем строки, 
            состоящие полностью из нулей и точек (value = len(p_file.columns)), потом ищем множество 
            дубликатов индексов и множество строк, почти полностью (value > 30) состоящих из нулей и точек. 
            Берем пересечение этих двух множеств и удаляем находящиеся в пересечении строки"""
            null_row = dict(p_file.isin([0, '.']).sum(axis=1))  # Проверяем на нули и точки
            all_null_index = list(
                {key: value for key, value in null_row.items() if value == len(p_file.columns)}.keys())
            p_file.drop(index=all_null_index, inplace=True)

            null_index = list(
                {key: value for key, value in null_row.items() if value > len(p_file.columns) - 5}.keys())
            same_index = dict(p_file['time'].duplicated(keep=False))
            same_index_row = list({key: value for key, value in same_index.items() if value is True}.keys())
            bad_index = list(set(null_index) & set(same_index_row))
            p_file.drop(index=bad_index, inplace=True)
            """Также может быть, что после фильтрации осталось больше строк, чем нужно, так как в старых 
            p-файлах может быть больше индексов, чем минут в дне. Тогда оставляем только 288"""
            if len(p_file.index) == 289:
                p_file = p_file.head(288)
        return p_file
