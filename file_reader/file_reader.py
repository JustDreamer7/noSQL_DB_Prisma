import datetime
import pandas as pd


# from pathlib import Path


class FileReader:
    __amp_n_cols = []
    for i in range(1, 17):
        __amp_n_cols.append(f'amp{i}')
        __amp_n_cols.append(f'n{i}')

    def __init__(self, cluster, single_date, path_to_files=''):
        self.cluster = cluster
        if cluster == 1:
            self.cluster_n = ''
        else:
            self.cluster_n = '2'
        self.path_to_files = path_to_files
        self.single_date = single_date
        self.n_file_path = self.making_file_path(file_type='n')
        self.n7_file_path = self.making_file_path(file_type='n7')
        self.t_file_path = self.making_file_path(file_type='t')
        self.p_file_path = self.making_file_path_eas_p(file_directory='nv', file_type='p')
        self.eas_file_path = self.making_file_path_eas_p(file_directory='EAS', file_type='eas')

    def __del__(self):
        pass

    def making_file_path(self, file_type):
        file_path = f'{self.path_to_files}\\{file_type}\\{self.cluster_n}{file_type}_{self.single_date.month:02}-{self.single_date.day:02}.{self.single_date.year - 2000:02}'
        return file_path

    def making_file_path_eas_p(self, file_directory, file_type):
        file_path = f'{self.path_to_files}/{file_directory}/{self.cluster}{file_type}_{self.single_date.month:02}-{self.single_date.day:02}.{self.single_date.year - 2000:02}'
        return file_path

    def reading_n_file(self):
        """Метод, прочитывающий n-файлы, возвращающий датафрейм дня на выходе. Или возвращающий filenotfounderror, если
                файла нет"""
        print(self.n_file_path)
        n_file = pd.read_csv(self.n_file_path,
                             sep=r'\s[-]*\s*', header=None, skipinitialspace=True, index_col=False, engine='python')
        n_file.dropna(axis=1, how='all', inplace=True)
        n_file.columns = ['time', 'number', 'sum_n', 'trigger'] + self.__class__.__amp_n_cols
        n_file = n_file[n_file['time'] < 86400]
        time_difference = n_file['time'].diff()
        bad_end_time_index = time_difference[time_difference < -10000].index
        if any(bad_end_time_index):
            n_file_today = n_file[n_file.index < bad_end_time_index[0]]
            n_file_day_after = n_file[n_file.index >= bad_end_time_index[0]]
            return n_file_today, n_file_day_after
        return n_file, []

    def reading_n7_file(self):
        print(self.n7_file_path)
        n7_file = pd.read_csv(self.n7_file_path,
                              sep=r'\s[-]*\s*', header=None, skipinitialspace=True, index_col=False, engine='python')
        n7_file.dropna(axis=1, how='all', inplace=True)
        n7_file[0] = n7_file[0].apply(lambda x: str(x).replace(',', '.')) # add this rows to file-twink
        n7_file = n7_file.astype({0: float})
        n7_file = n7_file[n7_file[0] < 86400]
        time_difference = n7_file[0].diff()
        bad_end_time_index = time_difference[time_difference < -10000].index
        if any(bad_end_time_index):
            n7_file_today = n7_file[n7_file.index < bad_end_time_index[0]]
            n7_file_day_after = n7_file[n7_file.index >= bad_end_time_index[0]]
            return n7_file_today, n7_file_day_after

        return n7_file, []

    @staticmethod
    def concat_data(file_today, file_day_after, single_date, concat_n_df):
        file_today['Date'] = [single_date.date()] * len(file_today.index)
        concat_n_df = pd.concat([concat_n_df, file_today], ignore_index=True)
        if any(file_day_after):
            file_day_after['Date'] = [(single_date + datetime.timedelta(
                days=1)).date()] * len(file_day_after.index)
            concat_n_df = pd.concat([concat_n_df, file_day_after],
                                    ignore_index=True)
        return concat_n_df

    def reading_t_file(self):
        """Converter for PRISMA t-files"""
        with open(self.t_file_path) as f:
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
        t_file_df = t_file_df[t_file_df["time"] < 86400]
        return t_file_df

    def reading_p_file(self):
        """Метод, прочитывающий p-файлы, возвращающий датафрейм дня на выходе. Или возвращающий filenotfounderror, если
        файла нет"""
        p_file = pd.read_csv(self.p_file_path,
                             sep=r'\s[-]*\s*', header=None, skipinitialspace=True, engine='python')
        p_file.dropna(axis=1, how='all', inplace=True)
        corr_p_file = self.correcting_p_file(p_file)
        return corr_p_file

    @staticmethod
    def correcting_p_file(p_file):
        """Метод, корректирующий старые файлы ПРИЗМА-32, возвращающий скорректированный датафрейм.
        Данный костыль нужен для старых p-файлов ПРИЗМА-32(до 14-15 гг.), в которых индексы строк,
        по сути обозначающие 5 минут реального времени между ранами, могут повторяться. """
        p_file['time'] = p_file[0]
        del p_file[0]
        p_file = p_file.sort_values(by='time')
        if len(p_file['time']) > len(p_file['time'].unique()):
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
