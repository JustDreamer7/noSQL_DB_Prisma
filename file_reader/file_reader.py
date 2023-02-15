import datetime
import pathlib

import pandas as pd


class FileReader:
    """Класс для чтения файлов ПРИЗМА-32, должен быть общим во всех модулях разработки"""

    __amp_n_cols = []
    for i in range(1, 17):
        __amp_n_cols.append(f'amp{i}')
        __amp_n_cols.append(f'n{i}')

    __slots__ = ["cluster", "cluster_n", 'path_to_files', 'single_date']

    # Для оптимизации вызова экземпляра класса, так как вызывается экземпляр на каждый день
    # работы установки.

    def __init__(self, cluster, single_date, path_to_files=''):
        self.cluster = cluster
        if cluster == 1:
            self.cluster_n = ''
        else:
            self.cluster_n = '2'
        self.path_to_files = path_to_files
        self.single_date = single_date

        # self.eas_file_path = self.making_file_path(file_directory='EAS', file_type='eas')
        # Понадобится, когда добавим eas-файлы

    def making_file_path(self, file_type, file_directory=None):
        """Функция для построения путей для различных типов файлов ПРИЗМА-32"""
        file_directory = file_type if file_directory is None else file_directory
        file_name = self.cluster_n + \
                    f'{file_type}_{self.single_date.month:02}-' + \
                    f'{self.single_date.day:02}.{self.single_date.year - 2000:02}'
        file_path = pathlib.PurePath(self.path_to_files, file_directory, file_name)
        return file_path

    def reading_n_file(self):
        """Метод, прочитывающий n-файлы. Делит файл на два датафрейма, если в файле присутствуют
         события следующего дня и возвращает их на выходе. Если в файле нет события следующего дня,
         то возвращает один датафрейм и пустую строку"""
        n_file_path = self.making_file_path(file_type='n')
        print(n_file_path)
        n_file = pd.read_csv(n_file_path,
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
        """Метод, прочитывающий n7-файлы. При ошибке формата файла ValueError(надо указать какой, кстати) запускает
        функцию конвертера n7-файлов. После успешного прочтения делит файл на два датафрейма, если в файле присутствуют
        события следующего дня и возвращает их на выходе. Если в файле нет события следующего дня, то возвращает один
        датафрейм и пустую строку"""
        n7_file_path = self.making_file_path(file_type='n7')
        print(n7_file_path)
        try:
            n7_file = pd.read_csv(n7_file_path,
                                  sep=r'\s[-]*\s*', header=None, skipinitialspace=True, index_col=False,
                                  engine='python')
            n7_file.dropna(axis=1, how='all', inplace=True)
            n7_file.columns = ['time', 'number', 'trigger'] + [f'amp{i}' for i in range(1, 17)]
        except ValueError:  # указать, что за ValueError
            self.n7_file_conventer(n7_file_path)
            n7_file = pd.read_csv(n7_file_path,
                                  sep=r'\s[-]*\s*', header=None, skipinitialspace=True, index_col=False,
                                  engine='python')
            n7_file.dropna(axis=1, how='all', inplace=True)
            n7_file.columns = ['time', 'number', 'trigger'] + [f'amp{i}' for i in range(1, 17)]
        n7_file['time'] = n7_file['time'].apply(lambda x: str(x).replace(',', '.'))  # add this rows to file-twink
        n7_file = n7_file.astype({'time': float})
        n7_file = n7_file[n7_file['time'] < 86400]
        time_difference = n7_file['time'].diff()
        bad_end_time_index = time_difference[time_difference < -10000].index
        if any(bad_end_time_index):
            n7_file_today = n7_file[n7_file.index < bad_end_time_index[0]]
            n7_file_day_after = n7_file[n7_file.index >= bad_end_time_index[0]]
            return n7_file_today, n7_file_day_after
        return n7_file, []

    @staticmethod
    def n7_file_conventer(n7_file_path):
        """Статический метод конвентера для n7-файлов, если в них амплитуды записываются не на одной строчке с
        событием"""
        with open(n7_file_path, 'r', encoding='utf-8') as n7_file_read:
            raw_data = n7_file_read.readlines()
        if len(list(filter(lambda x: x != '', raw_data[0].rstrip().split(' ')))) < 5:
            start_of_strings = [line.rstrip() for line in raw_data[::2]]
            end_of_strings = raw_data[1::2]
            raw_data = [x + ' ' + y for x, y in zip(start_of_strings, end_of_strings)]
            with open(n7_file_path, 'w', encoding='utf-8') as n7_file_write:
                n7_file_write.writelines(raw_data)
        else:
            raise FileNotFoundError

    def t_file_conventer(self):
        """Конвентер для t-файлов ПРИЗМА-32, возвращает датафрейм t-файла,
         с которым сделать merge датафрейма n-файла"""
        t_file_path = self.making_file_path(file_type='t')

        def reading_t_file(t_path):
            """Генератор для чтения t-файлов"""
            with open(t_path, encoding='utf-8') as file:
                for file_line in file.readlines():
                    yield file_line.rstrip()

        raw_data = list(reading_t_file(t_path=t_file_path))
        event_list, main_list = [], []
        sep = 0
        for i, _ in enumerate(raw_data):
            if raw_data[i] == '*#*':
                main_list.append(raw_data[sep].split(' '))
                event_list.append(raw_data[sep + 1:i])
                sep = i + 1
        plural_data_list = []
        for item in event_list:
            time_list, detector_list, neut_quantity_list = [], [], []
            for line in item:
                step = line.split(' ')
                for i in range(1, 17):
                    if int(step[i]) != 0:
                        time_list.append(round(int(step[0]) * (10 ** (-4)), 4))
                        detector_list.append(i)
                        neut_quantity_list.append(int(step[i]))
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
        """Метод, прочитывающий p-файлы, возвращающий датафрейм дня на выходе."""
        p_file_path = self.making_file_path(file_directory='nv', file_type='p')
        p_file = pd.read_csv(p_file_path,
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
            # После удаления полных дубликатов ищем повторяющиеся индексы. Сначала удаляем строки,
            # состоящие полностью из нулей и точек (value = len(p_file.columns)), потом ищем множество
            # дубликатов индексов и множество строк, почти полностью (value > 30) состоящих из нулей и точек.
            # Берем пересечение этих двух множеств и удаляем находящиеся в пересечении строки
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
            # Также может быть, что после фильтрации осталось больше строк, чем нужно, так как в старых
            # p-файлах может быть больше индексов, чем минут в дне. Тогда оставляем только 288
            if len(p_file.index) == 289:
                p_file = p_file.head(288)
        return p_file

    @staticmethod
    def concat_data(file_today, file_day_after, single_date, concat_n_df):
        """Статический метод соединения датафреймов файлов, полученных на выходе в один с добавления колонки с датой"""
        file_today['Date'] = [single_date.date()] * len(file_today.index)
        concat_n_df = pd.concat([concat_n_df, file_today], ignore_index=True)
        if any(file_day_after):  # не пропускает пустую строку
            file_day_after['Date'] = [(single_date + datetime.timedelta(
                days=1)).date()] * len(file_day_after.index)
            concat_n_df = pd.concat([concat_n_df, file_day_after], ignore_index=True)
        return concat_n_df

    @staticmethod
    def preparing_data(start_date, end_date, cluster, path_to_files):
        """Статический метод подготовки данных из n-файлов за определенный период для определенного кластера"""
        concat_n_df = pd.DataFrame(columns=['Date', 'time', 'trigger'] + FileReader.__amp_n_cols)
        for single_date in pd.date_range(start_date - datetime.timedelta(days=1), end_date):
            # минус один день в timedelta, так как начальные события первого дня могут остаться в n-файле предыдущего
            # дня
            try:
                n_file_reader = FileReader(cluster=cluster, single_date=single_date, path_to_files=path_to_files)
                n_file_today, n_file_day_after = n_file_reader.reading_n_file()
                concat_n_df = n_file_reader.concat_data(file_today=n_file_today, file_day_after=n_file_day_after,
                                                        concat_n_df=concat_n_df, single_date=single_date)
            except FileNotFoundError:
                print(
                    f"File {path_to_files}/n_{single_date.month:02}-" +
                    f"{single_date.day:02}.{single_date.year - 2000:02}', does not exist")
        return concat_n_df
