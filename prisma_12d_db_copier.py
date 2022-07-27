import pymongo
import pandas as pd
import datetime

from config_info.config import *

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def t_file_converter(path, cluster, date_):
    """Converter for PRISMA t-files"""
    with open(f'{path}{cluster}t_{date_.month:02}-{date_.day:02}.{date_.year - 2000:02}') as f:
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
    return main_list


def prisma_12d_past_data_copier(date, cluster):
    collection_prisma = prisma_db[f'{str(date)}_12d']
    if cluster == 1:
        n_file_template = f"n_{date.month:02}-{date.day:02}.{date.year - 2000:02}"
        n_file = pd.read_csv(PATH_TO_PRISMA_1_DATA + n_file_template, sep=' ', skipinitialspace=True, header=None)
        n_file = n_file.dropna(axis=1, how='all')
        print("Data file: {}".format(PATH_TO_PRISMA_1_DATA + n_file_template))
        t_file = t_file_converter(PATH_TO_PRISMA_1_T_FILES, "", date)
    else:
        n_file_template = f"2n_{date.month:02}-{date.day:02}.{date.year - 2000:02}"
        n_file = pd.read_csv(PATH_TO_PRISMA_2_DATA + n_file_template, sep=' ', skipinitialspace=True, header=None)
        n_file = n_file.dropna(axis=1, how='all')
        print("Data file: {}".format(PATH_TO_PRISMA_2_DATA + n_file_template))
        t_file = t_file_converter(PATH_TO_PRISMA_2_T_FILES, 2, date)
    for index in range(len(n_file.index)):
        params = list(n_file.iloc[index])
        neutron_params = t_file[index]
        event_time = str(datetime.timedelta(seconds=params[0]))
        event_date = datetime.date(date.year, date.month, date.day)
        # event_datetime = datetime.datetime(date.year, date.month, date.day, int(event_time.split(':')[0]),
        #                                    int(event_time.split(':')[1]), int(float(event_time.split(':')[2])),
        #                                    int(round(
        #                                        float(event_time.split(':')[2]) - int(float(event_time.split(':')[2])),
        #                                        2) * 10 ** 6))
        trigger = params[3]
        amp = [int(params[j]) for j in range(4, 36, 2)]
        n = [int(params[j]) for j in range(5, 37, 2)]

        n_time_delay = neutron_params[4]
        detector = neutron_params[5]
        n_in_step = neutron_params[6]

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

        try:
            new_record = {
                '_id': f'{event_date}_{cluster:02}_12d_{int(event_time.split(":")[0]):02}:' +
                       f'{int(event_time.split(":")[1]):02}:{int(float(event_time.split(":")[2])):02}.' +
                       f'{int(round(float(event_time.split(":")[2]) - int(float(event_time.split(":")[2])), 2) * 10 ** 3):03}.000.000',
                'time_ns': int(params[0] * 10e8),
                'cluster': cluster,
                'trigger': int(trigger),
                'detectors': det_params
            }
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {event_date}-{event_time}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cluster_1 = 1
    cluster_2 = 2
    date_time_start = datetime.date(2021, 11, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 11, 30)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        try:
            prisma_12d_past_data_copier(date, cluster_1)
        except FileNotFoundError:
            print(f'файла {cluster_1}-го кластера от {date} не существует')
        try:
            prisma_12d_past_data_copier(date, cluster_2)
        except FileNotFoundError:
            print(f'файла {cluster_2}-го кластера от {date} не существует')
    print('test')
