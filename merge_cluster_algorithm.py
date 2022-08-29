import pandas as pd
import pymongo
import datetime

from config_info.config import *
from preparing_to_merge import prepare_to_merge_12d, prepare_to_merge_7d
from merge_dinods_algorithm import merge_dinods

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def merge_clusters(data_cl_1, data_cl_2):
    """Процесс сшивки событий 1-го кластера с ближайшим по времени событием из 2-го.
    Временные ворота - 80 мс <<80e6>>"""
    merge_dict = {}
    for i in range(len(data_cl_1['time_ns'])):
        add_2_cl = data_cl_2[(data_cl_2['time_ns'] >= (data_cl_1['time_ns'][i] - 80e6)) & (
                data_cl_2['time_ns'] <= (data_cl_1['time_ns'][i] + 80e6))][['_id', 'time_ns']].reset_index(
            drop=True)
        # Тут идет рассмотрение возможности, когда на одно событие 1-го кластера идут два события второго
        if len(add_2_cl) == 1:
            merge_dict[data_cl_1['_id'][i]] = add_2_cl['_id'][0]
        elif len(add_2_cl) > 1:
            add_2_cl['time_ns'] = add_2_cl['time_ns'] - data_cl_1['time_ns'][i]
            add_2_cl.sort_values(by='time_ns', inplace=True).reset_index(drop=True)
            merge_dict[data_cl_1['_id'][i]] = add_2_cl['_id'][0]
    return merge_dict


def prisma_events_db_copier(date_):
    collection_prisma = prisma_db[f'{str(date_)}_events']
    data_cl_1 = prepare_to_merge_12d(date_, cluster=1, a_crit=6, fr=4)
    data_cl_2 = prepare_to_merge_12d(date_, cluster=2, a_crit=6, fr=4)

    merge_cl = merge_clusters(data_cl_1, data_cl_2)

    # Чтобы добавить в list_of_ids _id 7-х динодов.

    # data_cl_1_12d = prepare_to_merge_12d(date_, cluster=1, a_crit=11, fr=4)
    # data_cl_2_12d = prepare_to_merge_12d(date_, cluster=2, a_crit=11, fr=4)
    #
    # data_cl_1_7d = prepare_to_merge_7d(date_, cluster=1)
    # data_cl_2_7d = prepare_to_merge_7d(date_, cluster=2)
    #
    # merge_din_cl_1 = merge_dinods(data_cl_1_12d, data_cl_1_7d)
    # merge_din_cl_2 = merge_dinods(data_cl_2_12d, data_cl_2_7d)

    all_data_cl_1 = pd.DataFrame.from_records(
        pymongo.MongoClient(DB_URL)["prisma-32_db"][f'{str(date_)}_12d'].find(
            {'cluster': 1, '_id': {'$nin': list(merge_cl.keys())}}))
    all_data_cl_2 = pd.DataFrame.from_records(
        pymongo.MongoClient(DB_URL)["prisma-32_db"][f'{str(date_)}_12d'].find(
            {'cluster': 2, '_id': {'$nin': list(merge_cl.values())}}))
    data_cl_merge = pd.DataFrame.from_records(
        pymongo.MongoClient(DB_URL)["prisma-32_db"][f'{str(date_)}_12d'].find(
            {'cluster': 1, '_id': {'$in': list(merge_cl.keys())}}))
    # print(all_data_cl_1['_id'][2000])
    for i in range(len(all_data_cl_1.index)):
        id_list = all_data_cl_1['_id'][i].split('_')
        try:
            new_record = {
                '_id': f"{id_list[0]}_pe_{id_list[-1]}",
                'eas_event_time_ns': int(all_data_cl_1['time_ns'][i]),
                'mask': 1,
                'multiplicity': 1,
                'list_of_cluster_numbers': [1],
                'list_of_ids': [all_data_cl_1['_id'][i]]
            }
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {id_list[0]}_pe_{id_list[-1]}')

    for i in range(len(all_data_cl_2.index)):
        id_list = all_data_cl_2['_id'][i].split('_')

        try:
            new_record = {
                '_id': f"{id_list[0]}_pe_{id_list[-1]}",
                'eas_event_time_ns': int(all_data_cl_2['time_ns'][i]),
                'mask': 2,
                'multiplicity': 1,
                'list_of_cluster_numbers': [2],
                'list_of_ids': [all_data_cl_2['_id'][i]]
            }
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {id_list[0]}_pe_{id_list[-1]}')

    for i in range(len(data_cl_merge.index)):
        id_list = data_cl_merge['_id'][i].split('_')
        try:
            new_record = {
                '_id': f"{id_list[0]}_pe_{id_list[-1]}",
                'eas_event_time_ns': int(data_cl_merge['time_ns'][i]),
                'mask': 3,
                'multiplicity': 2,
                'list_of_cluster_numbers': [1, 2],
                'list_of_ids': [data_cl_merge['_id'][i], merge_cl[data_cl_merge['_id'][i]]]
            }
            ins_result = collection_prisma.insert_one(new_record)
            print(f'Copied - {ins_result.inserted_id}')
        except pymongo.errors.DuplicateKeyError:
            print(f'Ошибка - {id_list[0]}_pe_{id_list[-1]}')


if __name__ == '__main__':
    date_time_start = datetime.date(2021, 12, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 12, 31)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        prisma_events_db_copier(date)
    print('test')
