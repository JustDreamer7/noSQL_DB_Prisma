import pymongo
import datetime

from config_info.config import *
from preparing_to_merge import prepare_to_merge_12d, prepare_to_merge_7d

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def merge_dinods(data_din_12, data_din_7):
    """Процесс сшивки событий 12-го динода с событиями 7-го динода, лежащими внутри временных ворот.
    Временные ворота - 90 мс <<90e6>>. Временные ворота установлены временно. ТО ЕСТЬ ПОМЕНЯЙ ИХ, КОГДА ДОДЕЛАЕШЬ!!
    Можно модифицировать алгоритм, чтобы раз в месяц или в год, менялись временные ворота."""
    merge_dict = {}
    amp_cols = ['amp_1', 'amp_2', 'amp_3', 'amp_4', 'amp_5', 'amp_6',
                'amp_7', 'amp_8', 'amp_9', 'amp_10', 'amp_11', 'amp_12', 'amp_13', 'amp_14',
                'amp_15', 'amp_16']
    for i in range(len(data_din_12['time_ns'])):
        add_7d = data_din_7[(data_din_7['time_ns'] >= (data_din_12['time_ns'][i] - 90e6)) & (
                data_din_7['time_ns'] <= (data_din_12['time_ns'][i] + 90e6))][
            ['_id', 'time_ns'] + amp_cols].reset_index(drop=True)
        # Тут идет рассмотрение возможности, когда на одно событие 12-го динода идут два события 7-го.
        if len(add_7d) == 1:
            merge_dict[data_din_12['_id'][i]] = add_7d['_id'][0]
        elif len(add_7d) > 1:
            """Костыль на случай, что событий 7-го динода внутри временных ворот несколько. 
            В таком случае программа отбирает, то событие 7-го с максимальной амплитудой на том детекторе, 
            на котором максимальная амплитуда на 12-м диноде.
            Костыль требует доработки, так как не учитывает события, где все детекторы на 12-м под 511 код. АЦП"""
            main_col_12 = list({k: v for k, v in sorted(
                data_din_12[data_din_12["time_ns"] == data_din_12['time_ns'][i]][amp_cols].max().items(),
                key=lambda item: item[1], reverse=True)}.keys())[0]
            merge_dict[data_din_12['_id'][i]] = \
                add_7d[add_7d[main_col_12] == add_7d[[main_col_12]].max()[0]].reset_index(drop=True).head(1)['_id'][0]

    return merge_dict


def update_with_add_cluster_id(date_):
    collection_prisma_12d = prisma_db[f'{str(date_)}_12d']
    collection_prisma_7d = prisma_db[f'{str(date_)}_7d']
    data_cl_1_12d = prepare_to_merge_12d(date_, cluster=1, a_crit=11, fr=4)
    data_cl_2_12d = prepare_to_merge_12d(date_, cluster=2, a_crit=11, fr=4)

    data_cl_1_7d = prepare_to_merge_7d(date_, cluster=1)
    data_cl_2_7d = prepare_to_merge_7d(date_, cluster=2)

    merge_din_cl_1 = merge_dinods(data_cl_1_12d, data_cl_1_7d)
    merge_din_cl_2 = merge_dinods(data_cl_2_12d, data_cl_2_7d)

    for key in merge_din_cl_1:
        upd_id_result_12d_cl_1 = collection_prisma_12d.update_one({'_id': key, 'cluster': 1},
                                                                  {"$set": {'event_id_7d': merge_din_cl_1[key]}})
        upd_id_result_7d_cl_1 = collection_prisma_7d.update_one({'_id': merge_din_cl_1[key], 'cluster': 1},
                                                                {"$set": {'event_id_12d': key}})
        print(f'Added event_id_1 - {key} - {upd_id_result_12d_cl_1.raw_result}')
        print(f'Added event_id_2 - {merge_din_cl_1[key]} - {upd_id_result_7d_cl_1.raw_result}')

    for key in merge_din_cl_2:
        upd_id_result_12d_cl_2 = collection_prisma_12d.update_one({'_id': key, 'cluster': 2},
                                                                  {"$set": {'event_id_7d': merge_din_cl_2[key]}})
        upd_id_result_7d_cl_2 = collection_prisma_7d.update_one({'_id': merge_din_cl_2[key], 'cluster': 2},
                                                                {"$set": {'event_id_12d': key}})
        print(f'Added event_id_1 - {key} - {upd_id_result_12d_cl_2.raw_result}')
        print(f'Added event_id_2 - {merge_din_cl_2[key]} - {upd_id_result_7d_cl_2.raw_result}')


if __name__ == '__main__':
    date_time_start = datetime.date(2021, 12, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 12, 31)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        update_with_add_cluster_id(date)
    print('test')
