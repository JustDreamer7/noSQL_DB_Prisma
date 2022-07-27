import pandas as pd
import pymongo
from collections import defaultdict

from config_info.config import *


def prepare_to_merge_12d(date, cluster, a_crit, fr):
    """Накладываем на события из noSQL БД маску и отбираем нужные для сшивки по критериям отбора"""
    data_cl = pd.DataFrame.from_records(
        pymongo.MongoClient(DB_URL)["prisma-32_db"][f'{str(date)}_12d'].find({'cluster': cluster}))

    amp_dict = defaultdict(list)
    n_dict = defaultdict(list)
    for item in data_cl['detectors']:
        for j in [f'det_{i:02}' for i in range(1, 17)]:
            amp_dict[j].append(item[j]['amplitude'])
            n_dict[j].append(item[j]['neutrons'])

    amp_mask, n_mask = defaultdict(list), defaultdict(list)
    for amp_mask_event, n_mask_event in zip(data_cl['mask_of_hit_counters_a'], data_cl['mask_of_hit_counters_n']):
        for i in range(16):
            amp_mask[i + 1].append(list(f'{int(bin(amp_mask_event)[2:]):016}')[i])
            n_mask[i + 1].append(list(f'{int(bin(n_mask_event)[2:]):016}')[i])

    for i in range(1, 17):
        data_cl[f'amp_{i}'] = amp_dict[f'det_{i:02}']
        data_cl[f'n_{i}'] = n_dict[f'det_{i:02}']
        data_cl[f'amp_{i}_mask'] = amp_mask[i]
        data_cl[f'n_{i}_mask'] = n_mask[i]

    for i in range(1, 17):  # Нужны разные алгоритмы, с выборочным применением масок
        data_cl.loc[data_cl[data_cl[f'amp_{i}_mask'] == 0].index, f'amp_{i}'] = 0
        data_cl.loc[data_cl[data_cl[f'n_{i}_mask'] == 0].index, f'n_{i}'] = 0

    data_cl['fr_sum'] = data_cl[
        ['amp_1', 'amp_2', 'amp_3', 'amp_4', 'amp_5', 'amp_6', 'amp_7', 'amp_8', 'amp_9', 'amp_10', 'amp_11', 'amp_12',
         'amp_13', 'amp_14', 'amp_15', 'amp_16']].isin(range(a_crit, 550)).sum(axis=1, skipna=True)
    data_cl = data_cl.loc[data_cl[data_cl['fr_sum'] >= fr].index, :].reset_index(
        drop=True)

    return data_cl


def prepare_to_merge_7d(date, cluster):
    data_cl = pd.DataFrame.from_records(
        pymongo.MongoClient(DB_URL)["prisma-32_db"][f'{str(date)}_7d'].find({'cluster': cluster}))

    amp_dict = defaultdict(list)
    for item in data_cl['detectors']:
        for j in [f'det_{i:02}' for i in range(1, 17)]:
            amp_dict[j].append(item[j]['amplitude'])

    for i in range(1, 17):
        data_cl[f'amp_{i}'] = amp_dict[f'det_{i:02}']

    return data_cl
