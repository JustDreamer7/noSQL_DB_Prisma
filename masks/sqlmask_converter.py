import datetime

import sqlalchemy
import pandas as pd
import pymongo

from config_info.config import DB_URL

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def sqlmask_converter_to_nosql(cluster, start_date, end_date, mask_type):
    """Переводим маску из SQL БД в бинарный вид для транспортировки в noSQL БД.
    SQL вид существует для 2012-02-01::2021-12-31"""
    conn = 'postgresql+psycopg2://postgres:qwerty@localhost:5432/prisma'
    engine = sqlalchemy.create_engine(conn)
    connect = engine.connect()
    mask_prisma = pd.read_sql(
        f"SELECT * FROM mask_{cluster}_params WHERE date >= "
        f"'{start_date.year}-{start_date.month:02}-{start_date.day:02}' AND  date <= \
           '{end_date.year}-{end_date.month:02}-{end_date.day:02}' ORDER BY date asc;", connect)
    data_prisma_mask = mask_prisma[[f'{mask_type}{i}_mask' for i in range(16, 0, -1)]]

    for i in data_prisma_mask.index:
        mask_params = data_prisma_mask.iloc[i]

        collection_prisma = prisma_db[f'{str(mask_prisma["date"][i])}_12d']
        upd_result = collection_prisma.update_many({'cluster': cluster},
                                                   {"$set": {
                                                       f'mask_of_hit_counters_{mask_type}': int(
                                                           ''.join(mask_params.astype(str)), 2),
                                                       f'multiplicity_of_hit_counters_{mask_type}': sum(mask_params)}})
        print(f'Matched documents {mask_type}_mask_{cluster} - {upd_result.matched_count}')
        print(f'Modified documents {mask_type}_mask_{cluster} - {upd_result.modified_count}')


if __name__ == '__main__':
    date_time_start = datetime.date(2012, 12, 1)
    date_time_stop = datetime.date(2021, 12, 31)
    sqlmask_converter_to_nosql(cluster=1, start_date=date_time_start, end_date=date_time_stop, mask_type='amp')
    sqlmask_converter_to_nosql(cluster=2, start_date=date_time_start, end_date=date_time_stop, mask_type='amp')
    sqlmask_converter_to_nosql(cluster=1, start_date=date_time_start, end_date=date_time_stop, mask_type='n')
    sqlmask_converter_to_nosql(cluster=2, start_date=date_time_start, end_date=date_time_stop, mask_type='n')
    print('test')
