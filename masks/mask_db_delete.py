import datetime
import time

import pandas as pd

# import numpy as np
import pymongo

from config_info.config import DB_URL

# from file_reader.db_file_reader import DbFileReader

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def mask_db_delete(cluster, start_date, end_date, mask_type):
    """Удаление данных нейтронной или амплитудной маски, за выделенный промежуток времени."""
    for single_date in pd.date_range(start_date, end_date):
        collection_prisma = prisma_db[f'{str(single_date.date())}_12d']
        upd_result = collection_prisma.update_many({'cluster': cluster},
                                                   {"$unset": {
                                                       f'mask_of_hit_counters_{mask_type}': "",
                                                       f'multiplicity_of_hit_counters_{mask_type}': ""}})
        print(f'Matched documents {mask_type}_mask_{cluster} - {upd_result.matched_count}')
        print(f'Modified documents {mask_type}_mask_{cluster} - {upd_result.modified_count}')


if __name__ == "__main__":
    t1 = time.time_ns()
    mask_db_delete(cluster=1, start_date=datetime.date(2012, 1, 1), end_date=datetime.date(2021, 1, 31),
                   mask_type='amp')
    mask_db_delete(cluster=2, start_date=datetime.date(2012, 1, 1), end_date=datetime.date(2021, 1, 31),
                   mask_type='amp')
    mask_db_delete(cluster=1, start_date=datetime.date(2012, 1, 1), end_date=datetime.date(2021, 1, 31),
                   mask_type='n')
    mask_db_delete(cluster=2, start_date=datetime.date(2012, 1, 1), end_date=datetime.date(2021, 1, 31),
                   mask_type='n')
    print(f'time - {(time.time_ns() - t1) / 1e9}')
