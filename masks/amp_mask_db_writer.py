import datetime
import time

import pandas as pd

# import numpy as np
import pymongo

from config_info.config import DB_URL

# from file_reader.db_file_reader import DbFileReader

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def amp_mask_db_writer(cluster, start_date, end_date, path_to_mask):
    """Запись из excel файла данных амплитудной маски в БД, за указанный промежуток времени."""
    # Кажется нужно указать только год и месяц, который мы хотим записать в БД из excel.
    mask_amp = pd.read_excel(f'{path_to_mask}\\{cluster}cl_amp_mask_{start_date.year}.xlsx',
                             sheet_name=f'{start_date.year}-{start_date.month:02}')
    for i in mask_amp.index:
        daily_mask_data = mask_amp[[f"amp{i}_mask" for i in range(1, 17)]].iloc[0, :].tolist()
        collection_prisma = prisma_db[f'{str(mask_amp["date"][i].date())}_12d']
        upd_result = collection_prisma.update_many({'cluster': cluster},
                                                   {"$set": {
                                                       f'mask_of_hit_counters_amp': int(
                                                           ''.join(map(lambda x: str(x), daily_mask_data)), 2),
                                                       f'multiplicity_of_hit_counters_amp': sum(daily_mask_data)}})
        print(f'Matched documents amp_mask_{cluster} - {upd_result.matched_count}')
        print(f'Modified documents amp_mask_{cluster} - {upd_result.modified_count}')


if __name__ == "__main__":
    t1 = time.time_ns()
    amp_mask_db_writer(cluster=1, start_date=datetime.date(2021, 1, 1), end_date=datetime.date(2021, 1, 31),
                       path_to_mask="C:\\Users\\pad_z\\OneDrive\\Рабочий стол\\PrismaPassport\\amp_mask")
    print(f'time - {(time.time_ns() - t1) / 1e9}')
