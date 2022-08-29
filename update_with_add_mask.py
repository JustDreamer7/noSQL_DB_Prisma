import pymongo
import datetime

from sqlmask_converter import sqlmask_converter
from config_info.config import *

db_client = pymongo.MongoClient(DB_URL)
prisma_db = db_client["prisma-32_db"]


def update_with_add_mask(date_, binary_n_mask_1, binary_amp_mask_1, binary_n_mask_2, binary_amp_mask_2):
    collection_prisma = prisma_db[f'{str(date_)}_12d']
    upd_n_result_1 = collection_prisma.update_many({'cluster': 1}, {"$set": binary_n_mask_1[str(date_)]})
    print(f'Added n_mask_1 - {upd_n_result_1.raw_result}')
    upd_amp_result_1 = collection_prisma.update_many({'cluster': 1}, {"$set": binary_amp_mask_1[str(date_)]})
    print(f'Added amp_mask_1 - {upd_amp_result_1.raw_result}')
    upd_n_result_2 = collection_prisma.update_many({'cluster': 2}, {"$set": binary_n_mask_2[str(date_)]})
    print(f'Added n_mask_2 - {upd_n_result_2.raw_result}')
    upd_amp_result_2 = collection_prisma.update_many({'cluster': 2}, {"$set": binary_amp_mask_2[str(date_)]})
    print(f'Added amp_mask_2 - {upd_amp_result_2.raw_result}')


if __name__ == '__main__':
    date_time_start = datetime.date(2021, 12, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 12, 31)
    binary_n_mask_1, binary_amp_mask_1, binary_n_mask_2, binary_amp_mask_2 = sqlmask_converter(date_time_start,
                                                                                               date_time_stop)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        update_with_add_mask(date, binary_n_mask_1, binary_amp_mask_1, binary_n_mask_2, binary_amp_mask_2)

    print('test')
