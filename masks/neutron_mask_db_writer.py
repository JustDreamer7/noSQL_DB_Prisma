import datetime
import time

import pandas as pd
import pymongo

from processing_data_prisma import ProccessingPrismaCl
from file_reader.db_file_reader import DbFileReader


def neutron_mask_db_writer(cluster, start_date, end_date, path_to_db):
    for single_date in pd.date_range(start_date, end_date):
        db_file_reader = DbFileReader(cluster=cluster, single_date=single_date, db_url=path_to_db)
        data_cl = db_file_reader.reading_db()
        neutron_to_zero_trigger = ProccessingPrismaCl._neutron_to_zero_trigger(n_file=data_cl)
        n_mask_params = [int(val) for val in list(map(lambda x: x < 0.1, neutron_to_zero_trigger))]
        daily_binary_n_mask = {
            'mask_of_hit_counters_n': int(''.join([str(val) for val in n_mask_params]), 2),
            'multiplicity_of_hit_counters_n': sum(n_mask_params)}
        collection_prisma = pymongo.MongoClient(path_to_db)["prisma-32_db"][f'{str(single_date.date())}_12d']
        upd_n_result = collection_prisma.update_many({'cluster': cluster}, {"$set": daily_binary_n_mask})
        print(f'Matched documents n_mask_{cluster} - {upd_n_result.matched_count}')
        print(f'Modified documents n_mask_{cluster} - {upd_n_result.modified_count}')


if __name__ == "__main__":
    t1 = time.time_ns()
    neutron_mask_db_writer(cluster=1, start_date=datetime.date(2018, 2, 1), end_date=datetime.date(2018, 3, 31),
                           path_to_db="mongodb://localhost:27017/")
    neutron_mask_db_writer(cluster=2, start_date=datetime.date(2018, 2, 1), end_date=datetime.date(2018, 3, 31),
                           path_to_db="mongodb://localhost:27017/")
    print(f'time - {(time.time_ns()-t1)/1e9}')
