import pandas as pd
import datetime
import sqlalchemy


def sqlmask_converter(start_date, end_date):
    """Переводим маску из SQL БД в бинарный вид для транспортировки в noSQL БД"""
    conn = 'postgresql+psycopg2://postgres:qwerty@localhost:5432/prisma'
    engine = sqlalchemy.create_engine(conn)
    connect = engine.connect()
    mask_prisma_1 = pd.read_sql("SELECT * FROM mask_1_params WHERE date >= '{}-{:02}-{:02}' AND  date <= \
           '{}-{:02}-{:02}' ORDER BY date asc;".format(start_date.year, start_date.month, start_date.day, end_date.year,
                                                       end_date.month, end_date.day), connect)
    mask_prisma_2 = pd.read_sql("SELECT * FROM mask_2_params WHERE date >= '{}-{:02}-{:02}' AND  date <= \
           '{}-{:02}-{:02}' ORDER BY date asc;".format(start_date.year, start_date.month, start_date.day, end_date.year,
                                                       end_date.month, end_date.day), connect)
    # amp_data_prisma_1_mask = mask_prisma_1[
    #     ['amp1_mask', 'amp2_mask', 'amp3_mask', 'amp4_mask', 'amp5_mask', 'amp6_mask', 'amp7_mask', 'amp8_mask',
    #      'amp9_mask', 'amp10_mask', 'amp11_mask', 'amp12_mask', 'amp13_mask',
    #      'amp14_mask', 'amp15_mask', 'amp16_mask']]
    amp_data_prisma_1_mask = mask_prisma_1[
        ['amp16_mask', 'amp15_mask', 'amp14_mask', 'amp13_mask', 'amp12_mask', 'amp11_mask', 'amp10_mask', 'amp9_mask',
         'amp8_mask', 'amp7_mask', 'amp6_mask', 'amp5_mask', 'amp4_mask',
         'amp3_mask', 'amp2_mask', 'amp1_mask']]
    amp_data_prisma_2_mask = mask_prisma_2[
        ['amp16_mask', 'amp15_mask', 'amp14_mask', 'amp13_mask', 'amp12_mask', 'amp11_mask', 'amp10_mask', 'amp9_mask',
         'amp8_mask', 'amp7_mask', 'amp6_mask', 'amp5_mask', 'amp4_mask',
         'amp3_mask', 'amp2_mask', 'amp1_mask']]

    n_data_prisma_1_mask = mask_prisma_1[
        ['n16_mask', 'n15_mask', 'n14_mask', 'n13_mask', 'n12_mask', 'n11_mask', 'n10_mask', 'n9_mask',
         'n8_mask', 'n7_mask', 'n6_mask', 'n5_mask', 'n4_mask',
         'n3_mask', 'n2_mask', 'n1_mask']]
    n_data_prisma_2_mask = mask_prisma_2[
        ['n16_mask', 'n15_mask', 'n14_mask', 'n13_mask', 'n12_mask', 'n11_mask', 'n10_mask', 'n9_mask',
         'n8_mask', 'n7_mask', 'n6_mask', 'n5_mask', 'n4_mask',
         'n3_mask', 'n2_mask', 'n1_mask']]

    binary_n_mask_1 = {}
    binary_n_mask_2 = {}
    binary_amp_mask_1 = {}
    binary_amp_mask_2 = {}

    for i in amp_data_prisma_1_mask.index:
        amp_mask_params = amp_data_prisma_1_mask.iloc[i]
        n_mask_params = n_data_prisma_1_mask.iloc[i]
        binary_amp_mask_1[str(mask_prisma_1['date'][i])] = {
            'mask_of_hit_counters_a': int(''.join(amp_mask_params.astype(str)), 2),
            'multiplicity_of_hit_counters_a': sum(amp_mask_params)}

        binary_n_mask_1[str(mask_prisma_1['date'][i])] = {
            'mask_of_hit_counters_n': int(''.join(n_mask_params.astype(str)), 2),
            'multiplicity_of_hit_counters_n': sum(n_mask_params)}

    for i in amp_data_prisma_2_mask.index:
        amp_mask_params = amp_data_prisma_2_mask.iloc[i]
        n_mask_params = n_data_prisma_2_mask.iloc[i]
        binary_amp_mask_2[str(mask_prisma_2['date'][i])] = {
            'mask_of_hit_counters_a': int(''.join(amp_mask_params.astype(str)), 2),
            'multiplicity_of_hit_counters_a': sum(amp_mask_params)}

        binary_n_mask_2[str(mask_prisma_2['date'][i])] = {
            'mask_of_hit_counters_n': int(''.join(n_mask_params.astype(str)), 2),
            'multiplicity_of_hit_counters_n': sum(n_mask_params)}

    return binary_n_mask_1, binary_amp_mask_1, binary_n_mask_2, binary_amp_mask_2


if __name__ == '__main__':
    date_time_start = datetime.date(2021, 12, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 12, 31)
    sqlmask_converter(date_time_start, date_time_stop)
    print('test')
