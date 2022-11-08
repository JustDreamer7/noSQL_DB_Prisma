import datetime
from nosql_db_prisma import NoSQLPrisma


date_time_start = datetime.date(2020, 11, 11)  # посмотреть почему не собирается конец дня 2018-04-22
date_time_stop = datetime.date(2020, 11, 12)
# date_time_stop = datetime.date.today()
LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                 range((date_time_stop - date_time_start).days + 1)]
for date in LIST_OF_DATES:
    try:
        NoSQLPrisma(cluster=1, single_date=date).prisma_12d_past_data_copier()
    except FileNotFoundError:
        with open('files_not_found/n_1cl_files_not_found.txt', 'a+', encoding='utf-8') as f:
            f.write(f'n-файла 1-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=2, single_date=date).prisma_12d_past_data_copier()
    except FileNotFoundError:

        with open('files_not_found/n_2cl_files_not_found.txt', 'a+', encoding='utf-8') as f:
            f.write(f'n-файла 2-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=1, single_date=date).prisma_7d_past_data_copier()
    except FileNotFoundError:
        with open('files_not_found/n7_1cl_files_not_found.txt', 'a+', encoding='utf-8') as f:
            f.write(f'n7-файла 1-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=2, single_date=date).prisma_7d_past_data_copier()
    except FileNotFoundError:
        with open('files_not_found/n7_2cl_files_not_found.txt', 'a+', encoding='utf-8') as f:
            f.write(f'n7-файла 2-го кластера от {date} не существует\n')
print('test')
