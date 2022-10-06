import datetime
from noSQL_db_Prisma import NoSQLPrisma


date_time_start = datetime.date(2022, 9, 14)  # посмотреть почему не собирается конец дня 2018-04-22
# date_time_stop = datetime.date(2020, 10, 28)
date_time_stop = datetime.date.today()
LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                 range((date_time_stop - date_time_start).days + 1)]
for date in LIST_OF_DATES:
    try:
        NoSQLPrisma(cluster=1, single_date=date).prisma_12d_past_data_copier()
    except FileNotFoundError:
        print('нет')
        with open('files_not_found.txt', 'a+') as f:
            f.write(f'n-файла 1-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=2, single_date=date).prisma_12d_past_data_copier()
    except FileNotFoundError:

        with open('files_not_found.txt', 'a+') as f:
            f.write(f'n-файла 2-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=1, single_date=date).prisma_7d_past_data_copier()
    except FileNotFoundError:
        with open('files_not_found.txt', 'a+') as f:
            f.write(f'n7-файла 1-го кластера от {date} не существует\n')
    try:
        NoSQLPrisma(cluster=2, single_date=date).prisma_7d_past_data_copier()
    except FileNotFoundError:
        with open('files_not_found.txt', 'a+') as f:
            f.write(f'n7-файла 2-го кластера от {date} не существует\n')
print('test')
