import datetime


def eas_converter(date_, cluster):
    """Конвертер для eas-файлов, то есть для осциллограмм высокоэнергичных событий. На вход подается суточный eas-файл,
    который разделяется по одному событию и каждое из них записывается в новый txt-файл, названный как id события
    в базе данных Mongo_DB, с тем отличие, что вместо <<:>> стоят <<.>>"""
    eas_template = f"{cluster}eas{date_.month:02}-{date_.day:02}.{date_.year - 2000:02}"
    try:
        with open(f'D:\\PRISMA20\\P{cluster}\\EAS\\{eas_template}') as f:
            eas_file = f.readlines()
        eas_file = [line.rstrip() for line in eas_file]

        event_list = []
        main_list = []
        sep = 0
        for i in range(len(eas_file)):
            if eas_file[i] == '*#*':
                main_list.append(eas_file[sep].split(' '))
                event_list.append(eas_file[sep + 1:i])
                sep = i + 1

        for i in range(len(main_list)):
            main_list[i] = list(iter(filter(lambda x: x != '', main_list[i])))

        det_razvertka = []
        for block in event_list:
            det_per_event = []
            for line in block:
                event = line.split(' ')[1:]
                det_per_event.append(event)
            det_razvertka.append(det_per_event)

        for i in range(len(main_list)):
            main_list[i].extend(det_razvertka[i])

        result_path = 'D:\\PRISMA20\\eas\\'
        for i in range(len(main_list)):
            event_time = str(datetime.timedelta(seconds=float(main_list[i][0]))).replace(':', '.')
            with open(
                    f'{result_path}{date_.year}-{date_.month:02}-{date_.day:02}_{cluster:02}_12d_{event_time[:len(event_time) - 3]}.000.000.txt',
                    'w') as out:
                out.write(' '.join(main_list[i][0:4]) + '\n')
                for row in main_list[i][4:]:
                    out.write(' '.join(row) + '\n')
            print(
                f'Copied - {date_.year}-{date_.month:02}-{date_.day:02}_{cluster:02}_12d_{event_time[:len(event_time) - 3]}.000.000.txt')
    except FileNotFoundError:
        print(f'File does not exist - {eas_template}')


if __name__ == '__main__':
    date_time_start = datetime.date(2021, 11, 1)  # посмотреть почему не собирается конец дня 2018-04-22
    date_time_stop = datetime.date(2021, 11, 30)
    LIST_OF_DATES = [(date_time_start + datetime.timedelta(days=i)) for i in
                     range((date_time_stop - date_time_start).days + 1)]
    for date in LIST_OF_DATES:
        eas_converter(date, 1)
        eas_converter(date, 2)
    print('test')
