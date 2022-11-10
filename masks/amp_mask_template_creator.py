import datetime
import time

import pandas as pd
import numpy as np


def amp_mask_template_creator(cluster, year, path_to_mask):
    """Создание excel файла за год, в котором прописывается шаблон амплитудной маски."""
    with pd.ExcelWriter(f'{path_to_mask}\\{cluster}cl_amp_mask_{year}.xlsx') as writer:
        for period in pd.period_range(start=datetime.date(year, 1, 1), end=datetime.date(year, 12, 31), freq='M'):
            start_month = datetime.date(period.year, period.month, 1)
            end_month = datetime.date(period.year + int(period.month / 12), ((period.month % 12) + 1),
                                      1) - datetime.timedelta(days=1)
            daterange = [single_date.date() for single_date in pd.date_range(start_month, end_month)]
            amp_template = np.ones((len(daterange), 16))
            mask_amp = pd.DataFrame(amp_template.astype(int),
                                    columns=[f'amp{i}_mask' for i in range(1, 17)])
            mask_amp['date'] = daterange
            mask_amp = mask_amp[['date'] + [f'amp{i}_mask' for i in range(1, 17)]]
            mask_amp.to_excel(writer, sheet_name=str(period), index=False)


if __name__ == "__main__":
    t1 = time.time_ns()
    amp_mask_template_creator(cluster=1, year=2022,
                              path_to_mask="C:\\Users\\pad_z\\OneDrive\\Рабочий стол\\PrismaPassport\\amp_mask")
    print(f'time - {(time.time_ns() - t1) / 1e9}')
