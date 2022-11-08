# import datetime

from collections import defaultdict

import pandas as pd


class ProccessingPrismaCl:
    def __init__(self, n_data):
        self.n_data = n_data

    def period_processing_for_report(self, start_date, end_date):
        worktime_dict = defaultdict(list)
        n_vs_zero_tr_dict = defaultdict(list)
        event_counter_fr_4 = defaultdict(list)
        breaks_dict = defaultdict(list)
        count_rate_amp_5_fr_2 = defaultdict(list)
        count_rate_amp_10_fr_1 = defaultdict(list)

        for single_date in pd.date_range(start_date, end_date):
            worktime_dict['Date'].append(single_date)
            n_vs_zero_tr_dict['Date'].append(single_date)
            count_rate_amp_5_fr_2['Date'].append(single_date)
            count_rate_amp_10_fr_1['Date'].append(single_date)
            event_counter_fr_4['Date'].append(single_date)

            single_n_data = self.n_data[self.n_data['Date'] == single_date].reset_index(drop=True)

            if len(single_n_data) == 0:
                worktime_dict['Worktime'].append(0.00)
                for i in range(16):
                    n_vs_zero_tr_dict[f'n{i + 1}'].append(0.00)
                    count_rate_amp_5_fr_2[f'amp{i + 1}'].append(0.00)
                    count_rate_amp_10_fr_1[f'amp{i + 1}'].append(0.00)
                event_counter_fr_4['Events'].append(0.00)
                continue

            break_time_dict, worktime_item = self._counting_break_time(single_n_data, delta_time_crit=600)
            worktime_dict['Worktime'].append(worktime_item)
            # print(break_time_dict)
            if break_time_dict:
                breaks_dict['Date'].extend([single_date.date()] * len(break_time_dict['StartSeconds']))
                breaks_dict['StartSeconds'].extend(break_time_dict['StartSeconds'])
                breaks_dict['EndSeconds'].extend(break_time_dict['EndSeconds'])
            neutron_to_zero_trigger = self._neutron_to_zero_trigger(single_n_data)
            for i in range(16):
                n_vs_zero_tr_dict[f'n{i + 1}'].append(neutron_to_zero_trigger[i])
                count_rate_amp_5_fr_2[f'amp{i + 1}'].append(
                    self._set_event_counter(single_n_data, a_crit=6, freq=2)['count_rate'][i + 1])
                count_rate_amp_10_fr_1[f'amp{i + 1}'].append(
                    self._set_event_counter(single_n_data, a_crit=11, freq=1)['count_rate'][i + 1])

            event_counter_fr_4['Events'].append(
                self._set_event_counter(single_n_data, a_crit=6, freq=4)['sum_events'])

        amp_5_fr_2_frame = self.set_amp_df(a_crit=6, freq=2)
        amp_10_fr_1_frame = self.set_amp_df(a_crit=11, freq=1)
        worktime_frame = pd.DataFrame(worktime_dict)
        n_vs_zero_tr_frame = pd.DataFrame(n_vs_zero_tr_dict)
        breaks_frame = pd.DataFrame(breaks_dict)
        event_counter_fr_4 = pd.DataFrame(event_counter_fr_4)
        count_rate_amp_5_fr_2 = pd.DataFrame(count_rate_amp_5_fr_2)
        count_rate_amp_10_fr_1 = pd.DataFrame(count_rate_amp_10_fr_1)

        for column in [f'amp{i}' for i in range(1, 17)]:
            count_rate_amp_5_fr_2[column] = count_rate_amp_5_fr_2[column] / worktime_frame['Worktime']
            count_rate_amp_10_fr_1[column] = count_rate_amp_10_fr_1[column] / worktime_frame['Worktime']

        return worktime_frame, breaks_frame, n_vs_zero_tr_frame, event_counter_fr_4, count_rate_amp_5_fr_2, count_rate_amp_10_fr_1, amp_5_fr_2_frame, amp_10_fr_1_frame

    @staticmethod
    def _counting_break_time(n_file, delta_time_crit):
        """Метод, выявляющий в n-file 5-минутки, когда кластер не работал, возвращает начальное время остановки и
        конечное время остановки"""
        time_difference = n_file['time'].diff()
        daily_breaks_dict = defaultdict(list)
        worktime_item = 24.00
        # print(f'{time_difference=}')
        for i in range(1, len(time_difference)):
            if time_difference[i] > delta_time_crit:
                daily_breaks_dict['StartSeconds'].append(n_file['time'][i - 1])
                daily_breaks_dict['EndSeconds'].append(n_file['time'][i])
        if n_file['time'][0] > delta_time_crit:
            daily_breaks_dict['StartSeconds'].append(0)
            daily_breaks_dict['EndSeconds'].append(n_file['time'][0])
        if n_file['time'][len(n_file['time']) - 1] < 86400 - delta_time_crit:
            daily_breaks_dict['StartSeconds'].append(n_file['time'][len(n_file['time']) - 1])
            daily_breaks_dict['EndSeconds'].append(86399)
        if daily_breaks_dict:
            worktime_item = worktime_item - round(sum(
                [daily_breaks_dict['EndSeconds'][index] - daily_breaks_dict['StartSeconds'][index] for index in
                 range(len(daily_breaks_dict['StartSeconds']))]) / 3600, 2)
            return daily_breaks_dict, worktime_item
        else:
            return None, worktime_item

    @staticmethod
    def _neutron_to_zero_trigger(n_file):
        """Метод, обрабатывающий данные n-файлов ПРИЗМА-32, дающий на выходе нормированное число в событии,
        отобранных как нейтрон, при самозапуске"""
        counter_zero_tr = len(n_file[n_file['trigger'] == 0].index)
        zero_tr_frame = n_file[n_file['trigger'] == 0]
        return [round(zero_tr_frame[f'n{i}'].sum() / counter_zero_tr, 3) for i in range(1, 17)]

    # except ZeroDivisionError: Нужно дописать, если допустим нет нулевых триггеров

    @staticmethod
    def _set_event_counter(n_file, a_crit, freq):
        """Метод, обрабатывающий данные n-файлов ПРИЗМА-32, на вход подаются определенная амплитуда и количество
        детекторов, на которых амплитуда превышает заданную, на выходе метод возвращает словарь с параметрами:
        1) суммарное число событий на кластере, подходящих под заданные условия;
        2) датафрейм с амплитудами детекторов для каждого события, подходящего под заданные условия;
        3) количество превышений заданной амплитуды у детектора в событиях, подходящих под заданные условия; """
        cluster_count_rate = {}
        amp_frame = n_file[[f'amp{i}' for i in range(1, 17)]]
        amp_frame['fr_sum'] = amp_frame.isin(range(a_crit, 520)).sum(axis=1, skipna=True)  # noqa
        amp_frame = amp_frame[amp_frame['fr_sum'] >= freq].reset_index(drop=True)
        for i in range(1, 17):
            cluster_count_rate[i] = len(amp_frame[amp_frame[f'amp{i}'] >= a_crit])
        return {'sum_events': len(amp_frame.index),
                'count_rate': cluster_count_rate}

    def set_amp_df(self, a_crit, freq):
        amp_frame = self.n_data[[f'amp{i}' for i in range(1, 17)]]
        amp_frame['fr_sum'] = amp_frame.isin(range(a_crit, 520)).sum(axis=1, skipna=True)  # noqa
        amp_frame = amp_frame[amp_frame['fr_sum'] >= freq].reset_index(drop=True)
        return amp_frame[[f'amp{i}' for i in range(1, 17)]]
