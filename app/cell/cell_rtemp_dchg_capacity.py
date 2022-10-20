import statistics
import json
import re
import os

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable
from common.postgres_driver import create_conn, fetch_one
from common.log import get_logger


class CellRtempDchgCapacity(DefaultItem):
    def preprocess(self):
        """
        The function extracts the data from the csv file and returns a list of lists
        :return: The return value is a tuple of two elements. The first element is a boolean value
        indicating whether the operation is successful. The second element is the result of the operation.
        """

        def extract_file(csv_file, rated_capacity):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: The name of the CSV file to be processed
            :param rated_capacity: The rated capacity of the battery in kWh
            """

            try:
                if self.gbt == "GB 38031-2020":
                    df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Cycle', 'Step', 'Step time, S', 'Power, W', 'Amp-Hours, AH'])
                    df = df.dropna(how='any')

                    max_cycle = int(df[df['Power, W'] < 0]['Cycle'].max())
                    dchg_step = int(df[(df['Power, W'] < 0) & (df.Cycle == max_cycle)]["Step"].max())

                    dchg = []

                    for i in range(2, max_cycle + 1):
                        max_index = df[(df.Cycle == i) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

                        dchg.append(round(-1 * df.loc[max_index, 'Amp-Hours, AH'], 2))

                    return True, [csv_file.split('/')[-3]] + dchg + [f"{(dchg[-1] - dchg[-2]) / rated_capacity:.2%}"]
                else:
                    df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Cycle', 'Step', 'Step time, S', 'Voltage, V', 'Power, W', 'Amp-Hours, AH'])
                    df = df.dropna(how='any')

                    max_cycle = int(df[df['Power, W'] < 0]['Cycle'].max())
                    dchg_step = int(df[(df['Power, W'] < 0) & (df.Cycle == max_cycle)]["Step"].max())

                    dchg = []

                    for i in range(2, -1, -1):
                        max_index = df[(df.Cycle == max_cycle - i) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

                        dchg.append(round(-1 * df.loc[max_index, 'Amp-Hours, AH'], 2))

                    df = df[(df.Cycle == max_cycle) & (df.Step == dchg_step)][['Amp-Hours, AH', "Voltage, V"]]
                    df['Amp-Hours, AH'] = (-1 * df['Amp-Hours, AH']).round(2)

                    return True, [csv_file.split('/')[-3]] + dchg + [round(statistics.mean(dchg), 2)] + [df.to_numpy().tolist()]
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"

        sql = f"SELECT value FROM {DatabaseTable.PRODUCT} WHERE task_id='{self.task_id}' AND category='{self.category}' AND name='额定容量（Ah）'"

        with create_conn() as conn:
            ret, rated_capacity = fetch_one(conn, sql)

        if ret is False:
            return ret, rated_capacity

        if rated_capacity is None or rated_capacity[0] is None:
            return False, "Can not find 额定容量（Ah）."

        rated_capacity = float(rated_capacity[0])

        csv_files = []

        with os.scandir(self.item_path) as item_it:
            for item_entry in item_it:
                if os.path.exists(os.path.join(item_entry.path, "设备数据")):
                    with os.scandir(os.path.join(item_entry.path, "设备数据")) as data_it:
                        for data_entry in data_it:
                            if data_entry.name.endswith(".csv"):
                                csv_files.append(data_entry.path)

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(csv_file, rated_capacity) for csv_file in csv_files)

        if len(extract_results) == 0 or any([result[0] is False for result in extract_results]):
            return True, {}

        extract_results = [result[1] for result in extract_results]

        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        if self.gbt == "GB 38031-2020":
            item_result = {
                "table": [['样本编号'] + [f"C{i+1}(第{i+1}轮循环放电容量(AH))" for i in range(len(extract_results[0]) - 2)] + [f"|C{len(extract_results[0]) - 2}-C{len(extract_results[0]) - 3}|/额定放电容量"]]
            }

            for result in extract_results:
                item_result["table"].append(result)

            return True, item_result

        max_sample = None
        min_sample = None
        max_index = None
        min_index = None
        max_mean = 0
        min_mean = 0

        for index, sample in enumerate(extract_results):
            if max_sample is None or sample[-2] > max_mean:
                max_sample = sample[0]
                max_index = index
                max_mean = sample[-2]

            if min_sample is None or sample[-2] <= min_mean:
                min_sample = sample[0]
                min_index = index
                min_mean = sample[-2]

        extremum_capacity = max_mean - min_mean
        mean_capacity = statistics.mean([s[-2] for s in extract_results])

        item_result = {
            "table": [['样本编号', '第1轮循环放电容量(AH)', '第2轮循环放电容量(AH)', '第3轮循环放电容量(AH)', '平均放电容量(AH)']] + [s[0:5] for s in extract_results],
            "list": [
                ['样本平均放电容量极差(AH)', round(extremum_capacity, 2)],
                ['样本总平均放电容量(AH)', round(mean_capacity, 2)],
                ['样本放电容量极差百分比', f"{extremum_capacity / mean_capacity:.2%}"],
                ['样本额定放电容量(AH)', round(rated_capacity, 2)],
                ['样本平均放电容量最大值/样本额定放电容量', f"{max_mean / rated_capacity:.2%}"],
                ['样本平均放电容量最小值/样本额定放电容量', f"{min_mean / rated_capacity:.2%}"]
            ],
            "graph": [{"电压与放电容量曲线": ["放电容量(Ah)", "电压(V)"]}, {"keys": [max_sample, min_sample]}, {"values": [extract_results[max_index][-1], extract_results[min_index][-1]]}]
        }

        return True, item_result