import statistics
import json
import re
import csv
import pathlib
import os

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable
from common.log import get_logger


class PackEnergyDensity(DefaultItem):
    def preprocess(self):
        """
        It extracts the data from the device and saves it to the local file system
        :return: The return value is a tuple of two elements. The first element is a boolean value
        indicating whether the preprocessing is successful. The second element is a dictionary containing
        the preprocessing result.
        """

        def extract_file(dev_path):
            """
            This function extracts the file from the device and saves it to the local file system
            
            :param dev_path: the path to the device file where the file is located
            """

            try:
                dat_files = []
                row = []
                dchg = []
                weight = 1

                with os.scandir(os.path.join(dev_path, "设备数据")) as data_it:
                    for data_entry in data_it:
                        if data_entry.name.endswith(".dat"):
                            dat_files.append(data_entry.path)
                        elif data_entry.is_dir() and data_entry.name == "重量":
                            with os.scandir(data_entry.path) as weight_it:
                                for weight_entry in weight_it:
                                    if weight_entry.name.endswith(".txt"):
                                        weight = float(re.sub(r'\D+', '', pathlib.Path(weight_entry.path).read_text(encoding='utf8')))

                                        break

                if len(dat_files) > 0:
                    dat_files.sort()

                    df = pandas.concat([pandas.read_csv(dat_file, header=0, usecols=['cycle_1', 'ABCVoltage', 'ABCCurrent', 'ABCkWhOut', 'ABCCommandMode', 'StopCondition'], encoding='utf-8', delim_whitespace=True, quoting=csv.QUOTE_NONE) for dat_file in dat_files])

                    df.index = [i for i in range(df.index.size)]

                    max_cycle = int(df[(df['ABCCurrent'] < 0) & (df['ABCCommandMode'] == 1) & (df['StopCondition'] == 1)]['cycle_1'].max())

                    row.append(dev_path.split('/')[-1])

                    for i in range(2, -1, -1):
                        max_index = max(df[(df['cycle_1'] == max_cycle - i) & (df['ABCCurrent'] < 0) & (df['ABCCommandMode'] == 1) & (df['StopCondition'] == 1)].index)
                        dchg_df = df[(df['cycle_1'] == max_cycle - i) & (df['ABCCurrent'] < 0) & (df['ABCCommandMode'] == 1) & (df['StopCondition'] == 1)][['ABCkWhOut', 'ABCVoltage']]
                        dchg_df['ABCkWhOut'] = (-1000 * dchg_df['ABCkWhOut']).round(2)

                        row.append(round(-1 * df.loc[max_index, 'ABCkWhOut'], 2))
                        dchg.append(dchg_df.to_numpy().tolist())

                    row.append(round(statistics.mean(row[-3:]), 2))
                    row.append(weight)
                    row.append(round(row[-2] * 1000 / weight, 1))

                return True, row + dchg
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"

        item_result = {
            "table": [['样本编号', '第1轮循环放电能量(kWh)', '第2轮循环放电能量(kWh)', '第3轮循环放电能量(kWh)', '放电能量平均值(kWh)', '蓄电池系统质量(kg)', 'PED(Wh/kg)']],
            "graph": [{"电压与放电能量曲线": ["放电能量(Wh)", "电压(V)"]}, {"keys": ["第1轮循环放电", "第2轮循环放电", "第3轮循环放电"]}, {"values": []}]
        }

        with os.scandir(self.item_path) as item_it:
            for item_entry in item_it:
                if os.path.exists(os.path.join(item_entry.path, "设备数据")):
                    ret, extract_result = extract_file(item_entry.path)
                    if ret is False or len(extract_result) <= 7:
                        return True, {}

                    item_result["table"].append(extract_result[:7])
                    item_result["graph"][2]["values"] = extract_result[7:]

                    break

        return True, item_result