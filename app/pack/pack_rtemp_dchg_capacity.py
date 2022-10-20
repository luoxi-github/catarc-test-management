import json
import re
import csv
import os

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable
from common.postgres_driver import create_conn, fetch_one
from common.log import get_logger


class PackRtempDchgCapacity(DefaultItem):
    def preprocess(self):
        """
        It extracts the data from the device and saves it to the computer
        :return: The return value is a tuple of two values. The first value is a boolean value indicating
        whether the operation is successful. The second value is the result of the operation.
        """

        def extract_file(dev_path, rated_capacity):
            """
            This function extracts the file from the device and saves it to the computer
            
            :param dev_path: The path to the device that is to be tested
            :param rated_capacity: The rated capacity of the battery in mAh
            """

            try:
                dat_files = []
                row = []

                with os.scandir(os.path.join(dev_path, "设备数据")) as data_it:
                    for data_entry in data_it:
                        if data_entry.name.endswith(".dat"):
                            dat_files.append(data_entry.path)

                if len(dat_files) > 0:
                    dat_files.sort()

                    dchg_list = []

                    for dat_file in dat_files:
                        df = pandas.read_csv(dat_file, header=0, usecols=['TestTime.1', 'ABCCurrent', 'ABCAhOut', 'ABCCommandMode', 'StopCondition'], encoding='utf-8', delim_whitespace=True, quoting=csv.QUOTE_NONE)
                        df.index = [i for i in range(df.index.size)]

                        try:
                            index_min = min(df[(df['ABCCurrent'] < 0) & (df['ABCCommandMode'] == 1) & (df['StopCondition'] == 1)].index)
                            index_max = max(df[(df['ABCCurrent'] < 0) & (df['ABCCommandMode'] == 1) & (df['StopCondition'] == 1)].index)

                            if index_min > 0:
                                index_min = index_min - 1

                            if len(dchg_list) > 0 and df.loc[index_min, 'TestTime.1'] - dchg_list[-1]["time"] < 10:
                                dchg_list.pop()
                            else:
                                dchg_list.append({"ah": df.loc[index_min, 'ABCAhOut'], "time": df.loc[index_min, 'TestTime.1']})
                            
                            if len(dchg_list) > 0 and df.loc[index_max, 'TestTime.1'] - dchg_list[-1]["time"] < 10:
                                dchg_list.pop()
                            else:
                                dchg_list.append({"ah": df.loc[index_max, 'ABCAhOut'], "time": df.loc[index_max, 'TestTime.1']})
                        except:
                            pass

                    if len(dchg_list) > 5 and len(dchg_list) % 2 == 0:
                        row.append(dev_path.split('/')[-1])

                        dchg_list = dchg_list[2:]

                        for i in range(int(len(dchg_list) / 2)):
                            row.append(round(dchg_list[2*i]["ah"] - dchg_list[2*i + 1]["ah"], 2))

                        row.append(f"{(row[-1] - row[-2]) / rated_capacity:.2%}")

                return True, row
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

        rated_capacity = float(re.sub(r'\D+', '', rated_capacity[0]))

        dev_paths = []

        with os.scandir(self.item_path) as item_it:
            for item_entry in item_it:
                if os.path.exists(os.path.join(item_entry.path, "设备数据")):
                    dev_paths.append(item_entry.path)

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(dev_path, rated_capacity) for dev_path in dev_paths)

        if len(extract_results) == 0 or any([result[0] is False for result in extract_results]):
            return True, {}

        extract_results = [result[1] for result in extract_results]

        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        item_result = {
            "table": [['样本编号'] + [f"C{i+1}(第{i+1}轮循环放电容量(AH))" for i in range(len(extract_results[0]) - 2)] + [f"|C{len(extract_results[0]) - 2}-C{len(extract_results[0]) - 3}|/额定放电容量"]]
        }

        for result in extract_results:
            item_result["table"].append(result)

        return True, item_result