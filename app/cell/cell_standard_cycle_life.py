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


class CellStandardCycleLife(DefaultItem):
    def preprocess(self):
        """
        It reads a bunch of csv files, extracts some data from them, and returns a dictionary
        :return: The return value is a tuple of two values. The first value is a boolean value indicating
        whether the operation is successful. The second value is the result of the operation.
        """

        def extract_file(csv_file, rdc):
            """
            This function extracts the data from a csv file and returns a list of dictionaries
            
            :param csv_file: the name of the csv file that contains the data
            :param rdc: the root directory where the files are located
            """

            try:
                df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Cycle', 'Step', 'Step time, S', 'Voltage, V', 'Power, W', 'Amp-Hours, AH'])
                df = df.dropna(how='any')

                max_cycle = int(df[df['Power, W'] < 0]['Cycle'].max())
                dchg_step = int(df[(df['Power, W'] < 0) & (df.Cycle == max_cycle)]["Step"].max())

                dchg = []

                for i in range(2, max_cycle+1):
                    max_index = df[(df.Cycle == i) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

                    dchg.append(round(-1 * df.loc[max_index, 'Amp-Hours, AH'], 2))

                if max_cycle > 1000:
                    return True, [csv_file.split('/')[-3]] + [round(dchg[99], 2), round(dchg[199], 2), round(dchg[299], 2), round(dchg[399], 2), round(dchg[499], 2), round(dchg[999], 2)] + [round(rdc, 2)] + [f"{dchg[499] / rdc:.2%}", f"{dchg[999] / rdc:.2%}"] + [[[i+1, ah] for i, ah in enumerate(dchg)]]

                return True, [csv_file.split('/')[-3]] + [round(dchg[99], 2), round(dchg[199], 2), round(dchg[299], 2), round(dchg[399], 2), round(dchg[499], 2)] + [round(rdc, 2)] + [f"{dchg[499] / rdc:.2%}"] + [[[i+1, ah] for i, ah in enumerate(dchg)]]
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"


        sql = f"SELECT result FROM {DatabaseTable.STAT} WHERE task_id='{self.task_id}' AND gbt='{self.gbt}' AND category='{self.category}' AND item='室温放电容量'"

        with create_conn() as conn:
            ret, mrdc = fetch_one(conn, sql)

        if ret is False:
            return ret, mrdc

        if mrdc is None or mrdc[0] is None:
            return False, "Please parse item(室温放电容量) first."

        mrdc = json.loads(mrdc[0])

        mrdc = {row[0]: row[-1] for row in mrdc["table"][1:]}

        csv_files = []

        with os.scandir(self.item_path) as item_it:
            for item_entry in item_it:
                if os.path.exists(os.path.join(item_entry.path, "设备数据")):
                    with os.scandir(os.path.join(item_entry.path, "设备数据")) as data_it:
                        for data_entry in data_it:
                            if data_entry.name.endswith(".csv"):
                                csv_files.append(data_entry.path)

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(csv_file, mrdc[csv_file.split('/')[-3]]) for csv_file in csv_files)

        if len(extract_results) == 0 or any([result[0] is False for result in extract_results]):
            return True, {}

        extract_results = [result[1] for result in extract_results]

        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        if len(extract_results[0]) > 9:
            item_result = {
                "table": [['样本编号', '第100轮循环放电容量(AH)', '第200轮循环放电容量(AH)', '第300轮循环放电容量(AH)', '第400轮循环放电容量(AH)', '第500轮循环放电容量(AH)', '第1000轮循环放电容量(AH)', '室温放电容量(AH)', '第500轮循环放电容量/室温放电容量(AH)', '第1000轮循环放电容量/室温放电容量(AH)']],
                "graph": [{"放电容量与循环次数曲线": ["循环次数(次)", "放电容量(Ah)"]}, {"keys": []}, {"values": []}]
            }
        else:
            item_result = {
                "table": [['样本编号', '第100轮循环放电容量(AH)', '第200轮循环放电容量(AH)', '第300轮循环放电容量(AH)', '第400轮循环放电容量(AH)', '第500轮循环放电容量(AH)', '室温放电容量(AH)', '第500轮循环放电容量/室温放电容量(AH)']],
                "graph": [{"放电容量与循环次数曲线": ["循环次数(次)", "放电容量(Ah)"]}, {"keys": []}, {"values": []}]
            }

        for result in extract_results:
            item_result["table"].append(result[0:-1])

            item_result["graph"][1]["keys"].append(result[0])
            item_result["graph"][2]["values"].append(result[-1])

        return True, item_result