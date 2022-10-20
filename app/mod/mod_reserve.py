import json
import re
import os

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable
from common.postgres_driver import create_conn, fetch_one


class ModReserve(DefaultItem):
    def preprocess(self):
        """
        The function extracts the data from the csv file and returns a list of lists
        """

        def extract_file(csv_file):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: the file to extract the data from
            """

            df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Total Time, S', 'Cycle', 'Step', 'Step time, S', 'Power, W', 'Amp Hours Discharge, AH'])
            df = df.dropna(how='any')

            discharge_max_index = df[df["Power, W"] < 0]['Total Time, S'].astype('float64').idxmax()
            max_cycle = df.loc[discharge_max_index, "Cycle"]
            dchg_step = df.loc[discharge_max_index, "Step"]

            min_index = df[(df["Power, W"] < 0) & (df.Cycle == max_cycle) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmin()
            max_index = df[(df["Power, W"] < 0) & (df.Cycle == max_cycle) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

            return [csv_file.split('/')[-3], df.loc[min_index - 1, 'Amp Hours Discharge, AH'] - df.loc[max_index, 'Amp Hours Discharge, AH']]

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

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(csv_file,) for csv_file in csv_files)
        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        item_result = {
            "table": [["样本编号", "室温放电容量", "储存后放电容量", "储存后放电容量/室温放电容量", "是否合格"]]
        }

        for result in extract_results:
            item_result["table"].append([result[0], round(mrdc[result[0]], 2), round(result[1], 2), f"{result[1] / mrdc[result[0]]:.2%}", "合格" if result[1] / mrdc[result[0]] >= 0.9 else "不合格"])

        return True, item_result