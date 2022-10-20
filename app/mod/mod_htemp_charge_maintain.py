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


class ModHtempChargeMaintain(DefaultItem):
    def preprocess(self):
        """
        The function extracts the data from the csv file and returns a list of lists
        """

        def extract_file(csv_file):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: the file to extract the data from
            """

            try:
                df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Total Time, S', 'Cycle', 'Step', 'Step time, S', 'Power, W', 'Amp Hours Discharge, AH'])
                df = df.dropna(how='any')

                discharge_index = df[(df["Power, W"] < 0) & (df["Step time, S"] == 1)].index

                maintain_start = discharge_index[-2]
                recover_start = discharge_index[-1]

                maintain_end = df[(df.Cycle == df.loc[maintain_start, "Cycle"]) & (df.Step == df.loc[maintain_start, "Step"])]['Total Time, S'].astype('float64').idxmax()
                recover_end = df[(df.Cycle == df.loc[recover_start, "Cycle"]) & (df.Step == df.loc[recover_start, "Step"])]['Total Time, S'].astype('float64').idxmax()

                return True, [csv_file.split('/')[-3], df.loc[maintain_start - 1, 'Amp Hours Discharge, AH'] - df.loc[maintain_end, 'Amp Hours Discharge, AH'], df.loc[recover_start - 1, 'Amp Hours Discharge, AH'] - df.loc[recover_end, 'Amp Hours Discharge, AH']]
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

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(csv_file,) for csv_file in csv_files)

        if len(extract_results) == 0 or any([result[0] is False for result in extract_results]):
            return True, {}

        extract_results = [result[1] for result in extract_results]

        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        item_result = {
            "table": [["样本编号", "室温放电容量", "荷电保持容量", "恢复容量", "荷电保持容量/室温放电容量", "恢复容量/室温放电容量", "是否合格"]]
        }

        for result in extract_results:
            item_result["table"].append([result[0], round(mrdc[result[0]], 2), round(result[1], 2), round(result[2], 2), f"{result[1] / mrdc[result[0]]:.2%}", f"{result[2] / mrdc[result[0]]:.2%}", "合格" if result[1] / mrdc[result[0]] >= 0.85 and result[2] / mrdc[result[0]] >= 0.9 else "不合格"])

        return True, item_result