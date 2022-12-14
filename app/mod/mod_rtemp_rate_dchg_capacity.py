import json
import re
import os

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable, SampleCategory
from common.postgres_driver import create_conn, fetch_one
from common.log import get_logger


class ModRtempRateDchgCapacity(DefaultItem):
    def preprocess(self):
        """
        The function extracts the data from the csv file and returns a list of lists
        :return: The return value is a tuple of two values. The first value is a boolean value indicating
        whether the operation is successful. The second value is the result of the operation.
        """

        def extract_file(csv_file):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: the file to extract the data from
            """

            try:
                df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Total Time, S', 'Cycle', 'Step', 'Step time, S', 'Power, W', 'Voltage, V', 'Amp Hours Discharge, AH'])
                df = df.dropna(how='any')

                discharge_max_index = df[df["Power, W"] < 0]['Total Time, S'].astype('float64').idxmax()
                max_cycle = df.loc[discharge_max_index, "Cycle"]
                dchg_step = df.loc[discharge_max_index, "Step"]

                min_index = df[(df["Power, W"] < 0) & (df.Cycle == max_cycle) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmin()
                max_index = df[(df["Power, W"] < 0) & (df.Cycle == max_cycle) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

                init_ah = df.loc[min_index - 1, 'Amp Hours Discharge, AH']

                df = df[min_index: max_index + 1][['Amp Hours Discharge, AH', 'Voltage, V']]

                df['Amp Hours Discharge, AH'] = (init_ah - df['Amp Hours Discharge, AH']).round(2)

                return True, [csv_file.split('/')[-3], df.to_numpy().tolist()]
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"

        sql = f"SELECT value FROM {DatabaseTable.PRODUCT} WHERE task_id='{self.task_id}' AND category='{SampleCategory.CELL}' AND name='????????????'"

        with create_conn() as conn:
            ret, cell_type = fetch_one(conn, sql)

        if ret is False:
            return ret, cell_type

        if cell_type is None or cell_type[0] is None:
            return False, "Can not find ????????????."

        cell_type = cell_type[0].split('???')[-1].split('???')[0].strip()

        amp_rate = 0.9

        if cell_type == '?????????':
            amp_rate = 0.8

        sql = f"SELECT result FROM {DatabaseTable.STAT} WHERE task_id='{self.task_id}' AND gbt='{self.gbt}' AND category='{self.category}' AND item='??????????????????'"

        with create_conn() as conn:
            ret, mrdc = fetch_one(conn, sql)

        if ret is False:
            return ret, mrdc

        if mrdc is None or mrdc[0] is None:
            return False, "Can not find ??????????????????."

        mrdc = json.loads(mrdc[0])

        mrdc = {row[0]: row[-1] for row in mrdc["table"][1:]}

        csv_files = []

        with os.scandir(self.item_path) as item_it:
            for item_entry in item_it:
                if os.path.exists(os.path.join(item_entry.path, "????????????")):
                    with os.scandir(os.path.join(item_entry.path, "????????????")) as data_it:
                        for data_entry in data_it:
                            if data_entry.name.endswith(".csv"):
                                csv_files.append(data_entry.path)

        extract_results = Parallel(n_jobs=cpu_count())(delayed(extract_file)(csv_file,) for csv_file in csv_files)

        if len(extract_results) == 0 or any([result[0] is False for result in extract_results]):
            return True, {}

        extract_results = [result[1] for result in extract_results]

        extract_results.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        item_result = {
            "table": [["????????????", "???????????????????????????", "??????????????????", "??????????????????", "??????????????????/??????????????????", "????????????"]],
            "graph": [{"???????????????????????????": ["????????????(Ah)", "??????(V)"]}, {"keys": []}, {"values": []}]
        }

        for result in extract_results:
            item_result["table"].append([result[0], cell_type, round(mrdc[result[0]], 2), round(result[1][-1][0], 2), f"{result[1][-1][0] / mrdc[result[0]]:.2%}", "??????" if result[1][-1][0] / mrdc[result[0]] >= amp_rate else "?????????"])
            item_result["graph"][1]["keys"].append(result[0])
            item_result["graph"][2]["values"].append(result[1])

        return True, item_result