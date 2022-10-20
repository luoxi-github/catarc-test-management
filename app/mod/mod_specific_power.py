import json
import re
import os
import pathlib

import pandas
from joblib import Parallel, delayed
from joblib.parallel import cpu_count

from app.item import DefaultItem
from config.setting import DatabaseTable
from common.log import get_logger


class ModSpecificPower(DefaultItem):
    def preprocess(self):
        """
        It extracts data from a bunch of csv files, and then calculates some values based on the extracted
        data
        :return: a tuple of two elements. The first element is a boolean value indicating whether the
        function is successful or not. The second element is a dictionary containing the result of the
        function.
        """

        def extract_file(csv_file):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: the file to extract the data from
            """

            try:
                df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Total Time, S', 'Step time, S', 'Current, A', 'Power, W'])
                df = df.dropna(how='any')
                discharge_index = df[df["Power, W"] < 0]['Total Time, S'].astype('float64').idxmax()
                charge_index = df[df["Power, W"] > 0]['Total Time, S'].astype('float64').idxmax()

                discharge_list = [csv_file.split('/')[-3], "放电", -1 * df.loc[discharge_index, 'Current, A'], df.loc[discharge_index, 'Step time, S'], -1 * df.loc[discharge_index, 'Power, W']]
                charge_list = [csv_file.split('/')[-3], "充电", df.loc[charge_index, 'Current, A'], df.loc[charge_index, 'Step time, S'], df.loc[charge_index, 'Power, W']]

                return True, [discharge_list, charge_list]
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"

        mass_path = pathlib.Path(self.item_path).parent.joinpath("尺寸质量", "设备数据")
        mass_dict = {}

        if mass_path.exists():
            for f in mass_path.glob("*.xlsx"):
                try:
                    df_excel = pandas.read_excel(f.as_posix())

                    mass_dict = dict(df_excel[['样品编号', '模组质量/kg']].to_numpy().tolist())
                    break
                except:
                    pass

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

        if not mass_dict:
            return False, "Can not find 模组质量/kg."

        table = []

        for rows in extract_results:
            for row in rows:
                if row[0] in mass_dict:
                    table.append(row + [mass_dict[row[0]], round(row[-1] / mass_dict[row[0]], 2)])
                else:
                    table.append(row + ["", ""])

        table.sort(key=lambda x: int(re.sub("\D", "", x[0])))

        item_result = {
            "table": [['样本编号', '充放电类型', '电流(A)', '持续时间(s)', '功率(W)', '质量(kg)', '比功率(W/kg)']] + table,
        }

        return True, item_result