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


class ModRtempDchgCapacity(DefaultItem):
    def preprocess(self):
        """
        It extracts data from a csv file and returns a list of lists
        :return: A list of lists.
        """

        def extract_file(csv_file):
            """
            The function extracts the data from the csv file and returns a list of lists
            
            :param csv_file: the file to extract the data from
            """

            try:
                df = pandas.read_csv(csv_file, skiprows=13, encoding='gbk', usecols=['Cycle', 'Step', 'Step time, S', 'Voltage, V', 'Power, W', 'Amp Hours Discharge, AH'])
                df = df.dropna(how='any')

                max_cycle = int(df[df['Power, W'] < 0]['Cycle'].max())
                dchg_step = int(df[(df['Power, W'] < 0) & (df.Cycle == max_cycle)]["Step"].max())

                dchg = []

                for i in range(2, -1, -1):
                    min_index = df[(df.Cycle == max_cycle - i) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmin()
                    max_index = df[(df.Cycle == max_cycle - i) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmax()

                    dchg.append(round(df.loc[min_index - 1, 'Amp Hours Discharge, AH'] - df.loc[max_index, 'Amp Hours Discharge, AH'], 2))

                init_index = df[(df.Cycle == max_cycle) & (df.Step == dchg_step)]['Step time, S'].astype('float64').idxmin()
                init_ah = df.loc[init_index - 1, 'Amp Hours Discharge, AH']

                df = df[(df.Cycle == max_cycle) & (df.Step == dchg_step)][['Amp Hours Discharge, AH', "Voltage, V"]]
                df['Amp Hours Discharge, AH'] = (init_ah - df['Amp Hours Discharge, AH']).round(2)

                return True, [csv_file.split('/')[-3]] + dchg + [round(statistics.mean(dchg), 2)] + [df.to_numpy().tolist()]
            except Exception as e:
                logger = get_logger()
                logger.error(f"Caught exception: {e.__doc__}({e})")

                return False, f"Caught exception: {e.__doc__}({e})"

        sql = f"SELECT value FROM {DatabaseTable.PRODUCT} WHERE task_id='{self.task_id}' AND category='{self.category}' AND name='???????????????Ah???'"

        with create_conn() as conn:
            ret, rated_capacity = fetch_one(conn, sql)

        if ret is False:
            return ret, rated_capacity

        if rated_capacity is None or rated_capacity[0] is None:
            return False, "Can not find ???????????????Ah???."

        rated_capacity = float(rated_capacity[0])

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
            "table": [['????????????', '???1?????????????????????(AH)', '???2?????????????????????(AH)', '???3?????????????????????(AH)', '??????????????????(AH)']] + [s[0:5] for s in extract_results],
            "list": [
                ['??????????????????????????????(AH)', round(extremum_capacity, 2)],
                ['???????????????????????????(AH)', round(mean_capacity, 2)],
                ['?????????????????????????????????', f"{extremum_capacity / mean_capacity:.2%}"],
                ['????????????????????????(AH)', round(rated_capacity, 2)],
                ['?????????????????????????????????/????????????????????????', f"{max_mean / rated_capacity:.2%}"],
                ['?????????????????????????????????/????????????????????????', f"{min_mean / rated_capacity:.2%}"]
            ],
            "graph": [{"???????????????????????????": ["????????????(Ah)", "??????(V)"]}, {"keys": [max_sample, min_sample]}, {"values": [extract_results[max_index][-1], extract_results[min_index][-1]]}]
        }

        return True, item_result