import os
import re
import pathlib
import copy
import json

from config.setting import FILE_PATH, DatabaseTable, TestDataError
from common.postgres_driver import create_conn, fetch_one


# It's a class that has a default value for any attribute that doesn't exist
class DefaultItem(object):
    def __init__(self, task_id, gbt, category, item):
        """
        The function takes in a task_id, gbt, category, and item and then creates a path to the item.
        
        :param task_id: The task id of the task that the user is currently working on
        :param gbt: the name of the folder
        :param category: the category of the item
        :param item: the name of the file
        """

        self.task_id = task_id
        self.gbt = gbt
        self.category = category
        self.item = item
        self.item_path = os.path.join(FILE_PATH, self.task_id, self.gbt, self.category, self.item)


    def preprocess(self):
        return True, {}


    def get_stat(self):
        """
        It takes a path to a directory, and returns a dictionary of the files in that directory and its
        subdirectories
        :return: A dictionary with the following keys:
        """

        data = {"files": []}
        files_dict = {}

        with os.scandir(self.item_path) as sample_it:
            for sample_entry in sample_it:
                if sample_entry.is_dir():
                    for p in pathlib.Path(sample_entry.path).glob('**/*'):
                        if p.is_file():
                            files_dict.setdefault(sample_entry.name, {})
                            dir_name = '-'.join(p.as_posix()[len(sample_entry.path)+1 : ].split('/')[:-1])
                            files_dict[sample_entry.name].setdefault(dir_name, [])

                            if p.suffix in {".png", ".PNG", ".jpg", ".JPG", ".jpeg", ".JPEG", ".bmp", ".BMP", ".mp4", ".MP4"}:
                                files_dict[sample_entry.name][dir_name].append([p.name, p.as_posix(), "yes"])
                            else:
                                files_dict[sample_entry.name][dir_name].append([p.name, p.as_posix(), "no"])

        index = list(files_dict)
        index.sort(key=lambda x: int(re.sub("\D", "", x)) if re.search("\d+", x) is not None else 0)

        tid = 1

        for i in index:
            data["files"].append({"id": tid, "0": i, "children": []})

            tid += 1

            for key, value in files_dict[i].items():
                for v in value:
                    if key == '':
                        data["files"][-1]["children"].append({"id": tid, "0": f"{i}", "1": v[0], "2": v[1], "3": v[2]})
                    else:
                        data["files"][-1]["children"].append({"id": tid, "0": f"{i}-{key}", "1": v[0], "2": v[1], "3": v[2]})

                    tid += 1

        sql = f"SELECT result FROM {DatabaseTable.STAT} WHERE task_id='{self.task_id}' AND gbt='{self.gbt}' AND category='{self.category}' AND item='{self.item}'"

        with create_conn() as conn:
            ret, result = fetch_one(conn, sql)

        if ret is True and result is not None and result[0] is not None:
            data.update(json.loads(result[0]))

            data.setdefault("table", [])
            data.setdefault("list", [])
            data.setdefault("graph", [])

        return True, data


    def get_decision(self):
        """
        It returns the decision of the current state.
        """

        with os.scandir(self.item_path) as sample_it:
            for sample_entry in sample_it:
                if sample_entry.is_dir():
                    for dev in pathlib.Path(sample_entry.path).glob('**/*'):
                        if dev.name == "初步判定结果":
                            with os.scandir(dev.as_posix()) as data_it:
                                for data_entry in data_it:
                                    if data_entry.name.endswith(".txt"):
                                        return True, pathlib.Path(data_entry.path).read_text(encoding='utf8')

        return True, ""