import os
import json
import pathlib
import zipfile
from io import BytesIO

from flask import Flask, request, jsonify, make_response, send_from_directory, send_file
from flask_cors import CORS

from common.log import logger_config, get_logger
from config.setting import LOG_PATH, LOG_FILE, LOG_LEVEL, FILE_PATH, ITEM_LIST
from app.api import *


logger_config(os.path.join(LOG_PATH, LOG_FILE), LOG_LEVEL)

logger = get_logger()

logger.info("Service start.")


app = Flask(__name__)

app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_MIMETYPE'] = "application/json;charset=utf-8"
app.config['MAX_CONTENT_LENGTH'] = 10* 1024 * 1024 * 1024

CORS(app)


@app.route("/api/<func>", methods=['POST'])
def call_api(func):
    """
    It's a wrapper function that calls the API function specified by the parameter, and returns the
    result of the API function
    
    :param func: The function name to be called
    :return: a json object.
    """

    try:
        logger.info(f"call {func}")

        if func == "login":
            params = json.loads(request.data.decode('utf-8'))

            ret, data = verify_user(params["user"], params["password"])
        elif func == "upload_product_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            product_file = request.files['file']
            task_id = request.form["task_id"]
            file_path = os.path.join(FILE_PATH, task_id)

            pathlib.Path(file_path).mkdir(exist_ok=True)

            product_file.save(os.path.join(file_path, "product_file" + os.path.splitext(product_file.filename)[-1]), buffer_size=-1)

            ret, data = extract_product_file(task_id, product_file.filename)
        elif func == "upload_test_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            test_file = request.files['file']
            task_id = request.form["task_id"]

            file_path = os.path.join(FILE_PATH, task_id)

            pathlib.Path(file_path).mkdir(exist_ok=True)

            test_file.save(os.path.join(file_path, test_file.filename), buffer_size=-1)

            ret, data = upload_test_file(data, task_id, test_file.filename)
        elif func == "extract_test_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            ret, data = eval(func)(data, **params)
        elif func == "extract_item_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            item_file = request.files['file']
            task_id = request.form["task_id"]
            gbt = request.form["gbt"]
            category = request.form["category"]
            item = request.form["item"]
            item_note = request.form.get("item_note", None)

            item_file.save(os.path.join(FILE_PATH, task_id, item_file.filename), buffer_size=-1)

            ret, data = extract_item_file(data, task_id, gbt, category, item, item_file.filename, item_note)
        elif func == "display_img":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            response = make_response(open(params["img_path"], "rb").read())
            response.headers["Content-Type"] = "image/" + pathlib.Path(params["img_path"]).suffix[1:].lower()

            return response
        elif func == "download_img":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            return send_from_directory(os.path.dirname(params["img_path"]), os.path.basename(params["img_path"]), filename=os.path.basename(params["img_path"]), as_attachment=True)
        elif func == "download_product_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            ret, data = query_task_product_file(params["task_id"])
            if ret is True:
                return send_from_directory(os.path.join(FILE_PATH, params["task_id"]), data, filename=data, as_attachment=True)
        elif func == "download_test_file":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            os.chdir(os.path.join(FILE_PATH, params["task_id"]))

            memory_file = BytesIO()

            with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as myzip:
                with os.scandir("./") as gbt_it:
                    for gbt_entry in gbt_it:
                        if gbt_entry.is_dir() and gbt_entry.name in ITEM_LIST:
                            for dev in pathlib.Path(f"./{gbt_entry.name}").glob('**/*'):
                                if dev.is_file():
                                    myzip.writestr(dev.as_posix(), dev.read_bytes())

            memory_file.seek(0)

            return send_file(memory_file, attachment_filename=f'{params["task_id"]}.zip', as_attachment=True)
        elif func == "download_test_item_data":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            os.chdir(os.path.join(FILE_PATH, params["task_id"], params["gbt"], params["category"], params["item"]))

            memory_file = BytesIO()

            with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as myzip:
                for dev in pathlib.Path("./").glob('**/*'):
                    if dev.is_file():
                        myzip.writestr(dev.as_posix(), dev.read_bytes())

            memory_file.seek(0)

            return send_file(memory_file, attachment_filename=f'{params["item"]}.zip', as_attachment=True)
        elif func == "download_test_sample_data":
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            os.chdir(os.path.join(FILE_PATH, params["task_id"], params["gbt"], params["category"], params["item"], params["sample"], "设备数据"))

            memory_file = BytesIO()

            with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as myzip:
                for dev in pathlib.Path("./").glob('**/*'):
                    if dev.is_file():
                        myzip.writestr(dev.as_posix(), dev.read_bytes())

            memory_file.seek(0)

            return send_file(memory_file, attachment_filename=f'{params["sample"]}-设备数据.zip', as_attachment=True)
        else:
            ret, data = verify_token(request.headers.get('Authorization', ""))
            if ret is False:
                return jsonify({"ret": ret, "data": {"code": 0, "message": data}}), 401

            params = json.loads(request.data.decode('utf-8'))

            ret, data = eval(func)(**params)
    except Exception as e:
        logger.error(f"Caught exception: {e.__doc__}({e})")

        ret = False
        data = f"Caught exception: {e.__doc__}({e})"

    if ret is False:
        if not isinstance(data, dict):
            data = {"code": 0, "message": data}

    return jsonify({"ret": ret, "data": data})