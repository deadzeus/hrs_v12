# -*- coding: utf-8 -*-
import argparse
import importlib
import time
import traceback
from configparser import ConfigParser
from com.ezhiyang.www.hrs.worker.hrs_worker import HrsWorkerThread

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', default="需要指定配置文件")
args = parser.parse_args()
CONFIG_FILE = args.config

if CONFIG_FILE == '需要指定配置文件':
    print(CONFIG_FILE)
else:
    ASSIGNED_TASK_SET = set()

    while True:
        hrs_config = ConfigParser()
        hrs_config.read(CONFIG_FILE, encoding="UTF-8")

        task_list = hrs_config.options("task_config")
        for task_tag in task_list:
            try:
                if task_tag not in ASSIGNED_TASK_SET:
                    task_config = ConfigParser()
                    task_config.read(hrs_config['task_config'][task_tag], encoding="UTF-8")

                    task_module = importlib.import_module(hrs_config['task_module'][task_tag])
                    task_class = getattr(task_module, hrs_config['task_class'][task_tag])
                    task_instance = task_class(hrs_config, task_config)

                    task_worker = HrsWorkerThread(task_instance)
                    task_worker.start()

                    ASSIGNED_TASK_SET.add(task_tag)
            except Exception as e:
                traceback.print_exc()

        time.sleep(60 * 10)

