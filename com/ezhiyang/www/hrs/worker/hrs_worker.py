# -*- coding: utf-8 -*-
import pymongo
import threading
import time
import traceback


class HrsWorkerThread(threading.Thread):

    def __init__(self, task_instance):
        threading.Thread.__init__(self)
        self.task_instance = task_instance

    def run(self):
        try:
            mongo_connection = pymongo.MongoClient(self.task_instance.hrs_config['mongo']['server'])
            mongo_db = mongo_connection[self.task_instance.hrs_config['mongo']['db']]
            mongo_collection = mongo_db[self.task_instance.hrs_config['mongo']['collection']]

            while True:
                job_url_json_list = self.task_instance.grab_url()
                for job_url_json in job_url_json_list:
                    try:
                        res_json = self.task_instance.grab_page(job_url_json)
                        if res_json is None:
                            continue
                        mongo_collection.insert_one(res_json)
                    except Exception as e:
                        traceback.print_exc()
                time.sleep(15)
                # time.sleep(3600 * 24)
        except Exception as e:
            traceback.print_exc()
        finally:
            mongo_connection.close()

