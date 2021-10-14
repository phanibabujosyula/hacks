#!/usr/bin/env python
# coding: utf-8

# Libraries
import os, sys
python_home_path = os.path.abspath(r'D:\PythonHome')
sys.path.insert(1, python_home_path)
import cdtk.cdkt_common_functions as capsis_functions
import arcgis
from arcgis.gis import GIS
import time
from datetime import datetime
import csv
import logging

def get_env_details(config_file_name):
    try:
        axcsLocn = config_file_name
    except FileNotFoundError as em:
        print(em)
    except Exception as em:
        print(em)

    global agol_url
    global agol_un
    global agol_pwd
    global etl_csv_path
    global local_csv_path
    global archive_csv_path
    global error_csv_path
    global log_level
    global log_format
    global log_file_name
    global proxy_host
    global proxy_port
    
    env_info = capsis_functions.get_env_info(axcsLocn)
    agol_url = env_info.get('agolurl')
    agol_un = env_info.get('agolun')
    agol_pwd = env_info.get('agolpwd')
    etl_csv_path = env_info.get('etlcsvpath')
    local_csv_path = env_info.get('localcsvpath')
    archive_csv_path = env_info.get('archivecsvpath')
    error_csv_path = env_info.get('errorcsvpath')
    log_level = int(env_info.get('loglevel'))
    log_format = env_info.get('logformat')
    log_file_name = os.path.join(env_info.get('logpath'), env_info.get('logfilename') + '_' + datetime.now().strftime('%Y%m%d%H%M%S')+'.log')
    proxy_host = env_info.get('proxyhost')
    proxy_port = env_info.get('proxyport')

def upload_csv_files_to_agol(local_path, agol_url, agol_user, agol_pwd, proxy_host, proxy_port):
    try:
        try:
            logging.info('Connecting to AGOL %s' % (agol_url))
            gis = GIS(agol_url, agol_user, agol_pwd)
        except Exception as e:
            logging.error('Failed while connecting to AGOL environment %s. Reason: %s' % (gis, e))
        try:
            for file in os.listdir(local_path):
                update_csv = os.path.join(local_path, file)
                logging.info('Starting to add the csv file %s to AGOL bucket %s' % (update_csv, agol_url))
                try:
                    csv_item = gis.content.add({}, update_csv)
                    logging.info('Completed to add the csv file %s to AGOL bucket %s' % (update_csv, agol_url))
                    archive_csv = os.path.join(archive_csv_path, file.split(".")[0] + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.' + file.split(".")[1])
                    capsis_functions.move_file(update_csv, archive_csv)
                    logging.info('Moved the csv file %s to %s' % (update_csv, archive_csv))
                except Exception as e:
                    error_csv = os.path.join(error_csv_path, file.split(".")[0] + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.' + file.split(".")[1])
                    capsis_functions.move_file(update_csv, error_csv)
                    logging.info('Moved the csv file %s to %s' % (update_csv, error_csv))
                    logging.error('Failed to upload data file into AGOL Bucket %s. Reason: %s' % (update_csv, e))
        except Exception as e:
            logging.error('Failed to upload data file into AGOL Bucket %s. Reason: %s' % (update_csv, e))
    except Exception as e:
        logging.error('Failed to connect AGOL %s. Reason: %s' % (gis, e))

try:
    get_env_details(r"D:\PythonHome\agol\config\agol.config")
except FileNotFoundError as em:
    print(em)
except Exception as em:
    print(em)

print('log_file_name', log_file_name)
    
try:
    logger = capsis_functions.set_log_info(log_file_name, log_level, log_format)
except Exception as em:
    print(em)
    
capsis_functions.check_folder(folder_name=local_csv_path, delete_contents_flag='N')
logging.info("check_folder processing completed at: %s", (datetime.now().strftime('%Y%m%d %H%M%S')))

capsis_functions.move_all_files_in_dir(etl_csv_path,local_csv_path)
logging.info("move_all_files_in_dir processing completed at: %s", (datetime.now().strftime('%Y%m%d %H%M%S')))

capsis_functions.process_wait_seconds(30)
 
upload_csv_files_to_agol(local_csv_path, agol_url, agol_un, agol_pwd, proxy_host, proxy_port)
logging.info("upload_csv_files_to_agol processing completed at: %s", (datetime.now().strftime('%Y%m%d %H%M%S')))

logging.info("Script processing completed at: %s", (datetime.now().strftime('%Y%m%d %H%M%S')))
print("Script processing completed at: " + datetime.now().strftime('%Y%m%d %H%M%S'))
