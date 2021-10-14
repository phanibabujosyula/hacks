# -*- coding: utf-8 -*-
"""
CAPSIS common functions
should be used in all of the CAPSIS projects
"""

from datetime import datetime, timedelta
from dateutil import parser, tz
from functools import reduce
import json
import csv
import time
import cx_Oracle
import numpy as np
import os, shutil
import pandas as pd
from pytz import timezone
import logging
import sys, functools
from pathlib import Path


def get_env_info(axcsLocn):
    """Reads the control file and returns the information required for processing"""
    with open(axcsLocn, 'r') as local_item_file:
        reader = csv.DictReader(local_item_file)
        return { rows['key'] : rows['value'] for rows in reader}


def set_log_info(log_file_name, log_level, log_format):
    """Sets the loggin file, level"""
    logging.basicConfig(filename=log_file_name, level=log_level, format=log_format)

def process_wait_seconds(num_of_seconds):
    time.sleep(num_of_seconds)
    
def get_method_name():
    """Returns the method name currently being processed"""
    try:
        logging.info('%s' % ('start'))
        return sys._getframe(1).f_code.co_name
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))

def get_out_file_name(component_code, DB, start_date, end_date, out_file_path, out_file_name):
    try:
        logging.info('%s^%s^%s^%s' % (component_code, start_date, end_date, 'start'))
        dttm = datetime.now().strftime('%Y%m%d%H%M%S')
        file_name = 'RP' + start_date[0:8].replace('-', '_') + component_code + '_' + DB + '_' + out_file_name + '_' + start_date + '_' + end_date + '_' + dttm + '.csv'
        return os.path.join(out_file_path, file_name)
        logging.info('%s^%s^%s^%s' % (component_code, start_date, end_date, 'complete'))
    except Exception as em:
        logging.error('%s^%s^%s^%s' % (component_code, start_date, end_date, str(em).replace('\n', ' ').replace('\r', '')))

def get_logger(log_file_name, log_level, log_format):
    """ Creates a Log File and returns Logger object """
    logger = logging.Logger(log_file_name)
    handler = logging.FileHandler(log_file_name,'a+')
    handler.setFormatter(CustomFormatter('%(asctime)s|%(levelname)s|%(filename)s|%(funcName)s|%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

class CustomFormatter(logging.Formatter):
    """ Custom Formatter does these 2 things:
    1. Overrides 'funcName' with the value of 'func_name_override', if it exists.
    2. Overrides 'filename' with the value of 'file_name_override', if it exists.
    """

    def format(self, record):
        if hasattr(record, 'func_name_override'):
            record.funcName = record.func_name_override
        if hasattr(record, 'file_name_override'):
            record.filename = record.file_name_override
        return super(CustomFormatter, self).format(record)

def log_decorator(_func=None):
    def log_decorator_info(func):
        @functools.wraps(func)
        def log_decorator_wrapper(self, *args, **kwargs):
            """Build logger object"""
            logger_obj = log.get_logger(log_file_name=self.log_file_name, log_file_path=self.log_file_path)

            """log function begining"""
            logger_obj.info("Begin function")
            try:
                """ log return value from the function """
                value = func(self, *args, **kwargs)
                logger_obj.info(f"Returned: - End function {value!r}")
            except Exception as em:
                """log exception if occurs in function"""
                logger_obj.error(f"Exception:{0}", (em))
                raise
            return value
        return log_decorator_wrapper
    if _func is None:
        return log_decorator_info
    else:
        return log_decorator_info(_func)

def get_df_from_oracle(db_host, db_port, db_service_name, db_user, db_pwd, db_query):
    try:
        logging.info('%s' % ('start'))
        dsn_tns = cx_Oracle.makedsn(db_host, db_port, service_name=db_service_name)
        conn = cx_Oracle.connect(db_user, db_pwd, dsn_tns)
        df = pd.read_sql(db_query, con=conn)
        logging.info('%s' % ('complete'))
        return df
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))
    finally:
        conn.close()

def get_df_from_csv(csv_file_name):
    try:
        logging.info('%s' % ('start'))
        df = pd.read_csv(csv_file_name)
        logging.info('%s' % ('complete'))
        return df
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))

def copy_all_files_in_dir(from_path, to_path):
    """copies the content of one folder to another"""
    try:
        logging.info('%s' % ('start'))
        for filename in os.listdir(from_path):
            full_file_name = os.path.join(from_path, filename)
            shutil.copy(full_file_name, to_path)
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))

def copy_file(old_file_name, new_file_name):
    """copies the one file at a time another dir"""
    try:
        logging.info('%s' % ('start'))
        shutil.copy(old_file_name, new_file_name)
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))
        
def move_file(old_file_name, new_file_name):
    """copies the one file at a time another dir"""
    try:
        logging.info('%s' % ('start'))
        shutil.move(old_file_name, new_file_name)
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))

def move_all_files_in_dir(from_path, to_path):
    """moves the content of one folder to another"""
    try:
        logging.info('%s' % ('start'))
        for filename in os.listdir(from_path):
            full_file_name = os.path.join(from_path, filename)
            dst_file_name = os.path.join(to_path, filename)
            if os.path.exists(dst_file_name):
                os.remove(dst_file_name)
            shutil.move(full_file_name, to_path)
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))

def touch_file(touch_path, touch_file_name, times=None):
    """touches a file with no content"""
    touch_file_name = os.path.join(touch_path, touch_file_name)
    fhandle = open(touch_file_name, 'a')
    try:
        logging.info('%s' % ('start'))
        os.utime(touch_file_name, times)
        logging.info('%s' % ('complete'))
    except Exception as em:
        logging.error('%s' % (str(em).replace('\n', ' ').replace('\r', '')))
    finally:
        fhandle.close()

def check_folder(folder_name, delete_contents_flag):
    """checks the path exists and create the folder only if NOT exists"""
    if os.path.exists(folder_name):
        try:
            logging.info("Deleting folder contents: %s", folder_name)
            if delete_contents_flag == 'Y':
                delete_folder_contents(folder_name)
        except Exception as e:
            logging.critical('Failed to delete %s. Reason: %s' % (folder_name, e))
    else:
        try:
            logging.info("Creating folder : %s", folder_name)
            os.mkdir(folder_name)
        except Exception as e:
            logging.critical('Failed to creating %s. Reason: %s' % (folder_name, e))
            
def delete_folder_contents(folder_name):
    """delete folder contents if exists"""
    for filename in os.listdir(folder_name):
        file_path = os.path.join(folder_name, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.critical('Failed to delete %s. Reason: %s' % (file_path, e))

def copy_1files_to_local(src_path, dst_path):
    """copies the content of one folder to another"""
    for filename in os.listdir(remote_path):
        remote_file_path = os.path.join(remote_path, filename)
        logging.info('remote file path: %s' % (remote_file_path))
        logging.info('local file path: %s' % (local_path))
        try:
            logging.info('copying files from remote path %s to local path %s' % (local_path, remote_file_path))
            shutil.copy(remote_file_path, local_path)
        except Exception as c:
            logging.critical('Failed to copy %s. Reason: %s' % (remote_file_path, c))
        try:
            os.remove(remote_file_path)
        except Exception as d:
            logging.critical('Failed to delete %s. Reason: %s' % (remote_file_path, d))

        