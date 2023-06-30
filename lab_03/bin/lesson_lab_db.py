import os
import glob
import configparser
import sys, getopt

import time

import re

import pandas as pd
import logging
import logging.handlers

import mariadb

import __main__

#----------------------------------------- Variable ------------------------------------------
EXEC_PARA  ='N/A'
EXEC_PARA='-test -v'
conn = None
iRet = 0
INPUT_PATH = "input/"
OUTPUT_PATH = "output/"


INIT_SQL_BLUK_LOAD = """
    LOAD DATA LOCAL INFILE '{FULL_FILENAME}'
    INTO TABLE {STG_DATABASE}.{STG_TABLE}
    FIELDS TERMINATED BY '{DELIMETER}'  ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS
    ({LIST_STG_COLUMNS},STG_SOURCE)
    SET STG_SOURCE = '{FILENAME}';
"""

INIT_SQL_TRUNCATE_TABLE = """
    TRUNCATE TABLE {STG_DATABASE}.{STG_TABLE};
"""

INIT_SQL_COUNT_RECORD = """
    SELECT COUNT(*) AS REC_NO FROM {STG_DATABASE}.{STG_TABLE} WHERE STG_SOURCE = '{FILENAME}';
"""

#----------------------------------------- Logging ------------------------------------------
logFormatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

log = logging.getLogger()
# log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


#----------------------------------------- Function ------------------------------------------
def _usage(argv) :
    """
    >>> _usage(sys.argv) # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    SystemExit: 2
    """

    print ('Invalid parameters:',argv[0],EXEC_PARA)
    sys.exit(2)

def _db_connect(db_type) :
    
    """
    Args:
        :param db_type: String Type of Database
               stg_db: database
        :returns: database connection
            
    >>> _db_connect('Mariadb') # doctest: +ELLIPSIS
    (<mariadb.connection object at ...>, <mariadb.connection.cursor object at ...>)
    >>> _db_connect('NONO') # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    SystemExit: 2
    """

    try:
        if db_type == "Mariadb":
            # Connect to MariaDB Platform
            conn = mariadb.connect(
                user="root",
                password="example",
                host="127.0.0.1",
                port=3306,
                autocommit=True,
                local_infile = 1                
                # database=stg_db
            )            
        else:
            log.error("No DB Supports")
            sys.exit(2)

        # opts, args = getopt.getopt(sys.argv[1:],"hvf:t:d:",["conf=","type=","test"])
    except mariadb.Error as e:
        log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)        
    except:
        # log.error('usage: {} -f <config file>'.format(sys.argv[0]))
        # _usage(sys.argv)
        sys.exit(2)

    # return Cursor
    return (conn,conn.cursor())

def delete_tailer(full_filename):
    
    """
    Args:
        :param full_filename: String include path + filename
               
        :returns: number record on tailer record format : END;XXXXX
            
    >>> delete_tailer('input/DESCRIBE_LOG_EVENTS_20230625_154002.txt')
    150000
    >>> os.remove('input/DESCRIBE_LOG_EVENTS_20230625_154002.txt' + ".process")
    """

    record_no = 0

    try:    
        # open write file
        with open(full_filename+".process",'w') as fw :

            with open(full_filename) as fs :
                contents = fs.readlines()

                for line in contents:
                    
                    if line.startswith("END;"):
                        
                        #get number of record from tailer
                        record_no = int(line.split(';')[1])

                        log.info("skip tailer record and {:0,.0f} record(s)".format(record_no))

                    else:
                        fw.write(line)
            
            fw.flush()
                    
    except Exception as e:
        print (e.message, e.args)
    
    return(record_no)

#------------------------------------------ Argment ------------------------------------------
try:
    opts, args = getopt.getopt(sys.argv[1:],"hvtf:",["conf=","test"])
except getopt.GetoptError:
    # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
    _usage(sys.argv)
    sys.exit(2)

# # set initiai value
# CONF = "N/A"
# FILE_TYPE = "N/A"
# DB_TYPE = "N/A"
MODE = "N/A"

# INPUT_PATH     = "N/A"
# INPUT_FILENAME = "N/A"

for opt, arg in opts:
    # log.info(opt)
    if opt == '-h':
        # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
        _usage(sys.argv)
        sys.exit(2)
    # elif opt in ("-f", "--conf"):
    #     CONF = arg
    # elif opt in ("-t", "--type"):
    #     FILE_TYPE = arg
    # elif opt in ("-d", "--database"):
    #     DB_TYPE = arg        
    elif opt in ("-t","--test"):
        MODE = "Unit Test"      
    elif opt in ("-v"):
        MODE = "Unit Test Full Verbose Mode "
        continue
    else:
        _usage(sys.argv)

# if len(sys.argv) <= 1 :
#     _usage(sys.argv)

#------------------------------------------- Main --------------------------------------------
log.info("__name__ : {}".format(__name__))

log.info("MODE : {}".format(MODE))

if __name__ == "__main__" and MODE.startswith("Unit Test"):
    log.info("Start Unit Test")
    import doctest

    # call python3 bin\lesson_lab_db.py --test -v (full Verbose Mode)

    doctest.testmod()
    # doctest.testmod(verbose = True)
    log.info("Finish Unit Test")

else:

    try:

        log.info("Process files on path : {}".format(INPUT_PATH))

        # regular (2[0-3]|[01]?[0-9]) = 20-23 and 00-19
        pattern_filename = r'^DESCRIBE_LOG_EVENTS_\d{4}(1[0-2]|[0]?[0-9])([0-3]?[0-9])_(2[0-3]|[01]?[0-9])([0-5]?[0-9])([0-5]?[0-9]).txt$'  # Regex pattern for YYYY-MM-DD format

        # Connect DB
        db_conn = None
        db_cur  = None


        DB_TYPE = "Mariadb"
        (db_conn,db_cur) = _db_connect(DB_TYPE)


        #delete table staging
        sql_truncate_table = INIT_SQL_TRUNCATE_TABLE
        sql_truncate_table = sql_truncate_table.replace("{STG_DATABASE}","stg")
        sql_truncate_table = sql_truncate_table.replace("{STG_TABLE}"   ,"LOG_EVENTS")                    

        log.info("{}".format(sql_truncate_table)) 
        db_cur.execute(sql_truncate_table)

        
        file_list = os.listdir(INPUT_PATH)
        for filename in file_list:

            # filter only object type's file
            if os.path.isfile(INPUT_PATH + filename):
                if re.search(pattern_filename, filename):
                    log.info("Found filename : {}".format(filename))

                    file_record_no = delete_tailer(INPUT_PATH + filename)

                    # Ingest file to DB
                    sql_bulk_load = INIT_SQL_BLUK_LOAD
                    sql_bulk_load = sql_bulk_load.replace("{STG_DATABASE}" ,"stg")
                    sql_bulk_load = sql_bulk_load.replace("{STG_TABLE}"    ,"LOG_EVENTS")                    
                    sql_bulk_load = sql_bulk_load.replace("{DELIMETER}"    , "|")
                    sql_bulk_load = sql_bulk_load.replace("{FULL_FILENAME}",INPUT_PATH + filename+".process")
                    sql_bulk_load = sql_bulk_load.replace("{FILENAME}"     ,filename)
                    sql_bulk_load = sql_bulk_load.replace("{LIST_STG_COLUMNS}","DATE_TIME,NAME,CITY,ZIPCODE,BBAN,LOCALE,BANK_COUNTRY,IBAN,COUNTRY_CALLING_CODE,MSISDN,PHONE_NUMBER,STATUS,GENDER")
                   
                    log.info("{}".format(sql_bulk_load)) 

                    db_cur.execute(sql_bulk_load)
                    record_db = db_cur.rowcount
                    log.info("filename : {} {:0,.0f} record(s) on RowCount".format(filename, record_db))

                    # Reconcile Number of Records
                    log.info("-------------------------------------------------------------------------------")
                    # file_record_no = 9
                    if record_db == file_record_no :
                        log.info("Verify (File & DB) : Success")
                    else:
                        log.error("Verify (File & DB) : Fail ({},{})".format(file_record_no,record_db))
                        raise RuntimeError("Reconcile record number between File & DB")
                    log.info("-------------------------------------------------------------------------------")
                        

                    # remove processed file
                    os.remove(INPUT_PATH + filename + ".process")
        
        # disconnect DB
        db_cur.close()
        db_conn.close()
                                    
    except FileNotFoundError:
        print("Folder not found.")
    except NotADirectoryError:
        print("The specified path is not a directory.")
    except  RuntimeError as e:
        print("Reconcile Error")
        print (e.message, e.args)
    except  Exception as e:
        print("Unknow Error")
        print (e.message, e.args)

sys.exit(iRet)
