import os
import glob
import configparser
import sys, getopt

import time

import re

import pandas as pd
import logging
import logging.handlers

import __main__

#----------------------------------------- Variable ------------------------------------------
EXEC_PARA  ='N/A'
EXEC_PARA='-f <Config file>'
conn = None
iRet = 0
INPUT_PATH = "input/"
OUTPUT_PATH = "output/"

#----------------------------------------- Logging ------------------------------------------
logFormatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

log = logging.getLogger()
# log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)


#----------------------------------------- Function ------------------------------------------



#------------------------------------------ Argment ------------------------------------------
# try:
#     opts, args = getopt.getopt(sys.argv[1:],"hvf:t:d:",["conf=","type=","test"])
# except getopt.GetoptError:
#     # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
#     _usage(sys.argv)
#     sys.exit(2)

# # set initiai value
# CONF = "N/A"
# FILE_TYPE = "N/A"
# DB_TYPE = "N/A"
MODE = "N/A"

# INPUT_PATH     = "N/A"
# INPUT_FILENAME = "N/A"

# for opt, arg in opts:
#     # log.info(opt)
#     if opt == '-h':
#         # log.info('usage: {} -f <config file>'.format(sys.argv[0]))
#         _usage(sys.argv)
#         sys.exit(2)
#     elif opt in ("-f", "--conf"):
#         CONF = arg
#     elif opt in ("-t", "--type"):
#         FILE_TYPE = arg
#     elif opt in ("-d", "--database"):
#         DB_TYPE = arg        
#     elif opt in ("-v","--test"):
#         MODE = "Unit Test"        
#     else:
#         _usage(sys.argv)

# if len(sys.argv) <= 1 :
#     _usage(sys.argv)




#------------------------------------------- Main --------------------------------------------
# log.info("__name__ : {}".format(__name__))

if __name__ == "__main__" and MODE == "Unit Test":
    log.info("Start Unit Test")
    import doctest

log.info("Process files on path : {}".format(INPUT_PATH))

try:

    # pattern = r'DESCRIBE_LOG_EVENTS_\d{8}_\d{6}.txt'  # Regex pattern for YYYY-MM-DD format

    # regular (2[0-3]|[01]?[0-9]) = 20-23 and 00-19
    pattern_filename = r'DESCRIBE_LOG_EVENTS_\d{4}(1[0-2]|[0]?[0-9])([0-3]?[0-9])_(2[0-3]|[01]?[0-9])([0-5]?[0-9])([0-5]?[0-9]).txt'  # Regex pattern for YYYY-MM-DD format

    pattern_special_phonenumber = r'^\(\d{3}\)\d{3}-\d{4}$'

    # list file from folder INPUT_PATH
    
    statistic = {}
    statistic["active"] = 0
    statistic["m"] = 0
    statistic["f"] = 0
    statistic["min_zipcode"] = 999999999999
    statistic["max_zipcode"] = 0

    # open write file
    with open(OUTPUT_PATH + "special_phonenumber.txt.tmp",'w') as fw :

        file_list = os.listdir(INPUT_PATH)
        for filename in file_list:

            # filter only object type's file
            if os.path.isfile(INPUT_PATH + filename):
                if re.search(pattern_filename, filename):
                    log.info("Found filename : {}".format(filename))
                    
                    # open read file
                    with open(INPUT_PATH + filename) as fs :
                        contents = fs.readlines()
                
                        log.info("file : {} {:0,} record(s)".format(filename,len(contents)))
                        statistic[filename] = len(contents)
                        for line in contents:

                            line = line[:-1]
                            # log.info("{}".format(line))

                            # filter status = active
                            arr_line = line.split('|')

                            # skip if header & tailer
                            # header : DATE_TIME|NAME|CITY|ZIPCODE|BBAN|LOCALE|BANK_COUNTRY|IBAN|COUNTRY_CALLING_CODE|MSISDN|PHONE_NUMBER|STATUS|GENDER
                            # tailer : END;150000
                            if arr_line[0] == 'DATE_TIME' or "END;" in arr_line[0] :
                                continue

                            # find speical phone number
                            # format (XXX)XXX-XXXX
                            phonenumber = arr_line[10]
                            if re.search(pattern_special_phonenumber, phonenumber) :
                                # log.info("{}".format(phonenumber))
                                fw.writelines(phonenumber + "\n")
                            
                            # filter status = 'active'
                            status = arr_line[11]
                            if status == 'active' :

                                statistic["active"]+= 1
                                # log.info("{}".format(line))
                                # time.sleep(1)
                            else:
                                continue
                            
                            # cout m or f
                            gender=arr_line[12]
                            statistic[gender]+= 1

                            # find min or max zip code
                            zipcode=int(arr_line[3])
                            if zipcode < statistic["min_zipcode"] :
                                statistic["min_zipcode"] = zipcode
                            elif zipcode > statistic["max_zipcode"] :
                                statistic["max_zipcode"] = zipcode




    if os.path.exists(OUTPUT_PATH + "special_phonenumber.txt"):
        log.info("remove file : {} because will replace on next action".format(OUTPUT_PATH + "special_phonenumber.txt"))
        os.remove(OUTPUT_PATH + "special_phonenumber.txt")

    log.info("rename temp to file : {}".format(OUTPUT_PATH + "special_phonenumber.txt"))
    os.rename(OUTPUT_PATH + "special_phonenumber.txt.tmp",
              OUTPUT_PATH + "special_phonenumber.txt")
    
    log.info("------------------------------------------------------------------------")
    log.info("|                             Statistic                                |")
    log.info("------------------------------------------------------------------------")
    log.info("- active      : {:0,}".format(statistic["active"]))
    log.info("- gender m    : {:0,}".format(statistic["m"]))
    log.info("- gender f    : {:0,}".format(statistic["f"]))
    log.info("- zipcode min : {}".format(statistic["min_zipcode"]))
    log.info("- zipcode max : {}".format(statistic["max_zipcode"]))                
        
except FileNotFoundError:
    print("Folder not found.")
except NotADirectoryError:
    print("The specified path is not a directory.")

sys.exit(iRet) 
