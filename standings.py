import json
import mysql
from datetime import datetime,timezone,date, timedelta
from zoneinfo import ZoneInfo
import os
import csv
import sqlConn
import sql_insert_all

with open('config_db3.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)

# Read working directories from config_db.json
rootDir=Response['rootDir']
rootDir2=Response['rootDir2']
importLeagueFilter=Response['leagues']
Progress=Response['Progress']
bSaveInter = Response['bSaveIntermediateResults']

# mysql database connect information from config_db.json
mysqlDict={}

mysqlDict['userId'] = Response['username']
mysqlDict['pwd'] = Response["password"]
mysqlDict['hostName'] = Response["host"]
mysqlDict['odbcDriver'] = Response["odbcDriver"]
mysqlDict['dbName'] = Response["database"]
mysqlDict['queryDict'] = Response["queryDict"]
mysqlDict['queryDict_utf8'] = Response["queryDict_utf8"]
mysqlDict['osStr'] = Response['os']

#seasonType = 12654 #ENG.1
#seasonType = 12655 #ESP.1
#seasonType = 12826 #KSA.1
#seasons =[12654,12655,12826]
#seasons =[12655]
#seasons =[12654]
#lineupEventList = [704279, 704280, 704288]

dataSet = {"startDate": "2024-07-01",
           "endDate": "2025-06-30",
           "extractionDate": "2024-10-27",
           "status": "test_Standings"}
nStart=1
nEnd =18
msg = sql_insert_all.Install_All(mysqlDict,dataSet,rootDir,rootDir2,nStart,nEnd)
