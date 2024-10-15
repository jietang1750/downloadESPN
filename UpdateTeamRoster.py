import sqlConn
import json
from datetime import datetime,timezone,date, timedelta
import os
import csv
import ESPNSoccer
import extractESPNData01
import sql_insert_all

with open('config_db.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)
rootDir=Response['rootDir']
rootDir2=Response['rootDir2']
importLeagueFilter=Response['leagues']
Progress=Response['Progress']
bSaveInter = Response['bSaveIntermediateResults']

dir1 = rootDir
dir2 = rootDir2
eventDir = dir1 + 'events/'
saveDirRoster = dir2 + "tables/roster/"
eventImportDir = dir1

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

dataSet = {"startDate": "20240916",
           "endDate": "20240917",
           "extractionDate": "20240917",
           "status": "started"}
#
# get a list of existing eventIds in excel4soccer.Fixtures
#

userId = mysqlDict['userId']
pwd = mysqlDict['pwd']
hostName = mysqlDict['hostName']
dbName = mysqlDict['dbName']
odbcDriver = mysqlDict['odbcDriver']
osStr = mysqlDict['osStr']

nStart = 1
nEnd = 16

if osStr == "Windows":
    (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
elif osStr == "Linux":
    (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
else:
    (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
print(conn)

cursor.execute(f"SELECT eventId FROM Fixtures;")
rs=cursor.fetchall()
conn.close()

eventList = []
for item in rs:
    eventList.append(item[0])
nEvents=len(eventList)
print("number of events in dB:",nEvents)

errLog = []
currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

if os.path.isdir(eventDir):
    filelist = [f for f in os.listdir(eventDir)]
else:
    print("directory not found")
    filelist = []

newfilelist=filelist
i=0
nTotal = len(newfilelist)
errEvents=[]
eventIdNotInDB=[]
importedLeagues = []
for f in newfilelist:
    i += 1
    eventId = int(f.split('.')[0])
    if eventId in eventList:
       importedLeagues.append({"eventId":eventId})
    else:
        print("event not in DB:",eventId)
        eventIdNotInDB.append(eventId)
    if int(i/1000)*1000 == i or i == nTotal:
        print("append importEventIds", i, "of",nTotal)

print()
print("errEvents:")
print(errEvents)
print("eventId not in DB:")
print(eventIdNotInDB)
print()
print("no of events:")
print(len(importedLeagues))

#newLeagues = importedLeagues

newLeagues = []
i = 0
for tmpLeague in importedLeagues:
    i += 1
    # newLeagues.append(tmpLeague)
    if i >100000 and i <= 180000:
        newLeagues.append(tmpLeague)

nTotImports = len(newLeagues)
i = 0
tmpLeagues = []
for tmpLeague in newLeagues:
    i += 1
    tmpLeagues.append(tmpLeague)
    if int(i/1000)*1000 == i or i == nTotImports:
        print("process", i, "of",nTotImports)
        msg,errLog = extractESPNData01.extractRoster(eventDir,saveDirRoster,eventImportDir,tmpLeagues,errLog,currentTime)
        print(msg)
        msg = sql_insert_all.Install_All(mysqlDict, dataSet, rootDir, rootDir2, nStart, nEnd)
        print(msg)
        tmpLeagues = []
