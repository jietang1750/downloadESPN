import extractESPNData01
import sqlConn
import json
from datetime import datetime,timezone,date, timedelta
import os
import csv
import ESPNSoccer
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
fileDir1 = dir1 + "events/"
fileDir2 = dir1
fileDir3 = rootDir2 + "tables/"

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

snapshotfile = fileDir3 + "eventSnapshots.json"

#
# bScan = True scan rootDir/events/ directory and create snapshots files
# bScan = False import EventSnapshots.json into Database
#
#bScan = False
bScan = True 

nStart = 18
nEnd = 18

if bScan:
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

    if os.path.isdir(fileDir1):
        filelist = [f for f in os.listdir(fileDir1)]
    else:
        print("directory not found")
        filelist = []

    newfilelist=filelist
    i=0
    snapshotsList = []
    nTotal = len(newfilelist)
    errEvents=[]
    eventIdNotInDB=[]
    for f in newfilelist:
        i += 1
        eventId = f.split('.')[0]
        if int(eventId) in eventList:
            (event, code) = ESPNSoccer.open_event(eventId, fileDir2)
            if code ==0:
                #print(eventId)
                #snapshot = ESPNSoccer.eventSnapshot(event, eventId)
                try:
                    snapshot = ESPNSoccer.eventSnapshot(event,eventId)
                except:
                    print(eventId)
                    errEvents.append(eventId)
                    continue
                snapshotsList.append(snapshot)
            else:
                errEvents.append(eventId)
        else:
            print("event not in DB:",eventId)
            eventIdNotInDB.append(eventId)
        if int(i/1000)*1000 == i:
            print("append snapshot", i, "of",nTotal)

    print("append snapshot", i, "of", nTotal)

    print()
    with open(snapshotfile,"w") as file:
        json.dump(snapshotsList, file)
    file.close()
    print(snapshotfile)
    print("errEvents:")
    print(errEvents)
    print("eventId not in DB:")
    print(eventIdNotInDB)

msg = sql_insert_all.Install_All(mysqlDict, dataSet, rootDir, rootDir2, nStart, nEnd)
print(msg)
