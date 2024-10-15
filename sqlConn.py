import json
import pandas as pd
import sys
#from sqlalchemy.engine import URL
#from sqlalchemy import text, inspect, Table, Column, Integer, String, MetaData, DateTime, TIMESTAMP
from datetime import datetime
from datetime import datetime,timezone
from dateutil import tz

def connectDB(hostName,userId,pwd,dbName):
    # Connect to tang-03-lx
    import mysql.connector
    try:
        conn = mysql.connector.connect(
            host=hostName,
            user=userId,
            password=pwd,
            database=dbName
        )
        cur = conn.cursor(buffered=True)
    except Exception as e:
        print(e)
        print('task is terminated')
        sys.exit()
    return(conn,cur)

def connectDB2(host,user,passwrod,database):
    # Connect to tang-svr
    import mysql.connector
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='jtang',
            password='cstagt9903',
            database='excel4soccer'
        )
        cur = conn.cursor(buffered=True)
    except Exception as e:
        print(e)
        print('task is terminated')
        sys.exit()
    return(conn,cur)

def connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver):
    # Connect to tang-03-lx
    import pyodbc
    try:
        conn = pyodbc.connect(driver=odbcDriver,
                              Server=hostName,
                              database=dbName,
                              uid=userId,
                              pwd=pwd,
                              trusted_connection='yes'
                              )
        cur = conn.cursor()
    except Exception as e:
        print(e)
        print('task is terminated')
        print(hostName,userId,pwd,dbName,odbcDriver)
        sys.exit()
    return (conn,cur)

def connectDB_ODBC2(hostname,userId,dbname,odbcDriver):
    # Connect to tang-svr
    import pyodbc
    try:
        conn = pyodbc.connect(driver='{MySQL ODBC 8.4 Unicode Driver}',
                              Server='tang-svr',
                              database='excel4soccer',
                              uid='jtang',
                              pwd='cstagt9903',
                              trusted_connection='yes'
                              )
        cur = conn.cursor()
    except Exception as e:
        print(e)
        print('task is terminated')
        sys.exit()
    return (conn,cur)

def est2utc(datetimeEST):
    # strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    # strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    from_zone = tz.gettz('America/New_York')
    to_zone = tz.gettz('UTC')
    est = datetimeEST.replace(tzinfo=from_zone)
    return est.astimezone(to_zone)

def json2list1(tableJson):
    outputTable = []
    for rowJson in tableJson:
        tmpRow = []
        for key in rowJson.keys():
            tmpRow.append(rowJson[key])
        outputTable.append(tmpRow)

    return (outputTable)


def json2list2(tableJson):
    columnTable = []
    for tableKey in tableJson:
        rowJson = tableJson[tableKey]
        for key in rowJson:
            if key not in columnTable:
                columnTable.append(key)
    outputTable = []
    for tableKey in tableJson:
        rowJson = tableJson[tableKey]
        tmpRow = []
        for key in columnTable:
            if key in rowJson.keys():
                tmpRow.append(rowJson[key])
            else:
                tmpRow.append("None")
        outputTable.append(tmpRow)

    return (columnTable, outputTable)


def json2list3(tableJson):
    columnTable = ['index', 'name', 'value']
    outputTable = []
    i = 0
    for tableKey in tableJson:
        i += 1
        value = tableJson[tableKey]
        outputTable.append([i, tableKey, value])
    return (columnTable, outputTable)
def checkTableExists(dbcur, tablename):
    dbcur.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        return True
    return False
def createTableUpdateIdSQL(dbcur):
    tablename = "UpdateId"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table UpdateId already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            updateId int NOT NULL,
            updateContent VARCHAR(40),
            updateDateTime TIMESTAMP,
            dataSet VARCHAR(40),
            PRIMARY KEY (updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (msg)

def createTableLogSQL(dbcur):
    tablename = "UpdateLog"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table UpdateId already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            logId int NOT NULL,
            updateId int,
            tableName VARCHAR(128),
            logDateTime TIMESTAMP,
            message VARCHAR(1024),
            updatedRows int,
            insertedRows int,
            skippedRows int,
            totalRows int
            PRIMARY KEY (logId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (msg)


def updateLog(osStr, conn, cursor, logList):
    logTableName = "UpdateLog"
    bExist = checkTableExists(cursor, logTableName)
    # bExist = True
    maxLogId = 0
    if bExist:
        cursor.execute(f"SELECT MAX(logId) FROM {logTableName};")
        maxLogId = cursor.fetchall()[0][0]
    else:
        msg = createTableLogSQL(cursor)
        # print(msg)

    # print("max logId:", maxLogId)
    if osStr == "Windows":
        sql_insertLog = "INSERT INTO " + logTableName + " VALUES (" + "?," * 8 + "?)"
    else:
        sql_insertLog = "INSERT INTO " + logTableName + " VALUES (" + "%s," * 8 + "%s)"
    try:
        i = maxLogId
        errFlag = 0
        for tmpLog in logList:
            i += 1
            logId = int(i)
            updateId = int(tmpLog['updateId'])
            tableName = tmpLog['table']
            logDate = tmpLog['time']
            msg = tmpLog['msg']
            nUpdate = int(tmpLog['nUpdate'])
            nInsert = int(tmpLog['nInsert'])
            nSkip = int(tmpLog['nSkip'])
            nTotal = int(tmpLog['nTotal'])
            row=tuple([logId, updateId, tableName, logDate, msg, nUpdate, nInsert, nSkip, nTotal])
            cursor.execute(sql_insertLog, row)
            if "error" in msg:
                errFlag = -1
    except Exception as e:
        conn.rollback()
        print(e)
        msg = "log updated failed. " + str(e)
    else:
        conn.commit()
        msg = "log updated"
    return (msg,errFlag)
def getUpdateIdSQL(osStr, conn, cursor, content, dataSet):
    tablename = "UpdateId"
    msg = createTableUpdateIdSQL(cursor)
    # print(msg)
    if osStr == "Windows":
        insertStatement = f"INSERT INTO {tablename} VALUES (?,?,?,?);"
    else:
        insertStatement = f"INSERT INTO {tablename} VALUES (%s,%s,%s,%s);"
    currentTime = datetime.now(timezone.utc)
    # print(currentTime)
    cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
    if cursor.rowcount > 0:
        result = cursor.fetchall()
        # print("result", result)
        updateIdLast = result[0][0]
        updateContentLast = result[0][1]
        updateDatetimeLast = result[0][2]
        # print(updateIdLast, updateContentLast, updateDatetimeLast)
        updateId = updateIdLast + 1
        record = (updateId, content, currentTime, dataSet)
        # print(record)
        try:
            cursor.execute(insertStatement, record)
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename, 'transaction rolled back')
        else:
            # print(tablename, 'record inserted successfully')
            conn.commit()
        return (updateId, currentTime)
    else:
        record = [1, content, currentTime, dataSet]
        try:
            cursor.execute(insertStatement, record)
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename, 'transaction rolled back')
        else:
            # print(tablename, 'record inserted successfully')
            conn.commit()
        return (1, currentTime)
def seasonTypeCreateTableSQL(dbcur, tablename):
    # tablename = "SeasonType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            year VARCHAR(20),
            typeId int NOT NULL,
            name VARCHAR(200),
            slug VARCHAR(100),
            updateId int NOT NULL,
            PRIMARY KEY(typeId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def seasonTypeInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "seasonType"
    (bExist, msg) = seasonTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT year,name, slug"
                " FROM "+ tablename +
                " WHERE typeId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET year=?,"
                "name=?,"
                "slug=?,"
                "updateId=?"
                " WHERE typeId=?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT year,name, slug"
                " FROM "+ tablename +
                " WHERE typeId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET year=%s,"
                "name=%s,"
                "slug=%s,"
                "updateId=%s"
                " WHERE typeId=%s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(row)
                    n += 1
                    year = int(row['year'])
                    typeId = int(row['type'])
                    name = str(row['name'])
                    slug = str(row['slug'])
                    updateId = int(row['updateId'])
                    # print(i,'row=',year,typeId,name,slug,updateId)
                    #sql1 = f"""SELECT year,name, slug
                    #            FROM {tablename}
                    #            WHERE typeId = {typeId};
                    #        """
                    #cursor.execute(sql1)
                    val = (typeId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        yearOld = int(rs[0])
                        nameOld = str(rs[1])
                        slugOld = str(rs[2])
                        # print(year,',',yearOld, year == yearOld)
                        # print(name,',',nameOld, name == nameOld)
                        # print(slug,',',slugOld, slug == slugOld)
                        if year != yearOld or name != nameOld or slug != slugOld:
                            #if osStr == "Windows":
                            #    sql2 = ("UPDATE " + tablename +
                            #                " SET year=?,"
                            #                   "name=?,"
                            #                   "slug=?,"
                            #                   "updateId=?"
                            #               " WHERE typeId=?;")
                            #else:
                            #    sql2 = ("UPDATE " + tablename +
                            #            " SET year=%s,"
                            #            "name=%s,"
                            #            "slug=%s,"
                            #            "updateId=%s"
                            #            " WHERE typeId=%s;")
                            val = (year, name, slug, updateId, typeId)
                            # print(sql2)
                            # print(i, "update", year, typeId, name, slug, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", year, typeId, name, slug, updateId)
                            msg=("skip")
                            nSkip += 1
                    else:
                        #sql3 = f"""INSERT INTO {tablename}
                        #            VALUES {year},{typeId},\"{name}\",\"{slug}\",{updateId};
                        #        """
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", year, typeId, name, slug, updateId)
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                # print(tuple(row))
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                currentTime = datetime.now(timezone.utc)
                msg = tablename + " update complete"
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def statusTypeCreateTableSQL(dbcur, tablename):
    # tablename = "statusType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            name VARCHAR(50),
            state VARCHAR(20),
            completed BIT,
            description VARCHAR(100),
            detail VARCHAR(100),
            shortDetail VARCHAR(50),
            updateId int NOT NULL, 
            PRIMARY KEY(id,updateId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def statusTypeInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "statusType"
    (bExist, msg) = statusTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT name, state, description"
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET name = ?,"
                "state = ?,"
                "description = ?,"
                "updateId = ?"
                "WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT name, state, description"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET name = %s,"
                "state = %s,"
                "description = %s,"
                "updateId = %s"
                "WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    id = int(row['id'])
                    name = str(row['name'])
                    state = str(row['state'])
                    completed = bool(row['completed'])
                    description = str(row['description'])
                    detail = str(row['detail'])
                    shortDetail = str(row['shortDetail'])
                    updateId = row['updateId']
                    # print(i, "from df", id, name, state, completed, description, detail, shortDetail, updateId)
                    #sql1 = f"""SELECT name, state, description
                    #            FROM {tablename}
                    #            WHERE id = {id};
                    #        """
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        nameOld = str(rs[0])
                        stateOld = str(rs[1])
                        descriptionOld = str(rs[2])
                        # print(year,',',yearOld, year == yearOld)
                        # print(name,',',nameOld, name == nameOld)
                        # print(slug,',',slugOld, slug == slugOld)
                        if name != nameOld or state != stateOld or description != descriptionOld:
                            #sql2 = ("UPDATE " + tablename +
                            #            " SET name = ?,"
                            #               "state = ?,"
                            #               "description = ?,"
                            #               "updateId = ?"
                            #           "WHERE id = ?;")
                            # print(sql2)
                            val=(name, state,description, updateId, id)
                            nUpdate += 1
                            # print(i, "update", id, name, state, description, updateId)
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", id, name, state, description, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", id, name, state, completed, description, detail, shortDetail, updateId)
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                #cursor.commit()
                currentTime = datetime.now(timezone.utc)
                msg = tablename + " update complete"
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def leaguesCreateTableSQL(dbcur, tablename):
    # tablename = "SeasonType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL ,
            alternateId INT,
            name VARCHAR(200),
            abbreviation VARCHAR(100),
            shortName VARCHAR(100),
            midsizeName VARCHAR(40),
            slug VARCHAR(100),
            seasonTypeId INT,
            seasonHasStandings BIT ,
            logoUrl1 VARCHAR(255),
            logoUrl1LastUpdated DATETIME,
            logoUrl2 VARCHAR(255),
            logoUrl2LastUpdated DATETIME,
            hasStandings BIT,
            updateTime TIMESTAMP,
            isTournament BIT,
            updateId INT, 
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def leaguesInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "statusType"
    (bExist, msg) = leaguesCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT alternateId, name, abbreviation, shortName,"
                "midsizeName, slug, logoUrl1, logoUrl2, isTournament"
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET alternateId = ?,"
                "name = ?,"
                "abbreviation = ?,"
                "shortName = ?,"
                "midsizeName = ?,"
                "slug = ?,"
                "seasonTypeId = ?,"
                "seasonHasStandings = ?,"
                "logoUrl1 = ?,"
                "logoUrl1LastUpdated = ?,"
                "logoUrl2 = ?,"
                "logoUrl2LastUpdated = ?,"
                "hasStandings = ?,"
                "updateTime = ?,"
                "isTournament = ?,"
                "updateId = ?"
                " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT alternateId, name, abbreviation, shortName,"
                "midsizeName, slug, logoUrl1, logoUrl2, isTournament"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET alternateId = %s,"
                "name = %s,"
                "abbreviation = %s,"
                "shortName = %s,"
                "midsizeName = %s,"
                "slug = %s,"
                "seasonTypeId = %s,"
                "seasonHasStandings = %s,"
                "logoUrl1 = %s,"
                "logoUrl1LastUpdated = %s,"
                "logoUrl2 = %s,"
                "logoUrl2LastUpdated = %s,"
                "hasStandings = %s,"
                "updateTime = %s,"
                "isTournament = %s,"
                "updateId = %s"
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(row)
                    n += 1
                    id = int(row['id'])
                    alternateId = int(row['alternateId'])
                    name = str(row['name'])
                    abbreviation = str(row['abbreviation'])
                    shortName = str(row['shortName'])
                    midsizeName = str(row['midsizeName'])
                    slug = str(row['slug'])
                    seasonTypeId = int(row['seasonTypeId'])
                    seasonHasStandings = bool(row['seasonHasStandings'])
                    logoUrl1 = str(row['logoUrl1'])
                    logoUrl1LastUpdated = row['logoUrl1LastUpdated']
                    logoUrl2 = str(row['logoUrl2'])
                    logoUrl2LastUpdated = row['logoUrl2LastUpdated']
                    hasStandings = bool(row['hasStandings'])
                    updateTime = row['updateTime']
                    isTournament = bool(row['isTournament'])
                    updateId = int(row['updateId'])
                    # print(i, "from df", id, name, shortName, slug, midsizeName, isTournament, updateTime, updateId)
                    #sql1 = f"""SELECT alternateId, name, abbreviation, shortName,
                    #                  midsizeName, slug, logoUrl1, logoUrl2, isTournament
                    #            FROM {tablename}
                    #            WHERE id = {id};
                    #        """
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        alternateIdOld = int(rs[0])
                        nameOld = str(rs[1])
                        abbreviationOld = str(rs[2])
                        shortNameOld = str(rs[3])
                        midsizeNameOld = str(rs[4])
                        slugOld = str(rs[5])
                        logoUrl1Old = str(rs[6])
                        logoUrl2Old = str(rs[7])
                        isTournamentOld = bool(rs[8])
                        # print(year,',',yearOld, year == yearOld)
                        # print(name,',',nameOld, name == nameOld)
                        # print(slug,',',slugOld, slug == slugOld)
                        if (name != nameOld or
                                alternateId != alternateIdOld or
                                abbreviation != abbreviationOld or
                                shortName != shortNameOld or
                                midsizeName != midsizeNameOld or
                                slug != slugOld or
                                logoUrl1 != logoUrl1Old or
                                logoUrl2 != logoUrl2Old or
                                isTournament != isTournamentOld):
                            # sql2 = ("UPDATE " + tablename +
                            #           " SET alternateId = ?,"
                            #                "name = ?,"
                            #                "abbreviation = ?,"
                            #                "shortName = ?,"
                            #                "midsizeName = ?,"
                            #                "slug = ?,"
                            #                "seasonTypeId = ?,"
                            #                "seasonHasStandings = ?,"
                            #                "logoUrl1 = ?,"
                            #                "logoUrl1LastUpdated = ?,"
                            #                "logoUrl2 = ?,"
                            #                "logoUrl2LastUpdated = ?,"
                            #                "hasStandings = ?,"
                            #                "updateTime = ?,"
                            #                "isTournament = ?,"
                            #                "updateId = ?"
                            #            " WHERE id = ?;")
                            val = (
                                alternateId,
                                name,
                                abbreviation,
                                shortName,
                                midsizeName,
                                slug,
                                seasonTypeId,
                                seasonHasStandings,
                                logoUrl1,
                                logoUrl1LastUpdated,
                                logoUrl2,
                                logoUrl2LastUpdated,
                                hasStandings,
                                updateTime,
                                isTournament,
                                updateId,
                                id
                                )
                            # print(sql2)
                            # print(tuple(row))
                            # print(i, "update", id, name, shortName, slug, midsizeName, isTournament, updateTime,
                            #       updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(sql)
                        else:
                            # print(i, "skip", id, name, shortName, slug, midsizeName,isTournament,updateTime, updateId)
                            msg = "skip"
                            nSkip += 1

                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", id, name, shortName, slug, midsizeName, isTournament, updateTime, updateId)
                        nSkip += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                msg = e
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def venuesCreateTableSQL(dbcur, tablename):
    # tablename = "Venues"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            fullName VARCHAR(255),
            shortName VARCHAR(128),
            capacity INT,
            city VARCHAR(255),
            country VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def venuesInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "Venues"
    (bExist, msg) = venuesCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT fullName, shortName, capacity, city, country"
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET fullName = ?,"
                " shortName = ?,"
                " capacity = ?," 
                " city = ?,"
                " country = ?,"
                " updateId = ?" 
                " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT fullName, shortName, capacity, city, country"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET fullName = %s,"
                " shortName = %s,"
                " capacity = %s," 
                " city = %s,"
                " country = %s,"
                " updateId = %s" 
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(tuple(row))
                    n += 1
                    id = int(row['id'])
                    fullName = str(row['fullName']).replace('"',"'")
                    shortName = str(row['shortName']).replace('"',"'")
                    capacity = int(row['capacity'])
                    city = str(row['address.city'])
                    country = str(row['address.country'])
                    updateId = row['updateId']
                    # print(i, "from df", id, fullName, shortName, capacity, city, country, updateId)
                    #sql1 = f"""SELECT fullName, shortName, capacity, city, country
                    #            FROM {tablename}
                    #            WHERE id = {id};
                    #        """
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        fullNameOld = str(rs[0])
                        shortNameOld = str(rs[1])
                        capacityOld = int(rs[2])
                        cityOld = str(rs[3])
                        countryOld = str(rs[4])
                        if (fullName != fullNameOld or
                                shortName != shortNameOld or
                                capacity != capacityOld or
                                city != cityOld or
                                country != countryOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET fullName = ?,
                            #                shortName = ?,
                            #                capacity = ?,
                            #                city = ?,
                            #                country = ?,
                            #                updateId = ?
                            #            WHERE id = ?;
                            #        """
                            # print(sql2)
                            val = (
                                fullName,
                                shortName,
                                capacity,
                                city,
                                country,
                                updateId,
                                id
                            )
                            # print(i, "update", id, fullName, shortName, capacity, city, country, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", id, fullName, shortName, capacity, city, country, updateId)
                            msg = "skip"
                            nSkip += 1

                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", id, fullName, shortName, capacity, city, country, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def positionTypeCreateTableSQL(dbcur, tablename):
    # tablename = "PositionType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            name VARCHAR(128),
            displayName VARCHAR(255),
            abbreviation VARCHAR(128),
            updateId INT,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def positionTypeInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "PositionType"
    (bExist, msg) = positionTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT name, displayName, abbreviation"
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET name = ?,"
                "     displayName = ?,"
                "     abbreviation = ?,"
                "     updateId = ?"
                " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT name, displayName, abbreviation"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET name = %s,"
                "     displayName = %s,"
                "     abbreviation = %s,"
                "     updateId = %s"
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(row)
                    n += 1
                    id = int(row['id'])
                    name = str(row['name'])
                    displayName = str(row['displayName'])
                    abbreviation = str(row['abbreviation'])
                    updateId = row['updateId']
                    # print(i, "from df", name, displayName, abbreviation, updateId)
                    # sql1 = f"""SELECT displayName, abbreviation
                    #            FROM {tablename}
                    #            WHERE name = \"{name}\";
                    #        """
                    # print(sql1)
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        nameOld = str(rs[0])
                        displayNameOld = str(rs[1])
                        abbreviationOld = str(rs[2])
                        if (name != nameOld or displayName != displayNameOld or
                                abbreviation != abbreviationOld):
                            val = (
                                name,
                                displayName,
                                abbreviation,
                                updateId,
                                id
                                )
                            # print(i, "update", name, displayName, abbreviation, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", name, displayName, abbreviation, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", name, displayName, abbreviation, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        print("positionType Insert",msg)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def statTypeCreateTableSQL(dbcur, tablename):
    # tablename = "StatType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            name VARCHAR(128) NOT NULL,
            displayName VARCHAR(255),
            shortDisplayName VARCHAR(255),
            description VARCHAR(255),
            abbreviation VARCHAR(128),
            updateId INT,
            PRIMARY KEY(name),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def statTypeInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "StatType"
    (bExist, msg) = statTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT displayName, shortDisplayName,description, abbreviation"
                " FROM " + tablename +
                " WHERE name = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET displayName = ?,"
                "     shortDisplayName = ?,"
                "     description = ?,"
                "     abbreviation = ?,"
                "     updateId = ?"
                " WHERE name = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT displayName, shortDisplayName,description, abbreviation"
                " FROM " + tablename +
                " WHERE name = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET displayName = %s,"
                "     shortDisplayName = %s,"
                "     description = %s,"
                "     abbreviation = %s,"
                "     updateId = %s"
                " WHERE name = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    name = str(row['name'])
                    displayName = str(row['displayName'])
                    shortDisplayName = str(row['shortDisplayName'])
                    description = str(row['description'])
                    abbreviation = str(row['abbreviation'])
                    updateId = row['updateId']
                    # print(i, "from df", name, displayName, abbreviation, updateId)
                    #sql1 = f"""SELECT displayName, shortDisplayName,description, abbreviation
                    #            FROM {tablename}
                    #            WHERE name = \"{name}\";
                    #        """
                    # print(sql1)
                    val = (name,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        displayNameOld = str(rs[0])
                        shortDisplayNameOld = str(rs[1])
                        descriptionOld = str(rs[2])
                        abbreviationOld = str(rs[3])
                        if (displayName != displayNameOld or
                                shortDisplayName != shortDisplayNameOld or
                                description != descriptionOld or
                                abbreviation != abbreviationOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET displayName = ?,
                            #                shortDisplayName = ?,
                            #                description = ?,
                            #                abbreviation = ?,
                            #                updateId = ?
                            #            WHERE name = ?;
                            #        """
                            # print(sql2)
                            val = ( displayName,
                                    shortDisplayName,
                                    description,
                                    abbreviation,
                                    updateId,
                                    name)
                            # print(i, "update", name, displayName, abbreviation, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", name, displayName, abbreviation, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", name, displayName, abbreviation, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamsCreateTableSQL(dbcur, tablename):
    # tablename = "Teams"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            teamId INT NOT NULL,
            uid VARCHAR(255),
            location VARCHAR(255),
            name VARCHAR(255),
            abbreviation VARCHAR(50),
            displayName VARCHAR(255),
            shortDisplayName VARCHAR(128),
            color VARCHAR(10),
            alternateColor VARCHAR(10),
            isActive BIT,
            logoURL VARCHAR(255),
            venueId INT,
            updateTime TIMESTAMP,
            slug VARCHAR(255),
            updateId INT,
            PRIMARY KEY(teamId),
            FOREIGN KEY(venueId) REFERENCES Venues(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def teamsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Teams"
    (bExist, msg) = teamsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "ntotal = ", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql_venue = "INSERT INTO Venues VALUES (?,?,?,?,?,?,?);"
        sql1 = ("SELECT uid, location, name, abbreviation, displayName, shortDisplayName,"
                " color, alternateColor, isActive, logoURL, venueId"
                " FROM " + tablename +
                " WHERE teamId = ?;")
        sql2 = ("UPDATE " + tablename +
                "  SET uid = ?,"
                "      location = ?,"
                "      name = ?,"
                "      abbreviation = ?,"
                "      displayName = ?,"
                "      shortDisplayName = ?,"
                "      color = ?,"
                "      alternateColor = ?,"
                "      isActive = ?,"
                "      logoURL = ?,"
                "      venueId = ?,"
                "      updateTime = ?,"
                "      slug = ?,"
                "      updateId = ?"
                "      WHERE teamId = ?;")
        sql22 = "INSERT INTO Venues VALUES (?,?,?,?,?,?,?)"
        sql23 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql_venue = "INSERT INTO Venues VALUES (%s,%s,%s,%s,%s,%s,%s);"
        sql1 = ("SELECT uid, location, name, abbreviation, displayName, shortDisplayName,"
                " color, alternateColor, isActive, logoURL, venueId"
                " FROM " + tablename +
                " WHERE teamId = %s;")
        sql2 = ("UPDATE " + tablename +
                "  SET uid = %s,"
                "      location = %s,"
                "      name = %s,"
                "      abbreviation = %s,"
                "      displayName = %s,"
                "      shortDisplayName = %s,"
                "      color = %s,"
                "      alternateColor = %s,"
                "      isActive = %s,"
                "      logoURL = %s,"
                "      venueId = %s,"
                "      updateTime = %s,"
                "      slug = %s,"
                "      updateId = %s"
                "      WHERE teamId = %s;")
        sql22 = "INSERT INTO Venues VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sql23 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            venueTablename = "Venues"
            venueIdList = []
            cursor.execute(f"SELECT id FROM {venueTablename};")
            rs = cursor.fetchall()
            for row in rs:
                venueIdList.append(row[0])
            print("venue list length:", len(venueIdList))
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(tuple(row))
                    teamId = int(row['teamId'])
                    uid = str(row['uid'])
                    location = str(row['location'])
                    name = str(row['name'])
                    abbreviation = str(row['abbreviation'])
                    displayName = str(row['displayName'])
                    shortDisplayName = str(row['shortDisplayName'])
                    color = str(row['color'])
                    alternateColor = str(row['alternateColor'])
                    isActive = bool(row['isActive'])
                    logoURL = str(row['logo'])
                    venueId = row['venueId']
                    updateTime = row['updateTime']
                    slug = str(row['slug'])
                    updateId = row['updateId']
                    if venueId not in venueIdList:
                        # print('insert venue', tuple(row))
                        print(sql_venue)
                        cursor.execute(sql_venue, tuple([venueId, "", "", 0, location, "", updateId]))
                        venueIdList.append(venueId)
                    # print(i, "from df", teamId, name, displayName, isActive, updateId)
                    #sql1 = f"""SELECT uid, location, name, abbreviation, displayName, shortDisplayName,
                    #                    color, alternateColor, isActive, logoURL, venueId
                    #            FROM {tablename}
                    #            WHERE teamId = {teamId};
                    #        """
                    val = (teamId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        uidOld = str(rs[0])
                        locationOld = str(rs[1])
                        nameOld = str(rs[2])
                        abbreviationOld = str(rs[3])
                        displayNameOld = str(rs[4])
                        shortDisplayNameOld = str(rs[5])
                        colorOld = str(rs[6])
                        alternateColorOld = str(rs[7])
                        isActiveOld = bool(rs[8])
                        logoURLOld = str(rs[9])
                        venueIdOld = int(rs[10])
                        if (uid != uidOld or
                                location != locationOld or
                                name != nameOld or
                                abbreviation != abbreviationOld or
                                displayName != displayNameOld or
                                shortDisplayName != shortDisplayNameOld or
                                color != colorOld or
                                alternateColor != alternateColorOld or
                                isActive != isActiveOld or
                                logoURL != logoURLOld or
                                venueId != venueIdOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET uid = ?,
                            #                location = ?,
                            #                name = ?,
                            #                abbreviation = ?,
                            #                displayName = ?,
                            #                shortDisplayName = ?,
                            #                color = ?,
                            #                alternateColor = ?,
                            #                isActive = ?,
                            #                logoURL = ?,
                            #                venueId = ?,
                            #                updateTime = ?,
                            #                slug = ?,
                            #                updateId = ?
                            #            WHERE teamId = ?;
                            #        """
                            # print(sql2)
                            val = ( uid,
                                    location,
                                    name,
                                    abbreviation,
                                    displayName,
                                    shortDisplayName,
                                    color,
                                    alternateColor,
                                    isActive,
                                    logoURL,
                                    venueId,
                                    updateTime,
                                    slug,
                                    updateId,
                                    teamId)
                            # print(i, "update", teamId, name, displayName, isActive, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", teamId, name, displayName, isActive, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", teamId, name, displayName, venueId, isActive, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        try:
            for i, row in df_records.iterrows():
                nInsert += 1
                tmpVenueId = tuple(row)[11]
                tmpUpdateId = tuple(row)[14]
                if tmpVenueId not in venueIdList:
                    print(tuple(row))
                    #sql22 = "INSERT INTO Venues VALUES (?,?,?,?,?,?,?)"
                    cursor.execute(sql22, tuple([tmpVenueId, "", "", 0, "", "", tmpUpdateId]))
                #sql23 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                # print(sql)
                cursor.execute(sql23, tuple(row))
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename, 'transaction rolled back')
            msg = tablename + " insert error:" + str(e)
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
        else:
            print(tablename, 'record inserted successfully')
            conn.commit()
            msg = tablename + " insert complete"
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamStatNameCreateTableSQL(dbcur, tablename):
    # tablename = "TeamStatName"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            stat VARCHAR(255),
            name VARCHAR(255),
            updateId INT,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def teamStatNameInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "TeamStatName"
    (bExist, msg) = teamStatNameCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT stat, name" 
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE "+ tablename +
                " SET stat = ?,"
                "     name = ?,"
                "     updateId = ?"
                " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT stat, name"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET stat = %s,"
                "     name = %s,"
                "     updateId = %s"
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    id = int(row['id'])
                    stat = str(row['stat'])
                    name = str(row['name'])
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    # sql1 = f"""SELECT stat, name
                    #            FROM {tablename}
                    #            WHERE id = {id};
                    #        """
                    # print(sql1)
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        statOld = str(rs[0])
                        nameOld = str(rs[1])
                        if (name != nameOld or
                                stat != statOld):
                            # sql2 = f"""UPDATE {tablename}
                            #            SET stat = ?,
                            #                name = ?,
                            #                updateId = ?
                            #            WHERE id = ?;
                            #        """
                            # print(sql2)
                            val = (stat,
                                    name,
                                    updateId,
                                    id)
                        # print(i, "update", id, stat, name, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", id, stat, name, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", id, stat, name, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
    else:
        msg = "update " + tablename
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamStatName2CreateTableSQL(dbcur, tablename):
    # tablename = "TeamStatName2"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            stat VARCHAR(255),
            statAbbreviation VARCHAR(255),
            updateId INT,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def teamStatName2InsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "TeamStatName2"
    (bExist, msg) = teamStatName2CreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT stat, statAbbreviation"
                " FROM " + tablename +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET stat = ?,"
                "     statAbbreviation = ?,"
                "     updateId = ?"
                " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT stat, statAbbreviation"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET stat = %s,"
                "     statAbbreviation = %s,"
                "     updateId = %s"
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    id = int(row['id'])
                    stat = str(row['stat'])
                    statAbbreviation = str(row['statAbbreviation'])
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    # sql1 = f"""SELECT stat, statAbbreviation
                    #            FROM {tablename}
                    #            WHERE id = {id};
                    #        """
                    # print(sql1)
                    val = (id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        statOld = str(rs[0])
                        statAbbreviationOld = str(rs[1])
                        if (stat != statOld or
                                statAbbreviation != statAbbreviationOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET stat = ?,
                            #                statAbbreviation = ?,
                            #                updateId = ?
                            #            WHERE id = ?;
                            #        """
                            # print(sql2)
                            val = (stat,statAbbreviation, updateId,id)
                            # print(i, "update", id, stat, statAbbreviation, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", id, stat, statAbbreviation, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", id, stat, statAbbreviation, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def fixturesCreateTableSQL(dbcur, tablename):
    # tablename = "Fixtures"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            leagueId INT,
            uid VARCHAR(100),
            attendance INT,
            date DATETIME,
            startDate DATETIME,
            neutralSite BIT,
            conferenceCompetition BIT,
            boxscoreAvailable BIT,
            commentaryAvailable BIT,
            recent BIT,
            boxscoreSource VARCHAR(10),
            playByPlaySource VARCHAR(10),
            seasonType INT,
            statusId INT,
            clock INT,
            displayClock VARCHAR(20),
            period INT,
            venueId INT,
            homeTeamId INT,
            homeTeamUid VARCHAR(100),
            homeTeamOrder INT,
            homeTeamWinner BIT,
            homeTeamScore INT,
            homeTeamShootoutScore INT,
            homeTeamForm VARCHAR(10),
            awayTeamId INT,
            awayTeamUid VARCHAR(100),
            awayTeamOrder INT,
            awayTeamWinner BIT,
            awayTeamScore INT,
            awayTeamShootoutScore INT,
            awayTeamForm VARCHAR(10),
            hasStats BIT,
            homeYellowCard INT,
            homeRedCard INT,
            awayYellowCard INT,
            awayRedCard INT,
            updateTime TIMESTAMP,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId),
            FOREIGN KEY(homeTeamId) REFERENCES Teams(teamId),
            FOREIGN KEY(awayTeamId) REFERENCES Teams(teamId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def fixturesInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Fixtures"
    (bExist, msg) = fixturesCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT "
                "leagueId," 
                "attendance,"
                "date,"
                "seasonType,"
                "statusId,"
                "displayClock,"
                "venueId,"
                "homeTeamId,"
                "awayTeamId,"
                "homeTeamOrder,"
                "homeTeamWinner,"
                "homeTeamScore," 
                "awayTeamScore," 
                "homeTeamShootoutScore,"
                "awayTeamShootoutScore,"
                "homeYellowCard,"
                "awayYellowCard,"
                "homeRedCard," 
                "awayRedCard"
                " FROM " + tablename +
                " WHERE eventId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET leagueId = ?,"
                "     uid = ?,"
                "     attendance = ?,"
                "     date = ?,"
                "     startDate = ?,"
                "     neutralSite = ?,"
                "     conferenceCompetition = ?,"
                "     boxscoreAvailable = ?,"
                "     commentaryAvailable = ?,"
                "     recent = ?,"
                "     boxscoreSource = ?,"
                "     playByPlaySource = ?,"
                "     seasonType = ?,"
                "     statusId = ?,"
                "     clock = ?,"
                "     displayClock = ?,"
                "     period = ?,"
                "     venueId = ?,"
                "     homeTeamId = ?,"
                "     homeTeamUid = ?,"
                "     homeTeamOrder = ?,"
                "     homeTeamWinner = ?,"
                "     homeTeamScore = ?,"
                "     homeTeamShootoutScore = ?,"
                "     homeTeamForm = ?,"
                "     awayTeamId = ?,"
                "     awayTeamUid = ?,"
                "     awayTeamOrder = ?,"
                "     awayTeamWinner = ?,"
                "     awayTeamScore = ?,"
                "     awayTeamShootoutScore = ?,"
                "     awayTeamForm = ?,"
                "     hasStats = ?,"
                "     homeYellowCard = ?,"
                "     homeRedCard = ?,"
                "     awayYellowCard = ?,"
                "     awayRedCard = ?,"
                "     updateTime = ?,"
                "     updateId = ?"
                " WHERE eventId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT "
                "leagueId,"
                "attendance,"
                "date,"
                "seasonType,"
                "statusId,"
                "displayClock,"
                "venueId,"
                "homeTeamId,"
                "awayTeamId,"
                "homeTeamOrder,"
                "homeTeamWinner,"
                "homeTeamScore,"
                "awayTeamScore,"
                "homeTeamShootoutScore,"
                "awayTeamShootoutScore,"
                "homeYellowCard,"
                "awayYellowCard,"
                "homeRedCard,"
                "awayRedCard"
                " FROM " + tablename +
                " WHERE eventId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET leagueId = %s,"
                "     uid = %s,"
                "     attendance = %s,"
                "     date = %s,"
                "     startDate = %s,"
                "     neutralSite = %s,"
                "     conferenceCompetition = %s,"
                "     boxscoreAvailable = %s,"
                "     commentaryAvailable = %s,"
                "     recent = %s,"
                "     boxscoreSource = %s,"
                "     playByPlaySource = %s,"
                "     seasonType = %s,"
                "     statusId = %s,"
                "     clock = %s,"
                "     displayClock = %s,"
                "     period = %s,"
                "     venueId = %s,"
                "     homeTeamId = %s,"
                "     homeTeamUid = %s,"
                "     homeTeamOrder = %s,"
                "     homeTeamWinner = %s,"
                "     homeTeamScore = %s,"
                "     homeTeamShootoutScore = %s,"
                "     homeTeamForm = %s,"
                "     awayTeamId = %s,"
                "     awayTeamUid = %s,"
                "     awayTeamOrder = %s,"
                "     awayTeamWinner = %s,"
                "     awayTeamScore = %s,"
                "     awayTeamShootoutScore = %s,"
                "     awayTeamForm = %s,"
                "     hasStats = %s,"
                "     homeYellowCard = %s,"
                "     homeRedCard = %s,"
                "     awayYellowCard = %s,"
                "     awayRedCard = %s,"
                "     updateTime = %s,"
                "     updateId = %s"
                " WHERE eventId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n+=1
                    # print(row)
                    eventId = int(row['eventId'])
                    leagueId = int(row['leagueId'])
                    uid = str(row['uid'])
                    attendance = int(row['attendance'])
                    matchDatetime = row['date']
                    date = row['date']
                    startDate = row['startDate']
                    neutralSite = bool(row['neutralSite'])
                    conferenceCompetition = bool(row['conferenceCompetition'])
                    boxscoreAvailable = bool(row['boxscoreAvailable'])
                    commentaryAvailable = bool(row['commentaryAvailable'])
                    recent = bool(row['recent'])
                    boxscoreSource = str(row['boxscoreSource'])
                    playByPlaySource = str(row['playByPlaySource'])
                    seasonType = int(row['seasonType'])
                    statusId = int(row['statusId'])
                    clock = int(row['clock'])
                    displayClock = str(row['displayClock'])
                    period = int(row['period'])
                    venueId = int(row['venueId'])
                    homeTeamId = int(row['homeTeamId'])
                    homeTeamUid = str(row['homeTeamUid'])
                    homeTeamOrder = int(row['homeTeamOrder'])
                    homeTeamWinner = bool(row['homeTeamWinner'])
                    homeTeamScore = int(row['homeTeamScore'])
                    homeTeamShootoutScore = int(row['homeTeamShootoutScore'])
                    homeTeamForm = str(row['homeTeamForm'])
                    awayTeamId = int(row['awayTeamId'])
                    awayTeamUid = str(row['awayTeamUid'])
                    awayTeamOrder = int(row['awayTeamOrder'])
                    awayTeamWinner = bool(row['awayTeamWinner'])
                    awayTeamScore = int(row['awayTeamScore'])
                    awayTeamShootoutScore = int(row['awayTeamShootoutScore'])
                    awayTeamForm = str(row['awayTeamForm'])
                    hasStats = bool(row['hasStats'])
                    homeYellowCard = int(row['homeYellowCard'])
                    homeRedCard = int(row['homeRedCard'])
                    awayYellowCard = int(row['awayYellowCard'])
                    awayRedCard = int(row['awayRedCard'])
                    updateTime = row['updateTime']
                    updateId = row['updateId']
                    # print(i, "from df", eventId, statusId, homeTeamId, awayTeamId, updateId)
                    #sql1 = f"""SELECT
                    #            leagueId,
                    #            attendance,
                    #            date,
                    #            seasonType,
                    #            statusId,
                    #            displayClock,
                    #            venueId,
                    #            homeTeamId,
                    #            awayTeamId,
                    #            homeTeamOrder,
                    #            homeTeamWinner,
                    #            homeTeamScore,
                    #            awayTeamScore,
                    #            homeTeamShootoutScore,
                    #            awayTeamShootoutScore,
                    #            homeYellowCard,
                    #            awayYellowCard,
                    #            homeRedCard,
                    #            awayRedCard
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId};
                    #        """
                    # print(sql1)
                    val = (eventId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        leagueIdOld = rs[0]
                        attendanceOld = rs[1]
                        #matchDatetimeOld = datetime.strptime(''.join(rs[2].rsplit(":",1), "%Y-%m-%d %H:%M:%S%z"))
                        # matchDatetimeOld = est2utc(rs[2])
                        matchDatetimeOld = rs[2].replace(tzinfo=tz.gettz("UTC"))
                        seasonTypeOld = rs[3]
                        statusIdOld = rs[4]
                        displayClockOld = rs[5]
                        venueIdOld = rs[6]
                        homeTeamIdOld = rs[7]
                        awayTeamIdOld = rs[8]
                        homeTeamOrderOld = rs[9]
                        homeTeamWinnerOld = rs[10]
                        homeTeamScoreOld = rs[11]
                        awayTeamScoreOld = rs[12]
                        homeTeamShootoutScoreOld = rs[13]
                        awayTeamShootoutScoreOld = rs[14]
                        homeYellowCardOld = rs[15]
                        awayYellowCardOld = rs[16]
                        homeRedCardOld = rs[17]
                        awayRedCardOld = rs[18]
                        if (leagueId != leagueIdOld or
                            attendance != attendanceOld or
                            matchDatetime != matchDatetimeOld or
                            seasonType != seasonTypeOld or
                            statusId != statusIdOld or
                            displayClock != displayClockOld or
                            venueId != venueIdOld or
                            homeTeamId != homeTeamIdOld or
                            awayTeamId != awayTeamIdOld or
                            homeTeamOrder != homeTeamOrderOld or
                            homeTeamWinner != homeTeamWinnerOld or
                            homeTeamScore != homeTeamScoreOld or
                            awayTeamScore != awayTeamScoreOld or
                            homeTeamShootoutScore != homeTeamShootoutScoreOld or
                            awayTeamShootoutScore != awayTeamShootoutScoreOld or
                            homeYellowCard != homeYellowCardOld or
                            awayYellowCard != awayYellowCardOld or
                            homeRedCard != homeRedCardOld or
                            awayRedCard != awayRedCardOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET leagueId = ?,
                            #                uid = ?,
                            #                attendance = ?,
                            #                date = ?,
                            #                startDate = ?,
                            #                neutralSite = ?,
                            #                conferenceCompetition = ?,
                            #                boxscoreAvailable = ?,
                            #                commentaryAvailable = ?,
                            #                recent = ?,
                            #                boxscoreSource = ?,
                            #                playByPlaySource = ?,
                            #                seasonType = ?,
                            #                statusId = ?,
                            #                clock = ?,
                            #                displayClock = ?,
                            #                period = ?,
                            #                venueId = ?,
                            #                homeTeamId = ?,
                            #                homeTeamUid = ?,
                            #                homeTeamOrder = ?,
                            #                homeTeamWinner = ?,
                            #                homeTeamScore = ?,
                            #                homeTeamShootoutScore = ?,
                            #                homeTeamForm = ?,
                            #                awayTeamId = ?,
                            #                awayTeamUid = ?,
                            #                awayTeamOrder = ?,
                            #                awayTeamWinner = ?,
                            #                awayTeamScore = ?,
                            #                awayTeamShootoutScore = ?,
                            #                awayTeamForm = ?,
                            #                hasStats = ?,
                            #                homeYellowCard = ?,
                            #                homeRedCard = ?,
                            #                awayYellowCard = ?,
                            #                awayRedCard = ?,
                            #                updateTime = ?,
                            #                updateId = ?
                            #            WHERE eventId = ?;
                            #        """
                            # print(sql2)
                            val=(leagueId,
                                    uid,
                                    attendance,
                                    matchDatetime,
                                    startDate,
                                    neutralSite,
                                    conferenceCompetition,
                                    boxscoreAvailable,
                                    commentaryAvailable,
                                    recent,
                                    boxscoreSource,
                                    playByPlaySource,
                                    seasonType,
                                    statusId,
                                    clock,
                                    displayClock,
                                    period,
                                    venueId,
                                    homeTeamId,
                                    homeTeamUid,
                                    homeTeamOrder,
                                    homeTeamWinner,
                                    homeTeamScore,
                                    homeTeamShootoutScore,
                                    homeTeamForm,
                                    awayTeamId,
                                    awayTeamUid,
                                    awayTeamOrder,
                                    awayTeamWinner,
                                    awayTeamScore,
                                    awayTeamShootoutScore,
                                    awayTeamForm,
                                    hasStats,
                                    homeYellowCard,
                                    homeRedCard,
                                    awayYellowCard,
                                    awayRedCard,
                                    updateTime,
                                    updateId,
                                    eventId)
                            # print(n, "update", eventId, leagueId, attendance, date, seasonType, statusId, homeTeamId, awayTeamId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(n, "skip", eventId, statusId, homeTeamId, awayTeamId, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(n, "insert", eventId, statusId, homeTeamId, awayTeamId, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamUniformCreateTableSQL(dbcur, tablename):
    # tablename = "TeamUniform"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            teamOrder TINYINT,
            teamColor VARCHAR(10),
            teamAlternateColor VARCHAR(10),yy
            uniformType VARCHAR(10),
            uniformColor VARCHAR(10),
            uniformAlternateColor VARCHAR(10),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def teamUniformInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "TeamUniform"
    (bExist, msg) = teamUniformCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT teamOrder, teamColor, teamAlternateColor, uniformType,"
                "        uniformColor, uniformAlternateColor"
                " FROM " + tablename +
                " WHERE eventId = ? and teamId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET teamOrder = ?,"
                "     teamColor = ?,"
                "     teamAlternateColor = ?,"
                "     uniformType = ?,"
                "     uniformColor = ?,"
                "     uniformAlternateColor = ?,"
                "     updateId = ?"
                " WHERE eventId = ? and teamId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT teamOrder, teamColor, teamAlternateColor, uniformType,"
                "        uniformColor, uniformAlternateColor"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET teamOrder = %s,"
                "     teamColor = %s,"
                "     teamAlternateColor = %s,"
                "     uniformType = %s,"
                "     uniformColor = %s,"
                "     uniformAlternateColor = %s,"
                "     updateId = %s"
                " WHERE eventId = %s and teamId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    eventId = int(row['id'])
                    teamId = int(row['teamId'])
                    teamOrder = int(row['teamOrder'])
                    teamColor = str(row['teamColor'])
                    teamAlternateColor = str(row['teamAlternateColor'])
                    uniformType = row['uniformType']
                    uniformColor = str(row['uniformColor'])
                    uniformAlternateColor = str(row['uniformAlternateColor'])
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT teamOrder, teamColor, teamAlternateColor, uniformType,
                    #                    uniformColor, uniformAlternateColor
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId} and teamId = {teamId};
                    #        """
                    # print(sql1)
                    val = (eventId,teamId)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        teamOrderOld = int(rs[0])
                        teamColorOld = str(rs[1])
                        teamAlternateColorOld = str(rs[2])
                        uniformTypeOld = rs[3]
                        uniformColorOld = str(rs[4])
                        uniformAlternateColorOld = str(rs[5])
                        if (teamOrder != teamOrderOld or
                                teamColor != teamColorOld or
                                teamAlternateColor != teamAlternateColorOld or
                                uniformType != uniformTypeOld or
                                uniformColor != uniformColorOld or
                                uniformAlternateColor != uniformAlternateColorOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET teamOrder = ?,
                            #                teamColor = ?,
                            #                teamAlternateColor = ?,
                            #                uniformType = ?,
                            #                uniformColor = ?,
                            #                uniformAlternateColor = ?,
                            #                updateId = ?
                            #            WHERE eventId = ? and teamId = ?;
                            #        """
                            # print(sql2)
                            val = (teamOrder,
                                    teamColor,
                                    teamAlternateColor,
                                    uniformType,
                                    uniformColor,
                                    uniformAlternateColor,
                                    updateId,
                                   eventId,
                                   teamId)
                            # print(i, "update", eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                            #            uniformColor, uniformAlternateColor, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", id, eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                            #      uniformColor, uniformAlternateColor, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                        #       uniformColor, uniformAlternateColor, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamStatsCreateTableSQL(dbcur, tablename):
    # tablename = "TeamStats"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            teamOrder TINYINT,
            updateTime TIMESTAMP,
            hasStats BIT,
            foulsCommitted INT,
            yellowCards INT,
            redCards INT,
            offsides INT,
            wonCorners INT,
            saves INT,
            possessionPct FLOAT,
            totalShots INT,
            shotsOnTarget INT,
            shotPct FLOAT,
            penaltyKickGoals INT,
            penaltyKickShots INT,
            accuratePasses INT,
            totalPasses INT,
            passPct FLOAT,
            accurateCrosses INT,
            totalCrosses INT,
            crossPct FLOAT,
            totalLongBalls INT,
            accurateLongBalls INT,
            longballPct FLOAT,
            blockedShots INT,
            effectiveTackles INT,
            totalTackles INT,
            tacklePct FLOAT,
            interceptions INT,
            effectiveClearance INT,
            totalClearance INT,
            goalDifference INT,
            totalGoals INT,
            goalAssists INT,
            goalsConceded INT,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,teamId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def teamStatsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "TeamStats"
    (bExist, msg) = teamStatsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT teamOrder, hasStats, foulsCommitted, yellowCards, redCards, offsides,"
                "       woncorners, saves, possessionPct, totalShots, shotsOnTarget, shotPct,"
                "       penaltyKickGoals, penaltyKickShots,accuratePasses,totalPasses, passPct,"
                "       accurateCrosses, totalCrosses, crossPct, totalLongBalls, accurateLongBalls,"
                "       longballPct, blockedShots,"
                "       effectiveTackles, totalTackles, tacklePct, interceptions, effectiveClearance,"
                "       totalClearance, goalDifference, totalGoals, goalAssists, goalsConceded"
                " FROM " + tablename +
                " WHERE eventId = ? and teamId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET teamOrder = ?,"
                "     updateTime = ?,"
                "     hasStats = ?,"
                "     foulsCommitted = ?,"
                "     yellowCards = ?,"
                "     redCards = ?,"
                "     offsides = ?,"
                "     wonCorners = ?,"
                "     saves = ?,"
                "     possessionPct = ?," 
                "     totalShots = ?,"
                "     shotsOnTarget = ?," 
                "     shotPct = ?,"
                "     penaltyKickGoals = ?," 
                "     penaltyKickShots = ?," 
                "     accuratePasses = ?," 
                "     totalPasses = ?," 
                "     passPct = ?," 
                "     accurateCrosses = ?," 
                "     totalCrosses = ?," 
                "     crossPct = ?," 
                "     totalLongBalls = ?," 
                "     accurateLongBalls = ?," 
                "     longballPct = ?," 
                "     blockedShots = ?," 
                "     effectiveTackles = ?," 
                "     totalTackles = ?," 
                "     tacklePct = ?," 
                "     interceptions = ?," 
                "     effectiveClearance = ?," 
                "     totalClearance = ?," 
                "     goalDifference = ?," 
                "     totalGoals = ?," 
                "     goalAssists = ?," 
                "     goalsConceded = ?,"
                "     updateId = ?" 
                " WHERE eventId = ? and teamId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT teamOrder, hasStats, foulsCommitted, yellowCards, redCards, offsides,"
                "       woncorners, saves, possessionPct, totalShots, shotsOnTarget, shotPct,"
                "       penaltyKickGoals, penaltyKickShots,accuratePasses,totalPasses, passPct,"
                "       accurateCrosses, totalCrosses, crossPct, totalLongBalls, accurateLongBalls,"
                "       longballPct, blockedShots,"
                "       effectiveTackles, totalTackles, tacklePct, interceptions, effectiveClearance,"
                "       totalClearance, goalDifference, totalGoals, goalAssists, goalsConceded"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET teamOrder = %s,"
                "     updateTime = %s,"
                "     hasStats = %s,"
                "     foulsCommitted = %s,"
                "     yellowCards = %s,"
                "     redCards = %s,"
                "     offsides = %s,"
                "     wonCorners = %s,"
                "     saves = %s,"
                "     possessionPct = %s,"
                "     totalShots = %s,"
                "     shotsOnTarget = %s,"
                "     shotPct = %s,"
                "     penaltyKickGoals = %s,"
                "     penaltyKickShots = %s,"
                "     accuratePasses = %s,"
                "     totalPasses = %s,"
                "     passPct = %s,"
                "     accurateCrosses = %s,"
                "     totalCrosses = %s,"
                "     crossPct = %s,"
                "     totalLongBalls = %s,"
                "     accurateLongBalls = %s,"
                "     longballPct = %s,"
                "     blockedShots = %s,"
                "     effectiveTackles = %s,"
                "     totalTackles = %s,"
                "     tacklePct = %s,"
                "     interceptions = %s,"
                "     effectiveClearance = %s,"
                "     totalClearance = %s,"
                "     goalDifference = %s,"
                "     totalGoals = %s,"
                "     goalAssists = %s,"
                "     goalsConceded = %s,"
                "     updateId = %s"
                " WHERE eventId = %s and teamId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    eventId = int(row['eventId'])
                    teamId = int(row['teamId'])
                    teamOrder = int(row['teamOrder'])
                    updateTime = row['updateTime']
                    hasStats = bool(row['hasStats'])
                    foulsCommitted = int(row['foulsCommitted'])
                    yellowCards = int(row['yellowCards'])
                    redCards = int(row['redCards'])
                    offsides = int(row['offsides'])
                    wonCorners = int(row['wonCorners'])
                    saves = int(row['saves'])
                    possessionPct = float(row['possessionPct'])
                    totalShots = int(row['totalShots'])
                    shotsOnTarget = int(row['shotsOnTarget'])
                    shotPct = float(row['shotPct'])
                    penaltyKickGoals = int(row['penaltyKickGoals'])
                    penaltyKickShots = int(row['penaltyKickShots'])
                    accuratePasses = int(row['accuratePasses'])
                    totalPasses = int(row['totalPasses'])
                    passPct = float(row['passPct'])
                    accurateCrosses = int(row['accurateCrosses'])
                    totalCrosses = int(row['totalCrosses'])
                    crossPct = float(row['crossPct'])
                    totalLongBalls = int(row['totalLongBalls'])
                    accurateLongBalls = int(row['accurateLongBalls'])
                    longballPct = float(row['longballPct'])
                    blockedShots = int(row['blockedShots'])
                    effectiveTackles = int(row['effectiveTackles'])
                    totalTackles = int(row['totalTackles'])
                    tacklePct = float(row['tacklePct'])
                    interceptions = int(row['interceptions'])
                    effectiveClearance = int(row['effectiveClearance'])
                    totalClearance = int(row['totalClearance'])
                    goalDifference = int(row['goalDifference'])
                    totalGoals = int(row['totalGoals'])
                    goalAssists = int(row['goalAssists'])
                    goalsConceded = int(row['goalsConceded'])
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT teamOrder, hasStats, foulsCommitted, yellowCards, redCards, offsides,
                    #                    woncorners, saves, possessionPct, totalShots, shotsOnTarget, shotPct,
                    #                    penaltyKickGoals, penaltyKickShots,accuratePasses,totalPasses, passPct,
                    #                    accurateCrosses, totalCrosses, crossPct, totalLongBalls, accurateLongBalls,
                    #                    longballPct, blockedShots,
                    #                    effectiveTackles, totalTackles, tacklePct, interceptions, effectiveClearance,
                    #                    totalClearance, goalDifference, totalGoals, goalAssists, goalsConceded
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId} and teamId = {teamId};
                    #        """
                    # print(sql1)
                    val = (eventId, teamId)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        teamOrderOld = rs[0]
                        hasStatsOld = rs[1]
                        foulsCommittedOld = rs[2]
                        yellowCardsOld = rs[3]
                        redCardsOld = rs[4]
                        offsidesOld = rs[5]
                        wonCornersOld = rs[6]
                        savesOld = rs[7]
                        possessionPctOld = rs[8]
                        totalShotsOld = rs[9]
                        shotsOnTargetOld = rs[10]
                        shotPctOld = rs[11]
                        penaltyKickGoalsOld = rs[12]
                        penaltyKickShotsOld = rs[13]
                        accuratePassesOld = rs[14]
                        totalPassesOld = rs[15]
                        passPctOld = rs[16]
                        accurateCrossesOld = rs[17]
                        totalCrossesOld = rs[18]
                        crossPctOld = rs[19]
                        totalLongBallsOld = rs[20]
                        accurateLongBallsOld = rs[21]
                        longballPctOld = rs[22]
                        blockedShotsOld = rs[23]
                        effectiveTacklesOld = rs[24]
                        totalTacklesOld = rs[25]
                        tacklePctOld = rs[26]
                        interceptionsOld = rs[27]
                        effectiveClearanceOld = rs[28]
                        totalClearanceOld = rs[29]
                        goalDifferenceOld = rs[30]
                        totalGoalsOld = rs[31]
                        goalAssistsOld = rs[32]
                        goalsConcededOld = rs[33]
                        if (teamOrder != teamOrderOld or
                                hasStats != hasStatsOld or
                                foulsCommitted != foulsCommittedOld or
                                yellowCards != yellowCardsOld or
                                redCards != redCardsOld or
                                offsides != offsidesOld or
                                wonCorners != wonCornersOld or
                                saves != savesOld or
                                possessionPct != possessionPctOld or
                                totalShots != totalShotsOld or
                                shotsOnTarget != shotsOnTargetOld or
                                shotPct != shotPctOld or
                                penaltyKickGoals != penaltyKickGoalsOld or
                                penaltyKickShots != penaltyKickShotsOld or
                                accuratePasses != accuratePassesOld or
                                totalPasses != totalPassesOld or
                                passPct != passPctOld or
                                accurateCrosses != accurateCrossesOld or
                                totalCrosses != totalCrossesOld or
                                crossPct != crossPctOld or
                                totalLongBalls != totalLongBallsOld or
                                accurateLongBalls != accurateLongBallsOld or
                                longballPct != longballPctOld or
                                blockedShots != blockedShotsOld or
                                effectiveTackles != effectiveTacklesOld or
                                totalTackles != totalTacklesOld or
                                tacklePct != tacklePctOld or
                                interceptions != interceptionsOld or
                                effectiveClearance != effectiveClearanceOld or
                                totalClearance != totalClearanceOld or
                                goalDifference != goalDifferenceOld or
                                totalGoals != totalGoalsOld or
                                goalAssists != goalAssistsOld or
                                goalsConceded != goalsConcededOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET teamOrder = ?,
                            #                updateTime = ?,
                            #                hasStats = ?,
                            #                foulsCommitted = ?,
                            #                yellowCards = ?,
                            #                redCards = ?,
                            #                offsides = ?,
                            #                wonCorners = ?,
                            #                saves = ?,
                            #                possessionPct = ?,
                            #                totalShots = ?,
                            #                shotsOnTarget = ?,
                            #                shotPct = ?,
                            #                penaltyKickGoals = ?,
                            #                penaltyKickShots = ?,
                            #                accuratePasses = ?,
                            #                totalPasses = ?,
                            #                passPct = ?,
                            #                accurateCrosses = ?,
                            #                totalCrosses = ?,
                            #                crossPct = ?,
                            #                totalLongBalls = ?,
                            #                accurateLongBalls = ?,
                            #                longballPct = ?,
                            #                blockedShots = ?,
                            #                effectiveTackles = ?,
                            #                totalTackles = ?,
                            #                tacklePct = ?,
                            #                interceptions = ?,
                            #                effectiveClearance = ?,
                            #                totalClearance = ?,
                            #                goalDifference = ?,
                            #                totalGoals = ?,
                            #                goalAssists = ?,
                            #                goalsConceded = ?,
                            #                updateId = ?
                            #            WHERE eventId = ? and teamId = ?;
                            #        """
                            # print(sql2)
                            val = (teamOrder,
                                    updateTime,
                                    hasStats,
                                    foulsCommitted,
                                    yellowCards,
                                    redCards,
                                    offsides,
                                    wonCorners,
                                    saves,
                                    possessionPct,
                                    totalShots,
                                    shotsOnTarget,
                                    shotPct,
                                    penaltyKickGoals,
                                    penaltyKickShots,
                                    accuratePasses,
                                    totalPasses,
                                    passPct,
                                    accurateCrosses,
                                    totalCrosses,
                                    crossPct,
                                    totalLongBalls,
                                    accurateLongBalls,
                                    longballPct,
                                    blockedShots,
                                    effectiveTackles,
                                    totalTackles,
                                    tacklePct,
                                    interceptions,
                                    effectiveClearance,
                                    totalClearance,
                                    goalDifference,
                                    totalGoals,
                                    goalAssists,
                                    goalsConceded,
                                    updateId,
                                    eventId,
                                    teamId)
                            # print(i, "update", eventId, teamId, teamOrder, hasStats, updateId)
                            nUpdate += 1
                            # print(rs)
                            # print(tuple(row))
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", eventId, teamId, teamOrder, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, teamId, teamOrder, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def attendanceCreateTableSQL(dbcur, tablename):
    # tablename = "Attendance"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            attendance INT,
            updateId INT,
            PRIMARY KEY(eventId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def attendanceInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Attendance"
    (bExist, msg) = attendanceCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT attendance"
                " FROM " + tablename +
                " WHERE eventId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET attendance = ?,"
                "     updateId =?"
                " WHERE eventId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT attendance"
                " FROM " + tablename +
                " WHERE eventId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET attendance = %s,"
                "     updateId =%s"
                " WHERE eventId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    eventId = int(row['eventId'])
                    attendance = int(row['attendance'])
                    updateId = int(row['updateId'])
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT attendance
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId};
                    #        """
                    # print(sql1)
                    val = (eventId,)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        attendanceOld = int(rs[0])
                        if (attendance != attendanceOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET attendance = ?,
                            #                updateId =?
                            #            WHERE eventId = ?;
                            #        """
                            # print(sql2)
                            val = (attendance,updateId,eventId)
                            # print(i, "update", eventId, attendance, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", id, eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                            #      uniformColor, uniformAlternateColor, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, attendance, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def officialsCreateTableSQL(dbcur, tablename):
    # tablename = "Officials"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            fullName VARCHAR(128),
            displayName VARCHAR(128),
            refOrder INT NOT NULL,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,refOrder),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def officialsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Officials"
    (bExist, msg) = officialsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT fullName,displayName" 
                " FROM " + tablename +
                " WHERE eventId = ? and refOrder = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET fullName = ?,"
                "     displayName = ?,"
                "     updateId = ?"
                " WHERE eventId = ? and refOrder = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT fullName,displayName"
                " FROM " + tablename +
                " WHERE eventId = %s and refOrder = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET fullName = %s,"
                "     displayName = %s,"
                "     updateId = %s"
                " WHERE eventId = %s and refOrder = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateId DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            # print(msg)
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(row)
                    n += 1
                    eventId = int(row['eventId'])
                    fullName = row['fullName']
                    displayName = row['displayName']
                    refOrder = int(row['order'])
                    updateId = int(row['updateId'])
                    # print(i, "from df", eventId, fullName,displayName, refOrder, updateId)
                    #sql1 = f"""SELECT fullName,displayName
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId} and refOrder = {refOrder};
                    #        """
                    # print(sql1)
                    val = (eventId, refOrder)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        rs = cursor.fetchone()
                        # print(rs)
                        fullNameOld = rs[0]
                        displayNameOld = rs[1]
                        if (fullName != fullNameOld or
                            displayName != displayNameOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET fullName = ?,
                            #                displayName = ?,
                            #                updateId = ?
                            #            WHERE eventId = ? and refOrder = ?;
                            #        """
                            # print(sql2)
                            # print(val)
                            val = (fullName,displayName,updateId,eventId,refOrder)
                            # print(i, "update", eventId, refOrder,fullName,displayName, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        else:
                            # print(i, "skip", eventId, refOrder, fullName, displayName, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, refOrder, fullName, displayName, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def venueDBCreateTableSQL(dbcur, tablename):
    # tablename = "VenueDB"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            venueId INT NOT NULL,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,venueId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(venueId) REFERENCES Venues(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def venueDBInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "venueDB"
    (bExist, msg) = venueDBCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT venueId"
                " FROM " + tablename +
                " WHERE eventId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET venueId = ?,"
                "     updateId = ?" 
                " WHERE eventId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT venueId"
                " FROM " + tablename +
                " WHERE eventId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET venueId = %s,"
                "     updateId = %s"
                " WHERE eventId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    eventId = int(row['eventId'])
                    venueId = int(row['id'])
                    updateId = int(row['updateId'])
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT venueId
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId};
                    #        """
                    # print(sql1)
                    val = (eventId,)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        venueIdOld = int(rs[0])
                        if (venueId != venueIdOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET venueId = ?,
                            #                updateId = ?
                            #            WHERE eventId = ?;
                            #        """
                            # print(sql2)
                            val = (venueId,updateId,eventId)
                            # print(i, "update", eventId, venueId, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", eventId, venueId, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, venueId, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def keyEventTypeCreateTableSQL(dbcur, tablename):
    # tablename = "KeyEventType"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            keyEventId INT NOT NULL,
            keyEventName VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def keyEventTypeInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "KeyEventType"
    (bExist, msg) = keyEventTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT keyEventName"
                " FROM " + tablename +
                " WHERE keyEventId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET keyEventName = ?,"
                "     updateId = ?"
                " WHERE keyEventId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT keyEventName"
                " FROM " + tablename +
                " WHERE keyEventId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET keyEventName = %s,"
                "     updateId = %s"
                " WHERE keyEventId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    keyEventId = row['id']
                    keyEventName = row['text']
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT keyEventName
                    #            FROM {tablename}
                    #            WHERE keyEventId = {keyEventId};
                    #        """
                    # print(sql1)
                    val = (keyEventId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        keyEventNameOld = rs[0]
                        if (keyEventName != keyEventNameOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET keyEventName = ?,
                            #                updateId = ?
                            #           WHERE keyEventId = ?;
                            #       """
                            # print(sql2)
                            val = (keyEventName,updateId,keyEventId)
                            nUpdate += 1
                            # print(i, "update", keyEventId, keyEventName, updateId)
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", keyEventId, keyEventName, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", keyEventId, keyEventName, updateId)
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def keyEventSourceCreateTableSQL(dbcur, tablename):
    # tablename = "KeyEventSource"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            keyEventSourceIndex INT NOT NULL,
            keyEventSourceId INT NOT NULL,
            keyEventSourceName VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventSourceId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def keyEventSourceInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "KeyEventSource"
    (bExist, msg) = keyEventSourceCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT keyEventSourceName"
                " FROM " + tablename +
                " WHERE keyEventSourceId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET keyEventSourceIndex = ?," 
                "     keyEventSourceName = ?,"
                "     updateId = ?"
                " WHERE keyEventSourceId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT keyEventSourceName"
                " FROM " + tablename +
                " WHERE keyEventSourceId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET keyEventSourceIndex = %s,"
                "     keyEventSourceName = %s,"
                "     updateId = %s"
                " WHERE keyEventSourceId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    keyEventSourceIndex = row['index']
                    keyEventSourceId = row['id']
                    keyEventSourceName = row['name']
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT keyEventSourceName
                    #            FROM {tablename}
                    #            WHERE keyEventSourceId = {keyEventSourceId};
                    #        """
                    # print(sql1)
                    val = (keyEventSourceId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        keyEventSourceNameOld = rs[0]
                        if (keyEventSourceName != keyEventSourceNameOld):
                            #sql2 = f"""UPDATE {tablename}
                            #            SET keyEventSourceIndex = ?,
                            #                keyEventSourceName = ?,
                            #                updateId = ?
                            #            WHERE keyEventSourceId = {keyEventSourceId};
                            #        """
                            # print(sql2)
                            val = (keyEventSourceIndex,keyEventSourceName,updateId,keyEventSourceId)
                            nUpdate += 1
                            # print(i, "update", keyEventSourceIndex, keyEventSourceId, keyEventSourceName, updateId)
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            # print(i, "skip", keyEventSourceIndex, keyEventSourceId, keyEventSourceName, updateId)
                            msg = "skip"
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", keyEventSourceIndex, keyEventSourceId, keyEventSourceName, updateId)
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playsCreateTableSQL(dbcur, tablename):
    # tablename = "Plays"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            playOrder INT NOT NULL,
            playId INT,
            typeId INT,
            text VARCHAR(255),
            shortText VARCHAR(255),
            period INT,
            clockValue INT,
            clockDisplayValue VARCHAR(128),
            teamDisplayName VARCHAR(128),
            teamId INT,
            sourceId INT,
            scoringPlay BIT,
            shootout BIT,
            wallclock DATETIME,
            goalPositionX FLOAT,
            goalPositionY FLOAT,
            fieldPositionX FLOAT,
            fieldPositionY FLOAT,
            fieldPosition2X FLOAT,
            fieldPosition2Y FLOAT,
            updateId INT NOT NULL,
            PRIMARY KEY(playId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playsInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "Plays"
    (bExist, msg) = playsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records_full.iloc[0]['updateId']
    bInsert = True
    nSuccess = 0
    if osStr == "Windows":
        sql1 = ("SELECT playOrder,"
                            "typeId,"
                            "text,"
                            "shortText,"
                            "period,"
                            "clockValue,"
                            "clockDisplayValue,"
                            "teamDisplayName,"
                            "teamId,"
                            "sourceId,"
                            "scoringPlay,"
                            "shootout,"
                            "wallClock,"
                            "goalPositionX,"
                            "goalPositionY,"
                            "fieldPositionX,"
                            "fieldPositionY,"
                            "fieldPosition2X,"
                            "fieldPosition2Y"
                    " FROM " + tablename +
                    " WHERE eventId = ? and playId= ?;")
        sql2 = ("UPDATE " + tablename +
                    " SET playOrder = ?,"
                    "    typeId = ?,"
                    "    text = ?,"
                    "    shortText = ?,"
                    "    period = ?,"
                    "    clockValue = ?,"
                    "    clockDisplayValue = ?,"
                    "    teamDisplayName = ?,"
                    "    teamId = ?,"
                    "    sourceId = ?,"
                    "    scoringPlay = ?,"
                    "    shootout = ?,"
                    "    wallclock = ?,"
                    "    goalPositionX = ?,"
                    "    goalPositionY = ?,"
                    "    fieldPositionX = ?,"
                    "    fieldPositionY = ?,"
                    "    fieldPosition2X = ?,"
                    "    fieldPosition2Y = ?,"
                    "    updateId = ?"
                    " WHERE eventId = ? and playId = ?;")
    else:
        sql1 = ("SELECT playOrder,"
                "typeId,"
                "text,"
                "shortText,"
                "period,"
                "clockValue,"
                "clockDisplayValue,"
                "teamDisplayName,"
                "teamId,"
                "sourceId,"
                "scoringPlay,"
                "shootout,"
                "wallClock,"
                "goalPositionX,"
                "goalPositionY,"
                "fieldPositionX,"
                "fieldPositionY,"
                "fieldPosition2X,"
                "fieldPosition2Y"
                " FROM " + tablename +
                " WHERE eventId = %s and playId= %s;")
        sql2 = ("UPDATE " + tablename +
                " SET playOrder = %s,"
                "    typeId = %s,"
                "    text = %s,"
                "    shortText = %s,"
                "    period = %s,"
                "    clockValue = %s,"
                "    clockDisplayValue = %s,"
                "    teamDisplayName = %s,"
                "    teamId = %s,"
                "    sourceId = %s,"
                "    scoringPlay = %s,"
                "    shootout = %s,"
                "    wallclock = %s,"
                "    goalPositionX = %s,"
                "    goalPositionY = %s,"
                "    fieldPositionX = %s,"
                "    fieldPositionY = %s,"
                "    fieldPosition2X = %s,"
                "    fieldPosition2Y = %s,"
                "    updateId = %s"
                " WHERE eventId = %s and playId = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents=len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for tmpEventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == tmpEventId]
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                n += 1
                try:
                    for i, row in df_records.iterrows():
                        # print(row)
                        eventId = int(tmpEventId)
                        playOrder = int(row['order'])
                        playId = int(row['id'])
                        typeId = int(row['typeId'])
                        text = row['text']
                        shortText = row['shortText']
                        period = int(row['period'])
                        clockValue = int(row['clockValue'])
                        clockDisplayValue = row['clockDisplayValue']
                        teamDisplayName = str(row['teamDisplayName'])
                        teamId = int(row['teamId'])
                        sourceId = int(row['sourceId'])
                        scoringPlay = bool(row['scoringPlay'])
                        shootout = bool(row['shootout'])
                        wallclock = row['wallclock']
                        goalPositionX = row['goalPositionX']
                        goalPositionY = row['goalPositionY']
                        fieldPositionX = row['fieldPositionX']
                        fieldPositionY = row['fieldPositionY']
                        fieldPosition2X = row['fieldPosition2X']
                        fieldPosition2Y = row['fieldPosition2Y']
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT playOrder,
                        #                    typeId,
                        #                    text,
                        #                    shortText,
                        #                    period,
                        #                    clockValue,
                        #                    clockDisplayValue,
                        #                    teamDisplayName,
                        #                    teamId,
                        #                    sourceId,
                        #                    scoringPlay,
                        #                    shootout,
                        #                    wallClock,
                        #                    goalPositionX,
                        #                    goalPositionY,
                        #                    fieldPositionX,
                        #                    fieldPositionY,
                        #                    fieldPosition2X,
                        #                    fieldPosition2Y
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and playId= {playId};
                        #        """
                        # print(sql1)
                        val = (eventId,playId)
                        cursor.execute(sql1,val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            playOrderOld = rs[0]
                            typeIdOld = rs[1]
                            textOld = rs[2]
                            # if textOld == "None":
                            #     textOld = None
                            shortTextOld = rs[3]
                            # if shortTextOld == "None":
                            #    shortTextOld = None
                            periodOld = rs[4]
                            clockValueOld = rs[5]
                            clockDisplayValueOld = rs[6]
                            teamDisplayNameOld = str(rs[7])
                            # if teamDisplayNameOld == "None":
                            #    teamDisplayNameOld = None
                            teamIdOld = rs[8]
                            sourceIdOld = rs[9]
                            scoringPlayOld = rs[10]
                            shootoutOld = rs[11]
                            wallclockOld = rs[12].replace(tzinfo=tz.gettz('UTC'))
                            goalPositionXOld = rs[13]
                            goalPositionYOld = rs[14]
                            fieldPositionXOld = rs[15]
                            fieldPositionYOld = rs[16]
                            fieldPosition2XOld = rs[17]
                            fieldPosition2YOld = rs[18]
                            if (playOrder != playOrderOld or
                                    typeId != typeIdOld or
                                    text != textOld or
                                    shortText != shortTextOld or
                                    period != periodOld or
                                    clockValue != clockValueOld or
                                    clockDisplayValue != clockDisplayValueOld or
                                    teamDisplayName != teamDisplayNameOld or
                                    teamId != teamIdOld or
                                    sourceId != sourceIdOld or
                                    scoringPlay != scoringPlayOld or
                                    shootout != shootoutOld or
                                    wallclock != wallclockOld or
                                    goalPositionX != goalPositionXOld or
                                    goalPositionY != goalPositionYOld or
                                    fieldPositionX != fieldPositionXOld or
                                    fieldPositionY != fieldPositionYOld or
                                    fieldPosition2X != fieldPosition2XOld or
                                    fieldPosition2Y != fieldPosition2YOld):
                                #sql2 = f"""UPDATE {tablename}
                                #            SET playOrder = ?,
                                #                typeId = ?,
                                #                text = ?,
                                #                shortText = ?,
                                #                period = ?,
                                #                clockValue = ?,
                                #                clockDisplayValue = ?,
                                #                teamDisplayName = ?,
                                #                teamId = ?,
                                #                sourceId = ?,
                                #                scoringPlay = ?,
                                #                shootout = ?,
                                #                wallclock = ?,
                                #                goalPositionX = ?,
                                #                goalPositionY = ?,
                                #                fieldPositionX = ?,
                                #                fieldPositionY = ?,
                                #                fieldPosition2X = ?,
                                #                fieldPosition2Y = ?,
                                #                updateId = ?
                                #            WHERE eventId = ? and playId = ?;
                                #        """
                                # print(sql2)
                                val = (playOrder,
                                                typeId,
                                                text,
                                                shortText,
                                                period,
                                                clockValue,
                                                clockDisplayValue,
                                                teamDisplayName,
                                                teamId,
                                                sourceId,
                                                scoringPlay,
                                                shootout,
                                                wallclock,
                                                goalPositionX,
                                                goalPositionY,
                                                fieldPositionX,
                                                fieldPositionY,
                                                fieldPosition2X,
                                                fieldPosition2Y,
                                                updateId,
                                                eventId,
                                                playId)
                                # print(val)
                                # print(sql2)
                                # print(1, playOrder, playOrderOld, playOrder != playOrderOld)
                                # print(3, typeId, typeIdOld, typeId != typeIdOld)
                                # print(4, text, textOld, text != textOld)
                                # print(5, shortText, shortTextOld,
                                #       shortText == "", shortTextOld == "", shortText != shortTextOld)
                                # print(6, period, periodOld, period != periodOld)
                                # print(7, clockValue, clockValueOld, clockValue != clockValueOld)
                                # print(8, clockDisplayValue, clockDisplayValueOld,
                                #        clockDisplayValue != clockDisplayValueOld)
                                # print(9, teamDisplayName, teamDisplayNameOld,
                                #       teamDisplayName == None, teamDisplayNameOld == None,
                                #       teamDisplayName != teamDisplayNameOld)
                                # print(10, teamId, teamIdOld, teamId != teamIdOld)
                                # print(11, sourceId, sourceIdOld, sourceId != sourceIdOld)
                                # print(12, scoringPlay, scoringPlayOld, scoringPlay != scoringPlayOld)
                                # print(13, shootout, shootoutOld, shootout != shootoutOld)
                                # print(14, wallclock, wallclockOld, wallclock != wallclockOld)
                                # print(15, goalPositionX, goalPositionXOld, goalPositionX != goalPositionXOld)
                                # print(16, goalPositionY, goalPositionYOld, goalPositionY != goalPositionYOld)
                                # print(17, fieldPositionX, fieldPositionXOld, fieldPositionX != fieldPositionXOld)
                                # print(18, fieldPositionY, fieldPositionYOld, fieldPositionY != fieldPositionYOld)
                                # print(19, fieldPosition2X, fieldPosition2XOld, fieldPosition2X != fieldPosition2XOld)
                                # print(20, fieldPosition2Y, fieldPosition2YOld, fieldPosition2Y != fieldPosition2YOld)
                                # print(i, "update", n, nEvents, eventId, playId, playOrder, teamId,
                                #       teamDisplayName, shortText, updateId)
                                # print(i, "update", eventId, playId, teamId, teamDisplayName, shortText, updateId)
                                nUpdate += 1
                                cursor.execute(sql2,val)
                                # print(tuple(row))
                                # print(sql)
                            else:
                                # print(i, "skip", eventId, playId, teamId, teamDisplayName, shortText, updateId)
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            nInsert += 1
                            # print(i, "insert", n, nEvents, eventId, playId, playOrder, teamId,
                            #      teamDisplayName, shortText, updateId)
                            cursor.execute(sql3, tuple(row))
                        if int((i + 1) / 1000) * 1000 == (i + 1) or i + 1 == nTotalCol:
                            print(tablename, eventId, n, "out of", nEvents, "Processsed", i + 1, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename,'eventId=',eventId, 'transaction rolled back')
                    # print(tuple(row))
                    msg = tablename + " " +str(eventId) + " update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                else:
                    # print(tablename, eventId,n,'out of', nEvents,'record inserted successfully')
                    conn.commit()
                    msg = tablename + " " +str(eventId) + " update update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def keyEventsCreateTableSQL(dbcur, tablename):
    # tablename = "KeyEvents"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            keyEventOrder INT NOT NULL,
            keyEventId INT,
            typeId INT,
            period INT,
            clockValue INT,
            clockDisplayValue VARCHAR(128),
            scoringPlay BIT,
            sourceId INT,
            shootout BIT,
            keyEventText VARCHAR(255),
            keyEventShortText VARCHAR(255),
            teamId INT,
            teamDisplayName VARCHAR(128),
            goalPositionX FLOAT,
            goalPositionY FLOAT,
            fieldPositionX FLOAT,
            fieldPositionY FLOAT,
            fieldPosition2X FLOAT,
            fieldPosition2Y FLOAT,
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def keyEventsInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "KeyEvents"
    (bExist, msg) = keyEventsCreateTableSQL(cursor, tablename)
    # print(msg)
    errMessages = []
    updateId = df_records_full.iloc[0]['updateId']
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    nSuccess = 0
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    bInsert = True
    if osStr == "Windows":
        sql1 = ("SELECT keyEventOrder,"
                        "typeId,"
                        "period,"
                        "clockValue,"
                        "clockDisplayValue,"
                        "scoringPlay,"
                        "sourceId,"
                        "shootout,"
                        "keyEventText,"
                        "keyEventShortText,"
                        "teamId,"
                        "teamDisplayName,"
                        "goalPositionX,"
                        "goalPositionY,"
                        "fieldPositionX,"
                        "fieldPositionY,"
                        "fieldPosition2X,"
                        "fieldPosition2Y"
                " FROM " + tablename +
                " WHERE eventId = ? and keyEventId= ?;")
        sql2 = ("UPDATE " + tablename +
                    " SET keyEventOrder = ?,"
                    "typeId = ?,"
                    "keyEventText = ?,"
                    "keyEventShortText = ?,"
                    "period = ?,"
                    "clockValue = ?,"
                    "clockDisplayValue = ?,"
                    "teamDisplayName = ?,"
                    "teamId = ?,"
                    "sourceId = ?,"
                    "scoringPlay = ?,"
                    "shootout = ?,"
                    "goalPositionX = ?,"
                    "goalPositionY = ?,"
                    "fieldPositionX = ?,"
                    "fieldPositionY = ?,"
                    "fieldPosition2X = ?,"
                    "fieldPosition2Y = ?,"
                    "updateId = ?"
                    " WHERE eventId = ? and keyEventId = ?;")
    else:
        sql1 = ("SELECT keyEventOrder,"
                "typeId,"
                "period,"
                "clockValue,"
                "clockDisplayValue,"
                "scoringPlay,"
                "sourceId,"
                "shootout,"
                "keyEventText,"
                "keyEventShortText,"
                "teamId,"
                "teamDisplayName,"
                "goalPositionX,"
                "goalPositionY,"
                "fieldPositionX,"
                "fieldPositionY,"
                "fieldPosition2X,"
                "fieldPosition2Y"
                " FROM " + tablename +
                " WHERE eventId = %s and keyEventId= %s;")
        sql2 = ("UPDATE " + tablename +
                " SET keyEventOrder = %s,"
                "typeId = %s,"
                "keyEventText = %s,"
                "keyEventShortText = %s,"
                "period = %s,"
                "clockValue = %s,"
                "clockDisplayValue = %s,"
                "teamDisplayName = %s,"
                "teamId = %s,"
                "sourceId = %s,"
                "scoringPlay = %s,"
                "shootout = %s,"
                "goalPositionX = %s,"
                "goalPositionY = %s,"
                "fieldPositionX = %s,"
                "fieldPositionY = %s,"
                "fieldPosition2X = %s,"
                "fieldPosition2Y = %s,"
                "updateId = %s"
                " WHERE eventId = %s and keyEventId = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                n += 1
                try:
                    for i, row in df_records.iterrows():
                        # print(row)
                        eventId = int(row['eventId'])
                        keyEventOrder = int(row['order'])
                        keyEventId = int(row['id'])
                        typeId = int(row['typeId'])
                        period = int(row['period'])
                        clockValue = int(row['clockValue'])
                        clockDisplayValue = row['clockDisplayValue']
                        scoringPlay = bool(row['scoringPlay'])
                        sourceId = int(row['sourceId'])
                        shootout = bool(row['shootout'])
                        keyEventText = row['text']
                        keyEventShortText = row['shortText']
                        teamId = int(row['teamId'])
                        teamDisplayName = row['teamDisplayName']
                        goalPositionX = row['goalPositionX']
                        goalPositionY = row['goalPositionY']
                        fieldPositionX = row['fieldPositionX']
                        fieldPositionY = row['fieldPositionY']
                        fieldPosition2X = row['fieldPosition2X']
                        fieldPosition2Y = row['fieldPosition2Y']
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT keyEventOrder,
                        #                    typeId,
                        #                    period,
                        #                    clockValue,
                        #                    clockDisplayValue,
                        #                    scoringPlay,
                        #                    sourceId,
                        #                    shootout,
                        #                    keyEventText,
                        #                    keyEventShortText,
                        #                    teamId,
                        #                    teamDisplayName,
                        #                    goalPositionX,
                        #                    goalPositionY,
                        #                    fieldPositionX,
                        #                    fieldPositionY,
                        #                    fieldPosition2X,
                        #                    fieldPosition2Y
                        #            FROM {tablename}
                        #           WHERE eventId = {eventId} and keyEventId= {keyEventId};
                        #        """
                        # print(sql1)
                        val = (eventId, keyEventId)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            keyEventOrderOld = rs[0]
                            typeIdOld = rs[1]
                            periodOld = rs[2]
                            clockValueOld = rs[3]
                            clockDisplayValueOld = rs[4]
                            scoringPlayOld = rs[5]
                            sourceIdOld = rs[6]
                            shootoutOld = rs[7]
                            keyEventTextOld = rs[8]
                            if keyEventTextOld == "None":
                                keyEventTextOld = None
                            keyEventShortTextOld = rs[9]
                            if keyEventShortTextOld == "None":
                                keyEventShortTextOld = None
                            teamIdOld = rs[10]
                            teamDisplayNameOld = rs[11]
                            if teamDisplayNameOld == "None":
                                teamDisplayNameOld = None
                            goalPositionXOld = rs[12]
                            goalPositionYOld = rs[13]
                            fieldPositionXOld = rs[14]
                            fieldPositionYOld = rs[15]
                            fieldPosition2XOld = rs[16]
                            fieldPosition2YOld = rs[17]
                            if (keyEventOrder != keyEventOrderOld or
                                    typeId != typeIdOld or
                                    keyEventText != keyEventTextOld or
                                    keyEventShortText != keyEventShortTextOld or
                                    period != periodOld or
                                    clockValue != clockValueOld or
                                    clockDisplayValue != clockDisplayValueOld or
                                    teamDisplayName != teamDisplayNameOld or
                                    teamId != teamIdOld or
                                    sourceId != sourceIdOld or
                                    scoringPlay != scoringPlayOld or
                                    shootout != shootoutOld or
                                    goalPositionX != goalPositionXOld or
                                    goalPositionY != goalPositionYOld or
                                    fieldPositionX != fieldPositionXOld or
                                    fieldPositionY != fieldPositionYOld or
                                    fieldPosition2X != fieldPosition2XOld or
                                    fieldPosition2Y != fieldPosition2YOld):
                                #sql2 = f"""UPDATE {tablename}
                                #            SET keyEventOrder = ?,
                                #            typeId = ?,
                                #            keyEventText = ?,
                                #            keyEventShortText = ?,
                                #            period = ?,
                                #            clockValue = ?,
                                #            clockDisplayValue = ?,
                                #            teamDisplayName = ?,
                                #            teamId = ?,
                                #            sourceId = ?,
                                #            scoringPlay = ?,
                                #            shootout = ?,
                                #            goalPositionX = ?,
                                #            goalPositionY = ?,
                                #            fieldPositionX = ?,
                                #            fieldPositionY = ?,
                                #            fieldPosition2X = ?,
                                #            fieldPosition2Y = ?,
                                #            updateId = ?
                                #            WHERE eventId = ? and keyEventId = ?;
                                #        """
                                # print(sql2)
                                val = (keyEventOrder,
                                        typeId,
                                        keyEventText,
                                        keyEventShortText,
                                        period,
                                        clockValue,
                                        clockDisplayValue,
                                        teamDisplayName,
                                        teamId,
                                        sourceId,
                                        scoringPlay,
                                        shootout,
                                        goalPositionX,
                                        goalPositionY,
                                        fieldPositionX,
                                        fieldPositionY,
                                        fieldPosition2X,
                                        fieldPosition2Y,
                                        updateId,
                                        eventId,
                                        keyEventId
                                        )
                                # print(1, playOrder, playOrderOld, playOrder != playOrderOld)
                                # print(3, typeId, typeIdOld, typeId != typeIdOld)
                                # print(4, text, textOld, text != textOld)
                                # print(5, shortText, shortTextOld,
                                #      shortText == None, shortTextOld == None, shortText != shortTextOld)
                                # print(6, period, periodOld, period != periodOld)
                                # print(7, clockValue, clockValueOld, clockValue != clockValueOld)
                                # print(8, clockDisplayValue, clockDisplayValueOld,
                                #       clockDisplayValue != clockDisplayValueOld)
                                # print(9, teamDisplayName, teamDisplayNameOld,
                                #     teamDisplayName == None, teamDisplayNameOld == None,
                                #      teamDisplayName != teamDisplayNameOld)
                                # print(10, teamId, teamIdOld, teamId != teamIdOld)
                                # print(11, sourceId, sourceIdOld, sourceId != sourceIdOld)
                                # print(12, scoringPlay, scoringPlayOld, scoringPlay != scoringPlayOld)
                                # print(13, shootout, shootoutOld, shootout != shootoutOld)
                                # print(15, goalPositionX, goalPositionXOld, goalPositionX != goalPositionXOld)
                                # print(16, goalPositionY, goalPositionYOld, goalPositionY != goalPositionYOld)
                                # print(17, fieldPositionX, fieldPositionXOld, fieldPositionX != fieldPositionXOld)
                                # print(18, fieldPositionY, fieldPositionYOld, fieldPositionY != fieldPositionYOld)
                                # print(19, fieldPosition2X, fieldPosition2XOld, fieldPosition2X != fieldPosition2XOld)
                                # print(20, fieldPosition2Y, fieldPosition2YOld, fieldPosition2Y != fieldPosition2YOld)
                                nUpdate += 1
                                # print(i, "update", n, nEvents, eventId, keyEventId, keyEventOrder, teamId,
                                #       teamDisplayName, keyEventShortText, updateId)
                                cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                            else:
                                # print(i, "skip", n, nEvents, eventId, keyEventId, keyEventOrder, teamId,
                                #      teamDisplayName, keyEventShortText, updateId)
                                msg = "skip"
                                nSkip += 1

                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            nInsert += 1
                            # print(i, "insert", n, nEvents, eventId, keyEventId, keyEventOrder, teamId,
                            #       teamDisplayName, keyEventShortText, updateId)
                            cursor.execute(sql3, tuple(row))
                        if int((i + 1) / 1000) * 1000 == (i + 1) or i + 1 == nTotalCol:
                            print(tablename, eventId, n, "out of", nEvents, "Processsed", i + 1, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename, 'eventId=', eventId, 'transaction rolled back')
                    # print(tuple(row))
                    msg = tablename + " " + " update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate,'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                    # break
                else:
                    # print(tablename, eventId,n,'out of', nEvents,'record inserted successfully')
                    conn.commit()
                    msg = tablename + " " + str(eventId) + " update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                               'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def commentaryCreateTableSQL(dbcur, tablename):
    # tablename = "Commentary"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            commentaryOrder INT NOT NULL,
            clockValue INT,
            clockDisplayValue VARCHAR(10),
            commentaryText VARCHAR(4096),
            id INT,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,commentaryOrder),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def commentaryInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "Commentary"
    (bExist, msg) = commentaryCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename,'Total records=', nTotalCol)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip =0
    updateId = df_records_full.iloc[0]['updateId']
    nSuccess = 0
    bInsert = True
    if osStr == "Windows":
        sql1 = ("SELECT clockValue,"
                       " clockDisplayValue,"
                       " commentaryText,"
                       " id"
                " FROM " + tablename +
                " WHERE eventId = ? and commentaryOrder= ?;")
        sql2 = ("UPDATE " + tablename +
                   " SET clockValue = ?,"
                        "clockDisplayValue = ?,"
                        "commentaryText = ?,"
                        "id = ?,"
                        "updateId = ?"
                   " WHERE eventId = ? and commentaryOrder = ?;")
        sql4 = ("SELECT commentaryOrder"
                              " FROM " + tablename +
                              " WHERE eventId = ?;")
    else:
        sql1 = ("SELECT clockValue,"
                " clockDisplayValue,"
                " commentaryText,"
                " id"
                " FROM " + tablename +
                " WHERE eventId = %s and commentaryOrder= %s;")
        sql2 = ("UPDATE " + tablename +
                " SET clockValue = %s,"
                "clockDisplayValue = %s,"
                "commentaryText = %s,"
                "id = %s,"
                "updateId = %s"
                " WHERE eventId = %s and commentaryOrder = %s;")
        sql4 = ("SELECT commentaryOrder"
                " FROM " + tablename +
                " WHERE eventId = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                orderList = []
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                n += 1
                try:
                    for i, row in df_records.iterrows():
                        # print(row)
                        eventId = int(row['eventId'])
                        commentaryOrder = int(row['order'])
                        orderList.append(commentaryOrder)
                        clockValue = int(row['clockValue'])
                        clockDisplayValue = row['clockDisplayValue']
                        commentaryId = int(row['id'])
                        commentaryText = row['text']
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT clockValue,
                        #                    clockDisplayValue,
                        #                    commentaryText,
                        #                    id
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and commentaryOrder= {commentaryOrder};
                        #        """
                        # print(sql1)
                        val = (eventId, commentaryOrder)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            clockValueOld = rs[0]
                            clockDisplayValueOld = rs[1]
                            commentaryTextOld = rs[2]
                            if commentaryTextOld == "None":
                                commentaryTextOld = None
                            commentaryIdOld = rs[3]
                            if (clockValue != clockValueOld or
                                    clockDisplayValue != clockDisplayValueOld or
                                    commentaryText != commentaryTextOld or
                                    commentaryId != commentaryIdOld):
                                #sql2 = f"""UPDATE {tablename}
                                #             SET clockValue = ?,
                                #                clockDisplayValue = ?,
                                #                commentaryText = ?,
                                #                id = ?,
                                #                updateId = ?
                                #            WHERE eventId = ? and commentaryOrder = ?;
                                #        """
                                # print(sql2)
                                val = (clockValue, clockDisplayValue, commentaryText,commentaryId,updateId,
                                       eventId, commentaryOrder)
                                # print(1, playOrder, playOrderOld, playOrder != playOrderOld)
                                # print(3, typeId, typeIdOld, typeId != typeIdOld)
                                # print(4, text, textOld, text != textOld)
                                # print(5, shortText, shortTextOld,
                                #      shortText == None, shortTextOld == None, shortText != shortTextOld)
                                # print(6, period, periodOld, period != periodOld)
                                # print(7, clockValue, clockValueOld, clockValue != clockValueOld)
                                # print(8, clockDisplayValue, clockDisplayValueOld,
                                #       clockDisplayValue != clockDisplayValueOld)
                                # print(9, teamDisplayName, teamDisplayNameOld,
                                #     teamDisplayName == None, teamDisplayNameOld == None,
                                #      teamDisplayName != teamDisplayNameOld)
                                # print(10, teamId, teamIdOld, teamId != teamIdOld)
                                # print(11, sourceId, sourceIdOld, sourceId != sourceIdOld)
                                # print(12, scoringPlay, scoringPlayOld, scoringPlay != scoringPlayOld)
                                # print(13, shootout, shootoutOld, shootout != shootoutOld)
                                # print(15, goalPositionX, goalPositionXOld, goalPositionX != goalPositionXOld)
                                # print(16, goalPositionY, goalPositionYOld, goalPositionY != goalPositionYOld)
                                # print(17, fieldPositionX, fieldPositionXOld, fieldPositionX != fieldPositionXOld)
                                # print(18, fieldPositionY, fieldPositionYOld, fieldPositionY != fieldPositionYOld)
                                # print(19, fieldPosition2X, fieldPosition2XOld, fieldPosition2X != fieldPosition2XOld)
                                # print(20, fieldPosition2Y, fieldPosition2YOld, fieldPosition2Y != fieldPosition2YOld)
                                # print(i, "update", n, nEvents, eventId, playId, playOrder, teamId,
                                #       teamDisplayName, shortText, updateId)
                                # print(i, "update", eventId, playId, teamId, teamDisplayName, shortText, updateId)
                                nUpdate += 1
                                cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                            else:
                                # print(i, "skip", eventId, playId, teamId, teamDisplayName, shortText, updateId)
                                msg = "skip"
                                nSkip += 1

                        else:
                            #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(i, "insert", n, nEvents, eventId, commentaryOrder,commentaryText,updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                        if int((i + 1) / 1000) * 1000 == (i + 1) or i + 1 == nTotalCol:
                            print(tablename, eventId, n, "out of", nEvents, "Processsed", i + 1, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                    #sql4 = f"""SELECT commentaryOrder
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId};
                    #        """
                    val = (eventId,)
                    cursor.execute(sql4,val)
                    rs = cursor.fetchall()
                    deleteOrderList = []
                    for rs_row in rs:
                        nOrder = rs_row[0]
                        if nOrder not in orderList:
                            deleteOrderList.append(nOrder)
                    if len(deleteOrderList) > 0:
                        # print(deleteOrderList)
                        for nOrder in deleteOrderList:
                            sql5 = f""" DELETE 
                                        FROM {tablename}
                                        where eventId = {eventId} and commentaryOrder = {nOrder}
                                    """
                            cursor.execute(sql5)
                            print("delete in db", eventId, nOrder)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename, 'eventId=', eventId, 'transaction rolled back')
                    # print(tuple(row))
                    msg = tablename + " " + str(eventId) + " update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                else:
                    # print(tablename, eventId,n,'out of', nEvents,'record inserted successfully')
                    conn.commit()
                    msg = tablename + " " + str(eventId) + " update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def athletesCreateTableSQL(dbcur, tablename):
    # tablename = "Atheletes"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            uid VARCHAR(128),
            guid VARCHAR(128),
            lastName VARCHAR(256),
            fullName VARCHAR(256),
            displayName VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def athletesInsertRecordSQL(osStr,conn, cursor, tablename1, tablename2, df_records):
    # tablename1 = "Atheletes"
    # tablename2 = "PlayerDB"
    (bExist1, msg1) = athletesCreateTableSQL(cursor, tablename1)
    # print(msg1)
    (bExist2, msg2) = playerDBCreateTableSQL(cursor, tablename2)
    # print(msg2)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename1,tablename2,"Total records=", nTotal)
    # print(len(df_records.index))
    # print(df_records.shape)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    defaultTimeZone = tz.gettz("UTC")
    defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT uid,"
                        "guid,"
                        "lastName,"
                        "fullName,"
                        "displayName,"
                        "updateTime"
                " FROM " + tablename1 +
                " WHERE id = ?;")
        sql2 = ("UPDATE " + tablename1 +
                   " SET uid = ?,"
                        "guid = ?,"
                        "lastName = ?,"
                        "fullName = ?,"
                        "displayName = ?,"
                        "updateTime = ?,"
                        "updateId = ?"
                    " WHERE id = ?;")
        sql3 = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (nCol - 1) + "?)"
        sql4 = "INSERT INTO " + tablename2 + " VALUES (" + "?," * 37 + "?)"
        #sql5 = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT uid,"
                "guid,"
                "lastName,"
                "fullName,"
                "displayName,"
                "updateTime"
                " FROM " + tablename1 +
                " WHERE id = %s;")
        sql2 = ("UPDATE " + tablename1 +
                " SET uid = %s,"
                "guid = %s,"
                "lastName = %s,"
                "fullName = %s,"
                "displayName = %s,"
                "updateTime = %s,"
                "updateId = %s"
                " WHERE id = %s;")
        sql3 = "INSERT INTO " + tablename1 + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
        sql4 = "INSERT INTO " + tablename2 + " VALUES (" + "%s," * 37 + "%s)"
        #sql5 = "INSERT INTO " + tablename1 + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist1:
        cursor.execute(f"SELECT * FROM {tablename1} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename1
            bInsert = False
            try:
                n=0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    playerId = row['id']
                    uid = row['uid']
                    guid = row['guid']
                    lastName = row['lastName']
                    fullName = row['fullName']
                    displayName = row['displayName']
                    updateTime = row['updateTime']
                    # updateTime = datetime.strptime(updateTimeStr, '%Y-%m-%dT%H:%M:%SZ')
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT uid,
                    #                    guid,
                    #                    lastName,
                    #                    fullName,
                    #                    displayName,
                    #                    updateTime
                    #            FROM {tablename1}
                    #            WHERE id = {playerId};
                    #        """
                    # print(sql1)
                    val = (playerId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        uidOld = rs[0]
                        guidOld = rs[1]
                        lastNameOld = rs[2]
                        fullNameOld = rs[3]
                        displayNameOld = rs[4]
                        updateTimeOld = rs[5]
                        if updateTimeOld is not None:
                            updateTimeOld = updateTimeOld.replace(tzinfo=tz.gettz("UTC"))
                        else:
                            updateTimeOld = defaultTime
                        # updateTimeOld = datetime.strptime(updateTimeStrOld, '%Y-%m-%dT%H:%M:%SZ')
                        if (uid != uidOld or
                                guid != guidOld or
                                lastName != lastNameOld or
                                fullName != fullNameOld or
                                displayName != displayNameOld) and (updateTime > updateTimeOld):
                            # print(1, uid != uidOld)
                            # print(2, guid != guidOld,guid,guidOld)
                            # print(3, lastName != lastNameOld)
                            # print(4, fullName != fullNameOld)
                            # print(5, displayName != displayNameOld)
                            #sql2 = f"""UPDATE {tablename1}
                            #            SET uid = ?,
                            #                guid = ?,
                            #                lastName = ?,
                            #                fullName = ?,
                            #                displayName = ?,
                            #                updateTime = ?,
                            #                updateId = ?
                            #            WHERE id = ?;
                            #        """
                            # print(sql2)
                            val = (uid,
                                    guid,
                                    lastName,
                                    fullName,
                                    displayName,
                                    updateTime,
                                    updateId,
                                   playerId)
                            # print(i, "update", playerId, fullName, displayName, updateId)
                            nUpdate += 1
                            # print(tuple(row))
                            cursor.execute(sql2,val)
                        else:
                            # print(i, "skip", playerId, fullName, displayName, updateId)
                            # print(i, tuple(row))
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", playerId, fullName, displayName, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename1, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename1, 'transaction rolled back')
                msg = tablename1 + " update error:" + str(e)
                # print(sql1)
                # print(sql2)
                # print(val)
                # print(tuple(row))
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename1,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename1, 'record inserted successfully')
                conn.commit()
                msg = tablename1 + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename1,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
        if bInsert:  # insert rows into Table
            cursor.execute(f"SELECT id FROM {tablename2};")
            rs = cursor.fetchall()
            athleteIdList = []
            for row in rs:
                athleteIdList.append(row[0])
            # print(athleteIdList)
            k = 0
            try:
                for i, row in df_records.iterrows():
                    k += 1
                    tmpAthleteId = tuple(row)[0]
                    tmpAthleteUid = tuple(row)[1]
                    tmpAthleteGuid = tuple(row)[2]
                    tmpAthleteLastName = tuple(row)[3]
                    tmpAthleteFullName = tuple(row)[4]
                    tmpAthleteDisplayName = tuple(row)[5]
                    tmpUpdateId = tuple(row)[6]
                    if tmpAthleteId not in athleteIdList:
                        # print(tuple(row))
                        #sql2 = "INSERT INTO " + tablename2 + " VALUES (" + "?," * 37 + "?)"
                        cursor.execute(sql4, tuple([tmpAthleteId, tmpAthleteUid, tmpAthleteGuid, "", tmpAthleteLastName,
                                                    tmpAthleteFullName, tmpAthleteDisplayName,
                                                    "", None, "", None, "", None, None, "", "", "",
                                                    None, "", "", None, "", "", "", "", "", "",
                                                    "", None, "", "", "", "", "", "", "", "",
                                                    tmpUpdateId]))
                    #sql = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                    # print(sql)
                    nInsert += 1
                    cursor.execute(sql3, tuple(row))
                    if int(k / 1000) * 1000 == k or k == nTotal:
                        print(tablename1, "Processsed", k, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename1, 'transaction rolled back')
                msg = tablename1 + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename1,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename1, 'record inserted successfully')
                conn.commit()
                msg = tablename1 + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename1,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playerDBCreateTableSQL(dbcur, tablename):
    # tablename = "PlayerDB"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            uid VARCHAR(128),
            guid VARCHAR(128),
            firstName VARCHAR(128),
            lastName VARCHAR(128),
            fullName VARCHAR(128),
            displayName VARCHAR(128),
            shortName VARCHAR(128),
            weight DOUBLE(10,2),
            displayWeight VARCHAR(128),
            height DOUBLE(10,2),
            displayHeight VARCHAR(128),
            age INT,
            dateOfBirth VARCHAR(100),
            gender  VARCHAR(10),
            citizenship VARCHAR(128),
            slug VARCHAR(128),
            jersey INT,
            status VARCHAR(128),
            profiled BIT,
            timestamp TIMESTAMP,
            birthPlaceCountry VARCHAR(128),
            citizenshipCountryAlternateId  INT,
            citizenshipCountryAbbreviation  VARCHAR(128),
            birthCountryAlternateId INT,
            birthCountryAbbreviation VARCHAR(128),
            flag_href VARCHAR(256),
            flag_alt VARCHAR(128),
            positionId INT,
            positionName  VARCHAR(128),
            positionDisplayName VARCHAR(128),
            positionAbbreviation VARCHAR(128),
            middleName VARCHAR(128),
            headshot_href VARCHAR(128),
            headshot_alt  VARCHAR(128),
            birthPlaceCity VARCHAR(128),
            nickname  VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playerDBInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "PlayerDB"
    (bExist, msg) = playerDBCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    tablename1 = "Athletes"
    playerIdList = []
    cursor.execute(f"SELECT id FROM {tablename1};")
    rs = cursor.fetchall()
    for row in rs:
        playerIdList.append(row[0])
    # print("Player Id list length:", len(playerIdList))
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql_athletes = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 7 + "?)"
        sql1 = ("SELECT uid,"
                   "    guid,"
                   "    firstName,"
                   "    middleName,"
                   "    lastName,"
                   "    fullName,"
                   "    displayName,"
                   "    shortName,"
                   "    nickname,"
                   "    weight,"
                   "    displayWeight,"
                   "    height,"
                   "    displayHeight,"
                   "    age,"
                   "    dateOfBirth,"
                   "    gender,"
                   "    citizenship,"
                   "    slug,"
                   "    jersey,"
                   "    status,"
                   "    profiled,"
                   "    timestamp,"
                   "    birthPlaceCity,"
                   "    birthPlaceCountry,"
                   "    birthCountryAlternateId,"
                   "    birthCountryAbbreviation,"
                   "    citizenshipCountryAlternateId,"
                   "    citizenshipCountryAbbreviation,"
                   "    flag_href,"
                   "    flag_alt,"
                   "    positionId,"
                   "    positionName,"
                   "    positionDisplayName,"
                   "    positionAbbreviation,"
                   "    headshot_href,"
                   "    headshot_alt"
                   " FROM " + tablename +
                   " WHERE id = ?;")
        sql2 = (
                   "UPDATE PlayerDB" 
                   " SET " 
                       "uid=?,"
                       "guid=?,"
                       "firstName = ?,"
                       "middleName  = ?,"
                       "lastName = ?,"
                       "fullName = ?,"
                       "displayName = ?,"
                       "shortName = ?,"
                       "nickname  = ?,"
                       "weight = ?,"
                       "displayWeight = ?,"
                       "height = ?,"
                       "displayHeight = ?,"
                       "age = ?,"
                       "dateOfBirth = ?,"
                       "gender = ?,"
                       "citizenship = ?,"
                       "slug = ?,"
                       "jersey = ?,"
                       "status = ?,"
                       "profiled = ?,"
                       "timestamp = ?,"
                       "birthPlaceCity = ?,"
                       "birthPlaceCountry = ?,"
                       "birthCountryAlternateId = ?,"
                       "birthCountryAbbreviation = ?,"
                       "citizenshipCountryAlternateId = ?,"
                       "citizenshipCountryAbbreviation = ?,"
                       "flag_href = ?,"
                       "flag_alt = ?,"
                       "positionId = ?,"
                       "positionName  = ?,"
                       "positionDisplayName  = ?,"
                       "positionAbbreviation  = ?,"
                       "headshot_href = ?,"
                       "headshot_alt = ?,"
                       "updateId = ?"
                   " WHERE id = ?;"
        )
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql_athletes = "INSERT INTO " + tablename1 + " VALUES (" + "%s," * 7 + "%s)"
        sql1 = ("SELECT uid,"
                "    guid,"
                "    firstName,"
                "    middleName,"
                "    lastName,"
                "    fullName,"
                "    displayName,"
                "    shortName,"
                "    nickname,"
                "    weight,"
                "    displayWeight,"
                "    height,"
                "    displayHeight,"
                "    age,"
                "    dateOfBirth,"
                "    gender,"
                "    citizenship,"
                "    slug,"
                "    jersey,"
                "    status,"
                "    profiled,"
                "    timestamp,"
                "    birthPlaceCity,"
                "    birthPlaceCountry,"
                "    birthCountryAlternateId,"
                "    birthCountryAbbreviation,"
                "    citizenshipCountryAlternateId,"
                "    citizenshipCountryAbbreviation,"
                "    flag_href,"
                "    flag_alt,"
                "    positionId,"
                "    positionName,"
                "    positionDisplayName,"
                "    positionAbbreviation,"
                "    headshot_href,"
                "    headshot_alt"
                " FROM " + tablename +
                " WHERE id = %s;")
        sql2 = (
            "UPDATE PlayerDB"
            " SET "
            "uid=%s,"
            "guid=%s,"
            "firstName = %s,"
            "middleName  = %s,"
            "lastName = %s,"
            "fullName = %s,"
            "displayName = %s,"
            "shortName = %s,"
            "nickname  = %s,"
            "weight = %s,"
            "displayWeight = %s,"
            "height = %s,"
            "displayHeight = %s,"
            "age = %s,"
            "dateOfBirth = %s,"
            "gender = %s,"
            "citizenship = %s,"
            "slug = %s,"
            "jersey = %s,"
            "status = %s,"
            "profiled = %s,"
            "timestamp = %s,"
            "birthPlaceCity = %s,"
            "birthPlaceCountry = %s,"
            "birthCountryAlternateId = %s,"
            "birthCountryAbbreviation = %s,"
            "citizenshipCountryAlternateId = %s,"
            "citizenshipCountryAbbreviation = %s,"
            "flag_href = %s,"
            "flag_alt = %s,"
            "positionId = %s,"
            "positionName  = %s,"
            "positionDisplayName  = %s,"
            "positionAbbreviation  = %s,"
            "headshot_href = %s,"
            "headshot_alt = %s,"
            "updateId = %s"
            " WHERE id = %s;"
        )
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            # print(len(df_records.index))
            # print(df_records.shape)
            try:
                # print(tablename,"Total records=", nTotal)
                n=0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    playerId = int(row['id'])
                    uid = row['uid']
                    guid = row['guid']
                    firstName = row['firstName']
                    middleName = row['middleName']
                    lastName = row['lastName']
                    fullName = row['fullName']
                    displayName = row['displayName']
                    shortName = row['shortName']
                    nickname = row['nickname']
                    weight = row['weight']
                    displayWeight = row['displayWeight']
                    height = row['height']
                    displayHeight = row['displayHeight']
                    #if displayHeight[-1] == '"':
                    #    displayHeightStr = displayHeight + '"'
                    # elif displayHeight == 'None':
                    #     displayHeightStr = None
                    #else:
                    #    displayHeightStr = displayHeight
                    age = int(row['age'])
                    dateOfBirth = row['dateOfBirth']
                    gender = row['gender']
                    citizenship = row['citizenship']
                    slug = row['slug']
                    jersey = int(row['jersey'])
                    status = row['status']
                    profiled = row['profiled']
                    timestamp = row['timestamp']
                    birthPlaceCity = row['birthPlace.city']
                    birthPlaceCountry = row['birthPlace.country']
                    birthCountryAltId = int(row['birthCountry.alternateId'])
                    birthCountryAbbr = row['birthCountry.abbreviation']
                    citizenshipCountryAltId = int(row['citizenshipCountry.alternateId'])
                    citizenshipCountryAbbr = row['citizenshipCountry.abbreviation']
                    flagHref = row['flag.href']
                    flagAlt = row['flag.alt']
                    positionId = int(row['position.id'])
                    positionName = row['position.name']
                    positionDisplayName = row['position.displayName']
                    positionAbbr = row['position.abbreviation']
                    headshotHref = row['headshot.href']
                    headshotAlt = row['headshot.alt']
                    updateId = row['updateId']
                    if playerId not in playerIdList:
                        print("insert", tablename1)
                        #sql_athletes = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 7 + "?)"
                        # print(sql_athletes)
                        cursor.execute(sql_athletes, tuple([playerId, "", "", lastName,fullName,
                                                            displayName, timestamp, updateId]))
                        playerIdList.append(playerId)
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT uid,
                    #                    guid,
                    #                    firstName,
                    #                    middleName,
                    #                    lastName,
                    #                    fullName,
                    #                    displayName,
                    #                    shortName,
                    #                    nickname,
                    #                    weight,
                    #                    displayWeight,
                    #                    height,
                    #                    displayHeight,
                    #                    age,
                    #                    dateOfBirth,
                    #                    gender,
                    #                    citizenship,
                    #                    slug,
                    #                    jersey,
                    #                    status,
                    #                    profiled,
                    #                    timestamp,
                    #                    birthPlaceCity,
                    #                    birthPlaceCountry,
                    #                    birthCountryAlternateId,
                    #                    birthCountryAbbreviation,
                    #                    citizenshipCountryAlternateId,
                    #                    citizenshipCountryAbbreviation,
                    #                    flag_href,
                    #                    flag_alt,
                    #                    positionId,
                    #                    positionName,
                    #                    positionDisplayName,
                    #                    positionAbbreviation,
                    #                    headshot_href,
                    #                    headshot_alt
                    #            FROM {tablename}
                    #            WHERE id = {playerId};
                    #        """
                    # print(sql1)
                    val = (playerId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        rs = cursor.fetchone()
                        # print(rs)
                        uidOld = rs[0]
                        guidOld = rs[1]
                        firstNameOld = rs[2]
                        middleNameOld = rs[3]
                        lastNameOld = rs[4]
                        fullNameOld = rs[5]
                        displayNameOld = rs[6]
                        shortNameOld = rs[7]
                        nicknameOld = rs[8]
                        weightOld = rs[9]
                        displayWeightOld = rs[10]
                        heightOld = rs[11]
                        # print("height",rs[11])
                        displayHeightOld = rs[12]
                        # print("displayHeight",rs[12])
                        # print("age",rs[13])
                        ageOld = rs[13]
                        dateOfBirthOld = rs[14]
                        genderOld = rs[15]
                        citizenshipOld = rs[16]
                        slugOld = rs[17]
                        jerseyOld = rs[18]
                        statusOld = rs[19]
                        profiledOld = rs[20]
                        timestampOld = rs[21]
                        if timestampOld is not None:
                            timestampOld = timestampOld.replace(tzinfo=tz.gettz("UTC"))
                        birthPlaceCityOld = rs[22]
                        birthPlaceCountryOld = rs[23]
                        birthCountryAltIdOld = rs[24]
                        # print('birthCountryId',birthCountryAltIdOld)
                        birthCountryAbbrOld = rs[25]
                        citizenshipCountryAltIdOld = rs[26]
                        # print('citizenshipCountryId',citizenshipCountryAltIdOld)
                        citizenshipCountryAbbrOld = rs[27]
                        flagHrefOld = rs[28]
                        flagAltOld = rs[29]
                        positionIdOld = rs[30]
                        positionNameOld = rs[31]
                        positionDisplayNameOld = rs[32]
                        positionAbbrOld = rs[33]
                        headshotHrefOld = rs[34]
                        headshotAltOld = rs[35]
                        # print(tuple(row))
                        # print(rs)
                        """
                        print(1, uid != uidOld)
                        print(2, guid != guidOld)
                        print(3, firstName != firstNameOld)
                        print(4, lastName != lastNameOld)
                        print(5, fullName != fullNameOld)
                        print(6, displayName != displayNameOld)
                        print(7, shortName != shortNameOld)
                        print(8, weight != weightOld)
                        print(9, displayWeight != displayWeightOld)
                        print(10, height != heightOld)
                        print(11, displayHeight != displayHeightOld)
                        print(12, age != ageOld)
                        print(13, dateOfBirth != dateOfBirthOld)
                        print(14, gender != genderOld)
                        print(15, citizenship != citizenshipOld)
                        print(15, citizenship,citizenshipOld)
                        print(16, slug != slugOld)
                        print(17, jersey, jerseyOld, jersey != jerseyOld)
                        print(18, status != statusOld)
                        print(19, profiled != profiledOld)
                        print(20, timestamp != timestampOld)
                        print(21, birthPlaceCountry != birthPlaceCountryOld)
                        print(22, citizenshipCountryAltId != citizenshipCountryAltIdOld)
                        print(22, citizenshipCountryAltId, citizenshipCountryAltIdOld)
                        print(23, citizenshipCountryAbbr != citizenshipCountryAbbrOld)
                        print(24, birthCountryAltId != birthCountryAltIdOld)
                        print(24, birthCountryAltId, birthCountryAltIdOld)
                        print(25, birthCountryAbbr != birthCountryAbbrOld)
                        print(26, flagHref != flagHrefOld)
                        print(27, flagAlt != flagAltOld)
                        print(28, positionId != positionIdOld)
                        print(29, positionName != positionNameOld)
                        print(30, positionDisplayName != positionDisplayNameOld)
                        print(31, positionAbbr != positionAbbrOld)
                        print(32, middleName != middleNameOld)
                        print(33, headshotHref != headshotHrefOld)
                        print(34, headshotAlt != headshotAltOld)
                        print(35, birthPlaceCity != birthPlaceCityOld)
                        print(36, nickname != nicknameOld)
                        """
                        if (uid != uidOld or
                                guid != guidOld or
                                firstName != firstNameOld or
                                lastName != lastNameOld or
                                fullName != fullNameOld or
                                displayName != displayNameOld or
                                shortName != shortNameOld or
                                weight != weightOld or
                                displayWeight != displayWeightOld or
                                height != heightOld or
                                displayHeight != displayHeightOld or
                                age != ageOld or
                                dateOfBirth != dateOfBirthOld or
                                gender != genderOld or
                                citizenship != citizenshipOld or
                                slug != slugOld or
                                jersey != jerseyOld or
                                status != statusOld or
                                profiled != profiledOld or
                                birthPlaceCountry != birthPlaceCountryOld or
                                str(citizenshipCountryAltId) != citizenshipCountryAltIdOld or
                                citizenshipCountryAbbr != citizenshipCountryAbbrOld or
                                str(birthCountryAltId) != birthCountryAltIdOld or
                                birthCountryAbbr != birthCountryAbbrOld or
                                flagHref != flagHrefOld or
                                flagAlt != flagAltOld or
                                positionId != positionIdOld or
                                positionName != positionNameOld or
                                positionDisplayName != positionDisplayNameOld or
                                positionAbbr != positionAbbrOld or
                                middleName != middleNameOld or
                                headshotHref != headshotHrefOld or
                                headshotAlt != headshotAltOld or
                                birthPlaceCity != birthPlaceCityOld or
                                nickname != nicknameOld):
                            """
                            print(1, uid != uidOld)
                            print(2, guid != guidOld)
                            print(3, firstName != firstNameOld)
                            print(4, lastName != lastNameOld)
                            print(5, fullName != fullNameOld)
                            print(6, displayName != displayNameOld)
                            print(7, shortName != shortNameOld)
                            print(8, weight != weightOld)
                            print(9, displayWeight != displayWeightOld)
                            print(10, height != heightOld)
                            print(11, displayHeight != displayHeightOld)
                            print(12, age != ageOld)
                            print(13, dateOfBirth != dateOfBirthOld)
                            print(14, gender != genderOld)
                            print(15, citizenship != citizenshipOld)
                            print(15, citizenship,citizenshipOld)
                            print(16, slug != slugOld)
                            print(17, jersey, jerseyOld, jersey != jerseyOld)
                            print(18, status != statusOld)
                            print(19, profiled != profiledOld)
                            print(20, timestamp != timestampOld)
                            print(21, birthPlaceCountry != birthPlaceCountryOld)
                            print(22, citizenshipCountryAltId != citizenshipCountryAltIdOld)
                            print(22, citizenshipCountryAltId, citizenshipCountryAltIdOld)
                            print(23, citizenshipCountryAbbr != citizenshipCountryAbbrOld)
                            print(24, birthCountryAltId != birthCountryAltIdOld)
                            print(24, birthCountryAltId, birthCountryAltIdOld)
                            print(25, birthCountryAbbr != birthCountryAbbrOld)
                            print(26, flagHref != flagHrefOld)
                            print(27, flagAlt != flagAltOld)
                            print(28, positionId != positionIdOld)
                            print(29, positionName != positionNameOld)
                            print(30, positionDisplayName != positionDisplayNameOld)
                            print(31, positionAbbr != positionAbbrOld)
                            print(32, middleName != middleNameOld)
                            print(33, headshotHref != headshotHrefOld)
                            print(34, headshotAlt != headshotAltOld)
                            print(35, birthPlaceCity != birthPlaceCityOld)
                            print(36, nickname != nicknameOld)
                            """
                            #sql2 = f"""
                            #            UPDATE PlayerDB
                            #            SET
                            #                uid=?,
                            #                guid=?,
                            #                firstName = ?,
                            #                middleName  = ?,
                            #                lastName = ?,
                            #                fullName = ?,
                            #                displayName = ?,
                            #                shortName = ?,
                            #                nickname  = ?,
                            #                weight = ?,
                            #                displayWeight = ?,
                            #                height = ?,
                            #                displayHeight = ?,
                            #                age = ?,
                            #                dateOfBirth = ?,
                            #                gender = ?,
                            #                citizenship = ?,
                            #                slug = ?,
                            #                jersey = ?,
                            #                status = ?,
                            #                profiled = ?,
                            #                timestamp = ?,
                            #                birthPlaceCity = ?,
                            #                birthPlaceCountry = ?,
                            #                birthCountryAlternateId = ?,
                            #                birthCountryAbbreviation = ?,
                            #                citizenshipCountryAlternateId = ?,
                            #                citizenshipCountryAbbreviation = ?,
                            #                flag_href = ?,
                            #                flag_alt = ?,
                            #                positionId = ?,
                            #                positionName  = ?,
                            #                positionDisplayName  = ?,
                            #                positionAbbreviation  = ?,
                            #                headshot_href = ?,
                            #                headshot_alt = ?,
                            #                updateId = ?
                            #            WHERE id = ?;
                            #           """
                            update_tuple = (uid,
                                            guid,
                                            firstName,
                                            middleName,
                                            lastName,
                                            fullName,
                                            displayName,
                                            shortName,
                                            nickname,
                                            weight,
                                            displayWeight,
                                            height,
                                            displayHeight,
                                            age,
                                            dateOfBirth,
                                            gender,
                                            citizenship,
                                            slug,
                                            jersey,
                                            status,
                                            profiled,
                                            timestamp,
                                            birthPlaceCity,
                                            birthPlaceCountry,
                                            birthCountryAltId,
                                            birthCountryAbbr,
                                            citizenshipCountryAltId,
                                            citizenshipCountryAbbr,
                                            flagHref,
                                            flagAlt,
                                            positionId,
                                            positionName,
                                            positionDisplayName,
                                            positionAbbr,
                                            headshotHref,
                                            headshotAlt,
                                            updateId,
                                            playerId)
                            # print(sql2)
                            # print(update_tuple)
                            # print(n, "update", playerId, fullName, displayHeight,
                            #      timestamp, timestampOld, timestamp == timestampOld, gender, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,update_tuple)
                        else:
                            # print(i, "skip", playerId, fullName,displayName, timestamp)
                            # print(i, tuple(row))
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(n, "insert", playerId, fullName, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playParticipantsCreateTableSQL(dbcur, tablename):
    # tablename = "PlayParticipants"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            playId INT NOT NULL,
            playOrder INT,
            participant VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,playId,playOrder),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(playId) REFERENCES Plays(playId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playParticipantsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "playParticipants"
    (bExist, msg) = playParticipantsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    # print(len(df_records.index))
    # print(df_records.shape)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    defaultTimeZone = tz.gettz("UTC")
    defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT participant"
                 " FROM " + tablename +
                 " WHERE eventId = ? and playId = ? and playOrder = ?;")
        sql2 = ("UPDATE " + tablename +
                  " SET participant = ?,"
                      " updateId = ?"
                  " WHERE eventId = ? and playId = ? and playOrder = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT participant"
                " FROM " + tablename +
                " WHERE eventId = %s and playId = %s and playOrder = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET participant = %s,"
                " updateId = %s"
                " WHERE eventId = %s and playId = %s and playOrder = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            # tablename1 = "Plays"
            # playIdList = []
            # rs = cursor.execute(f"SELECT playId FROM {tablename1};").fetchall()
            # for row in rs:
            #     playIdList.append(row[0])
            # print("play Id list length:", len(playIdList))
            # nOrder = 9000
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    eventId = int(row['eventId'])
                    playId = int(row['id'])
                    playOrder = int(row['order'])
                    participant = row['participant']
                    updateId = int(row['updateId'])
                    # if playId not in playIdList:
                    #     nOrder += 1
                    #     tmpRow = tuple([eventId,nOrder,playId,0,
                    #                        "","",0,0,"","",0,0,"","",defaultTime,0,0,0,0,0,0,updateId ])
                    #     sql_plays = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (len(tmpRow) - 1) + "?)"
                    #     cursor.execute(sql_plays, tmpRow)
                    #     playIdList.append(playId)
                    #     print(n, "insert plays", eventId, playId, participant)
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT participant
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId} and playId = {playId} and playOrder = {playOrder};
                    #        """
                    # print(sql1)
                    val = (eventId, playId, playOrder)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        participantOld = rs[0]
                        if (participant != participantOld):
                            # print(1, participant != participantOld)
                            # sql2 = f"""UPDATE {tablename}
                            #            SET participant = ?,
                            #                updateId = ?
                            #            WHERE eventId = ? and playId = ? and playOrder = ?;
                            #        """
                            # print(sql2)
                            val = (participant,updateId, eventId, playId, playOrder)
                            # print(i, "update", eventId, playId, playOrder,participant,updateId)
                            # print(tuple(row))
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        else:
                            # print(i, "skip", eventId, playId, playOrder, participant, updateId)
                            # print(i, tuple(row))
                            nSkip += 1
                            msg = "skip"
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", eventId, playId, playOrder, participant, updateId)
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def keyEventParticipantsCreateTableSQL(dbcur, tablename):
    # tablename = "KeyEventParticipants"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            keyEventId INT NOT NULL,
            keyEventOrder INT,
            participant VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,keyEventId,keyEventOrder),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            FOREIGN KEY(keyEventId) REFERENCES KeyEvents(keyEventId),
            FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def keyEventParticipantsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "KeyEventParticipants"
    (bExist, msg) = keyEventParticipantsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    # print(len(df_records.index))
    # print(df_records.shape)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    tablename1 = "KeyEvents"
    keyEventIdList = []
    cursor.execute(f"SELECT keyEventId FROM {tablename1};")
    rs = cursor.fetchall()
    for row in rs:
        keyEventIdList.append(row[0])
    # print("keyEvent Id list length:", len(keyEventIdList))
    nOrder = 9000
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT participant"
                 " FROM " + tablename +
                 " WHERE eventId = ?" 
                 "  and keyEventId = ?"
                 "  and keyEventOrder = ?;")
        sql2 = ("UPDATE " + tablename +
                  " SET participant = ?,"
                       " updateId = ?"
                    " WHERE eventId = ? and "
                    " keyEventId = ? and "
                    " keyEventOrder = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT participant"
                " FROM " + tablename +
                " WHERE eventId = %s"
                "  and keyEventId = %s"
                "  and keyEventOrder = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET participant = %s,"
                " updateId = %s"
                " WHERE eventId = %s and "
                " keyEventId = %s and "
                " keyEventOrder = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                # print(tablename,"Total records=", nTotal)
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    eventId = row['eventId']
                    keyEventId = row['id']
                    keyEventOrder = row['order']
                    participant = row['participant']
                    updateId = row['updateId']
                    # print(i, "from df", id, stat, name, updateId)
                    # if keyEventId not in keyEventIdList:
                    #     nOrder += 1
                    #    tmpRow = tuple([eventId,nOrder,keyEventId,0,
                    #                    0,0,"","",0,"","","",0,"",0,0,0,0,0,0,updateId])
                    #     sql_plays = "INSERT INTO " + tablename1 + " VALUES  (" + "?," * (len(tmpRow) - 1) + "?)"
                    #     cursor.execute(sql_plays, tmpRow)
                    #    keyEventIdList.append(keyEventId)
                    #     print(n, "insert plays", eventId, keyEventId, participant)
                    # sql1 = f"""SELECT participant
                    #            FROM {tablename}
                    #            WHERE eventId = {eventId}
                    #            and keyEventId = {keyEventId}
                    #            and keyEventOrder = {keyEventOrder};
                    #        """
                    val = (eventId, keyEventId, keyEventOrder)
                    # print(sql1)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        participantOld = rs[0]
                        if (participant != participantOld):
                            # print(1, participant != participantOld)
                            #sql2 = f"""UPDATE {tablename}
                            #            SET participant = \"{participant}\",
                            #                updateId = {updateId}
                            #            WHERE eventId = {eventId} and
                            #            keyEventId = {keyEventId} and
                            #            keyEventOrder = {keyEventOrder};
                            #        """
                            val = (participant, updateId, eventId, keyEventId, keyEventOrder)
                            # print(sql2)
                            # print(i, "update", eventId, keyEventId, keyEventOrder, participant, updateId)
                            # print(tuple(row))
                            nUpdate += 1
                            cursor.execute(sql2, val)
                        else:
                            # print(i, "skip", eventId, keyEventId, keyEventOrder, participant, updateId)
                            # print(i, tuple(row))
                            msg = "skip"
                            nSkip += 1

                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, keyEventId, keyEventOrder, participant, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                # print(sql2)
                # print(val)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playerPlaysCreateTableSQL(dbcur, tablename):
    # tablename = "PlayerPlays"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            homeAway VARCHAR(10),
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            hasPlays BIT,
            clockDisplayValue VARCHAR(128),
            clockValue INT,
            playOrder INT,
            scoringPlay BIT,
            substitution BIT,
            redCard BIT,
            yellowCard BIT,
            penaltyKick BIT,
            ownGoal BIT,
            didScore BIT,
            updateTime TIMESTAMP,
            didAssist BIT,
            updateId INT NOT NULL,
            PRIMARY KEY (eventID, teamID, playOrder),
            CONSTRAINT fk_PlayerPlays_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_PlayerPlays_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_PlayerPlays_id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_PlayerPlays_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playerPlaysInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "PlayerPlays"
    (bExist, msg) = playerPlaysCreateTableSQL(cursor, tablename)
    # print(msg)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records_full.iloc[0]['updateId']
    nSuccess = 0
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    # print(len(df_records_full.index))
    # print(df_records_full.shape)
    if osStr == "Windows":
        sql1 = ("SELECT homeAway, athleteId, athleteDisplayName," 
                           " hasPlays,"
                           " clockDisplayValue, clockValue,"
                           " scoringPlay, substitution, redCard, yellowCard, penaltyKick, ownGoal,"
                           " didScore, didAssist"
                    " FROM " + tablename +
                    " WHERE eventId = ? and teamId = ? and playOrder = ?")
        sql2 = ("UPDATE " + tablename +
                    " SET homeAway = ?,"
                    "athleteId = ?,"
                    "athleteDisplayName = ?,"
                    "hasPlays = ?,"
                    "clockDisplayValue = ?,"
                    "clockValue = ?,"
                    "scoringPlay = ?,"
                    "substitution = ?,"
                    "redCard = ?,"
                    "yellowCard = ?,"
                    "penaltyKick = ?,"
                    "ownGoal = ?,"
                    "didScore = ?,"
                    "updateTime = ?,"
                    "didAssist = ?,"
                    "updateId = ?"
                    " WHERE eventId = ? and "
                           " teamId = ? and "
                           " playOrder = ?;")
        sql4 = ("SELECT playOrder"
                  " FROM " + tablename +
                  " WHERE eventId = ? and teamId = ?;")
        sql5 = (" DELETE"
                   " FROM " + tablename +
                   " WHERE eventId = ? and teamId = ? and playOrder = ?;")
    else:
        sql1 = ("SELECT homeAway, athleteId, athleteDisplayName," 
                           " hasPlays,"
                           " clockDisplayValue, clockValue,"
                           " scoringPlay, substitution, redCard, yellowCard, penaltyKick, ownGoal,"
                           " didScore, didAssist"
                    " FROM " + tablename +
                    " WHERE eventId = %s and teamId = %s and playOrder = %s")
        sql2 = ("UPDATE " + tablename +
                    " SET homeAway = %s,"
                    "athleteId = %s,"
                    "athleteDisplayName = %s,"
                    "hasPlays = %s,"
                    "clockDisplayValue = %s,"
                    "clockValue = %s,"
                    "scoringPlay = %s,"
                    "substitution = %s,"
                    "redCard = %s,"
                    "yellowCard = %s,"
                    "penaltyKick = %s,"
                    "ownGoal = %s,"
                    "didScore = %s,"
                    "updateTime = %s,"
                    "didAssist = %s,"
                    "updateId = %s"
                    " WHERE eventId = %s and "
                           " teamId = %s and "
                           " playOrder = %s;")
        sql4 = ("SELECT playOrder"
                  " FROM " + tablename +
                  " WHERE eventId = %s and teamId = %s;")
        sql5 = (" DELETE"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s and playOrder = %s;")
    bInsert = True
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                orderList = []
                teamList = []
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                try:
                    # print(tablename,"Total records=", nTotal)
                    for i, row in df_records.iterrows():
                        n += 1
                        # print(n,i,tuple(row))
                        eventId = int(row['eventId'])
                        teamId = int(row['teamId'])
                        if teamId not in teamList:
                            teamList.append(teamId)
                        homeAway = row['homeAway']
                        athleteId = int(row['athleteId'])
                        athleteDisplayName = row['athleteDisplayName']
                        hasPlays = bool(row['hasPlays'])
                        clockDisplayValue = row['clockDisplayValue']
                        clockValue = int(row['clockValue'])
                        playOrder = int(row['order'])
                        orderList.append(playOrder)
                        scoringPlay = bool(row['scoringPlay'])
                        substitution = bool(row['substitution'])
                        redCard = bool(row['redCard'])
                        yellowCard = bool(row['yellowCard'])
                        penaltyKick = bool(row['penaltyKick'])
                        ownGoal = bool(row['ownGoal'])
                        didScore = bool(row['didScore'])
                        updateTime = row['updateTime']
                        didAssist = bool(row['didAssist'])
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT homeAway, athleteId, athleteDisplayName,
                        #                    hasPlays,
                        #                    clockDisplayValue, clockValue,
                        #                    scoringPlay, substitution, redCard, yellowCard, penaltyKick, ownGoal,
                        #                    didScore, didAssist
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId} and playOrder = {playOrder}
                        #        """
                        # print(sql1)
                        val = (eventId, teamId, playOrder)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            homeAwayOld = rs[0]
                            athleteIdOld = rs[1]
                            athleteDisplayNameOld = rs[2]
                            hasPlaysOld = rs[3]
                            clockDisplayValueOld = rs[4]
                            clockValueOld = rs[5]
                            scoringPlayOld = rs[6]
                            substituionOld = rs[7]
                            redCardOld = rs[8]
                            yellowCardOld = rs[9]
                            penaltyKickOld = rs[10]
                            ownGoalOld = rs[11]
                            didScoreOld = rs[12]
                            didAssistOld = rs[13]
                            if (homeAway != homeAwayOld or
                                athleteId != athleteIdOld or
                                athleteDisplayName != athleteDisplayNameOld or
                                hasPlays != hasPlaysOld or
                                clockDisplayValue != clockDisplayValueOld or
                                clockValue != clockValueOld or
                                scoringPlay != scoringPlayOld or
                                substitution != substituionOld or
                                redCard != redCardOld or
                                yellowCard != yellowCardOld or
                                penaltyKick != penaltyKickOld or
                                ownGoal != ownGoalOld or
                                didScore != didScoreOld or
                                didAssist != didAssistOld):
                                # print(homeAway != homeAwayOld)
                                # print(athleteId != athleteIdOld)
                                # print(athleteDisplayName != athleteDisplayNameOld)
                                # print(1, hasPlays != hasPlaysOld)
                                # print(2, clockDisplayValue != clockDisplayValueOld)
                                # print(3, clockValue != clockValueOld)
                                # print(4, scoringPlay != scoringPlayOld)
                                # print(5, substitution != substituionOld)
                                # print(6, redCard != redCardOld)
                                # print(7, yellowCard != yellowCardOld)
                                # print(8, penaltyKick != penaltyKickOld)
                                # print(9, ownGoal != ownGoalOld)
                                # print(10, didScore != didScoreOld)
                                # print(11, didAssist != didAssistOld)
                                #sql2 = f"""UPDATE {tablename}
                                #            SET homeAway = ?,
                                #            athleteId = ?,
                                #            athleteDisplayName = ?,
                                #            hasPlays = ?,
                                #            clockDisplayValue = ?,
                                #            clockValue = ?,
                                #            scoringPlay = ?,
                                #            substitution = ?,
                                #            redCard = ?,
                                #            yellowCard = ?,
                                #            penaltyKick = ?,
                                #            ownGoal = ?,
                                #            didScore = ?,
                                #            updateTime = ?,
                                #            didAssist = ?,
                                #            updateId = ?
                                #            WHERE eventId = ? and
                                #                    teamId = ? and
                                #                    playOrder = ?;
                                #        """
                                # print(sql2)
                                val = (homeAway,
                                        athleteId,
                                        athleteDisplayName,
                                        hasPlays,
                                        clockDisplayValue,
                                        clockValue,
                                        scoringPlay,
                                        substitution,
                                        redCard,
                                        yellowCard,
                                        penaltyKick,
                                        ownGoal,
                                        didScore,
                                        updateTime,
                                        didAssist,
                                        updateId,
                                        eventId,
                                        teamId,
                                        playOrder
                                        )
                                # print(n, "update", eventId, teamId,playOrder, athleteId, athleteDisplayName, updateId)
                                # print(tuple(row))
                                nUpdate += 1
                                cursor.execute(sql2,val)
                            else:
                                # print(n, "skip", eventId, teamId,playOrder, athleteId, athleteDisplayName, updateId)
                                #
                                # print(i, tuple(row))
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(n, "insert", eventId, teamId,playOrder, athleteId, athleteDisplayName, updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                        if int(n / 1000) * 1000 == n or n == nTotalCol:
                            print(tablename, "Processsed", n, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                    for teamId in teamList:
                        #sql4 = f"""SELECT playOrder
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId};
                        #        """
                        val = (eventId, teamId)
                        cursor.execute(sql4,val)
                        rs = cursor.fetchall()
                        deleteOrderList = []
                        for rs_row in rs:
                            nOrder = rs_row[0]
                            if nOrder not in orderList:
                                deleteOrderList.append(nOrder)
                        if len(deleteOrderList) > 0:
                            # print(deleteOrderList)
                            for nOrder in deleteOrderList:
                                #sql5 = f""" DELETE
                                #            FROM {tablename}
                                #            where eventId = {eventId} and teamId = {teamId} and playOrder = {nOrder}
                                #        """
                                val = (eventId, teamId, nOrder)
                                cursor.execute(sql5, val)
                                print("delete in db", eventId, teamId, nOrder)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename, eventId, 'transaction rolled back')
                    msg = tablename + " " + str(eventId)+" update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                else:
                    conn.commit()
                    # print(tablename, 'record inserted successfully')
                    msg = tablename + " " + str(eventId)+" update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def playerStatsCreateTableSQL(dbcur, tablename):
    # tablename = "PlayStats"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            appearances INT,
            foulsCommitted INT,
            foulsSuffered  INT,
            ownGoals INT,
            redCards INT,
            subIns INT,
            yellowCards INT,
            goalAssists INT,
            shotsOnTarget INT,
            totalGoals INT,
            totalShots INT,
            goalsConceded INT,
            saves INT,
            shotsFaced INT,
            hasStats BIT,
            updateTime TIMESTAMP,
            offsides INT, 
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId, athleteId),
            CONSTRAINT fk_PlayStats_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_PlayStats_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_PlayStats_Id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_PlayStats_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playerStatsInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "PlayStats"
    (bExist, msg) = playerStatsCreateTableSQL(cursor, tablename)
    # print(msg)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records_full.iloc[0]['updateId']
    nSuccess = 0
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    # print(len(df_records_full.index))
    # print(df_records_full.shape)
    bInsert = True
    if osStr == "Windows":
        sql1 = ("SELECT athleteDisplayName," 
                      "  appearances, foulsCommitted, foulsSuffered, ownGoals,"
                      "  redCards, subIns, yellowCards, goalAssists," 
                      "  shotsOnTarget, totalGoals, totalShots,"
                      "  goalsConceded, saves, shotsFaced, hasStats,"
                      "  offsides"
                " FROM " + tablename +
                " WHERE eventId = ? and teamId = ? and athleteId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET athleteDisplayName = ?,"
                   " appearances = ?,"
                   " foulsCommitted = ?,"
                   " foulsSuffered = ?,"
                   " ownGoals = ?,"
                   " redCards = ?,"
                   " subIns = ?,"
                   " yellowCards = ?,"
                   " goalAssists = ?,"
                   " shotsOnTarget = ?,"
                   " totalGoals = ?,"
                   " totalShots = ?,"
                   " goalsConceded = ?,"
                   " saves = ?,"
                   " shotsFaced = ?,"
                   " hasStats = ?,"
                   " updateTime = ?,"
                   " offsides = ?,"
                   " updateId = ?"
                " WHERE eventId = ? and "
                      "  teamId = ? and " 
                      "  athleteId = ?;")
        sql4 = ("SELECT athleteId"
                  " FROM " + tablename +
                  " WHERE eventId = ? and teamId = ?;")
    else:
        sql1 = ("SELECT athleteDisplayName,"
                "  appearances, foulsCommitted, foulsSuffered, ownGoals,"
                "  redCards, subIns, yellowCards, goalAssists,"
                "  shotsOnTarget, totalGoals, totalShots,"
                "  goalsConceded, saves, shotsFaced, hasStats,"
                "  offsides"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s and athleteId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET athleteDisplayName = %s,"
                " appearances = %s,"
                " foulsCommitted = %s,"
                " foulsSuffered = %s,"
                " ownGoals = %s,"
                " redCards = %s,"
                " subIns = %s,"
                " yellowCards = %s,"
                " goalAssists = %s,"
                " shotsOnTarget = %s,"
                " totalGoals = %s,"
                " totalShots = %s,"
                " goalsConceded = %s,"
                " saves = %s,"
                " shotsFaced = %s,"
                " hasStats = %s,"
                " updateTime = %s,"
                " offsides = %s,"
                " updateId = %s"
                " WHERE eventId = %s and "
                "  teamId = %s and "
                "  athleteId = %s;")
        sql4 = ("SELECT athleteId"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                athleteList = []
                teamList = []
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                try:
                    # print(tablename,"Total records=", nTotal)
                    for i, row in df_records.iterrows():
                        n += 1
                        # print(n,i,tuple(row))
                        eventId = int(row['eventId'])
                        teamId = int(row['teamId'])
                        if teamId not in teamList:
                            teamList.append(teamId)
                        athleteId = int(row['athleteId'])
                        if athleteId not in athleteList:
                            athleteList.append(athleteId)
                        athleteDisplayName = row['athleteDisplayName']
                        appearances = int(row['appearances'])
                        foulsCommitted = int(row['foulsCommitted'])
                        foulsSuffered = int(row['foulsSuffered'])
                        ownGoals = int(row['ownGoals'])
                        redCards = int(row['redCards'])
                        subIns = int(row['subIns'])
                        yellowCards = int(row['yellowCards'])
                        goalAssists = int(row['goalAssists'])
                        shotsOnTarget = int(row['shotsOnTarget'])
                        totalGoals = int(row['totalGoals'])
                        totalShots = int(row['totalShots'])
                        goalsConceded = int(row['goalsConceded'])
                        saves = int(row['saves'])
                        shotsFaced = int(row['shotsFaced'])
                        hasStats = bool(row['hasStats'])
                        updateTime = row['updateTime']
                        offsides = int(row['offsides'])
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT athleteDisplayName,
                        #                    appearances, foulsCommitted, foulsSuffered, ownGoals,
                        #                    redCards, subIns, yellowCards, goalAssists,
                        #                    shotsOnTarget, totalGoals, totalShots,
                        #                    goalsConceded, saves, shotsFaced, hasStats,
                        #                    offsides
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId} and athleteId = {athleteId}
                        #        """
                        # print(sql1)
                        val = (eventId, teamId, athleteId)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            athleteDisplayNameOld = rs[0]
                            appearancesOld = rs[1]
                            foulsCommittedOld = rs[2]
                            foulsSufferedOld = rs[3]
                            ownGoalsOld = rs[4]
                            redCardsOld = rs[5]
                            subInsOld = rs[6]
                            yellowCardsOld = rs[7]
                            goalAssistsOld = rs[8]
                            shotsOnTargetOld = rs[9]
                            totalGoalsOld = rs[10]
                            totalShotsOld = rs[11]
                            goalsConcededOld = rs[12]
                            savesOld = rs[13]
                            shotsFacedOld = rs[14]
                            hasStatsOld = rs[15]
                            offsidesOld = rs[16]
                            if(athleteDisplayName != athleteDisplayNameOld or
                                appearances != appearancesOld or
                                foulsCommitted != foulsCommittedOld or
                                foulsSuffered != foulsSufferedOld or
                                ownGoals != ownGoalsOld or
                                redCards != redCardsOld or
                                subIns != subInsOld or
                                yellowCards != yellowCardsOld or
                                goalAssists != goalAssistsOld or
                                shotsOnTarget != shotsOnTargetOld or
                                totalGoals != totalGoalsOld or
                                totalShots != totalShotsOld or
                                goalsConceded != goalsConcededOld or
                                saves != savesOld or
                                shotsFaced != shotsFacedOld or
                                hasStats != hasStatsOld or
                                offsides != offsidesOld):
                                #sql2 = f"""UPDATE {tablename}
                                #            SET athleteDisplayName = ?,
                                #                appearances = ?,
                                #                foulsCommitted = ?,
                                #                foulsSuffered = ?,
                                #                ownGoals = ?,
                                #                redCards = ?,
                                #                subIns = ?,
                                #                yellowCards = ?,
                                #                goalAssists = ?,
                                #                shotsOnTarget = ?,
                                #                totalGoals = ?,
                                #                totalShots = ?,
                                #                goalsConceded = ?,
                                #                saves = ?,
                                #                shotsFaced = ?,
                                #                hasStats = ?,
                                #                updateTime = ?,
                                #                offsides = ?,
                                #                updateId = ?
                                #            WHERE eventId = ? and
                                #                    teamId = ? and
                                #                    athleteId = ?;
                                #        """
                                # print(sql2)
                                val = (athleteDisplayName,
                                        appearances,
                                        foulsCommitted,
                                        foulsSuffered,
                                        ownGoals,
                                        redCards,
                                        subIns,
                                        yellowCards,
                                        goalAssists,
                                        shotsOnTarget,
                                        totalGoals,
                                        totalShots,
                                        goalsConceded,
                                        saves,
                                        shotsFaced,
                                        hasStats,
                                        updateTime,
                                        offsides,
                                        updateId,
                                        eventId,
                                        teamId,
                                        athleteId
                                        )
                                nUpdate += 1
                                # print(n, "update", eventId, teamId, athleteId, athleteDisplayName, updateId)
                                # print(tuple(row))
                                cursor.execute(sql2,val)
                            else:
                                # print(i, "skip", eventId, teamId, athleteId, athleteDisplayName, updateId)
                                #
                                # print(i, tuple(row))
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            nInsert += 1
                            # print(n, "insert", eventId, teamId, athleteId, athleteDisplayName, updateId)
                            cursor.execute(sql3, tuple(row))
                        if int(n / 1000) * 1000 == n or n == nTotalCol:
                            print(tablename, "Processsed", n, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                    for teamId in teamList:
                        #sql4 = f"""SELECT athleteId
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId};
                        #        """
                        val = (eventId, teamId)
                        cursor.execute(sql4, val)
                        rs = cursor.fetchall()
                        deleteStatsList = []
                        for rs_row in rs:
                            athleteId = rs_row[0]
                            if athleteId not in athleteList:
                                deleteStatsList.append(athleteId)
                        if len(deleteStatsList) > 0:
                            # print(deleteOrderList)
                            for athleteId in deleteStatsList:
                                sql5 = f""" DELETE 
                                            FROM {tablename}
                                            where eventId = {eventId} and teamid={teamId} and athleteId = {athleteId}
                                        """
                                cursor.execute(sql5)
                                print("delete in db", eventId, athleteId)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename, eventId, 'transaction rolled back')
                    msg = tablename + " " + str(eventId) + " update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert,'nSkip':nSkip, 'nTotal':nTotalCol})
                    nSuccess = 0
                else:
                    conn.commit()
                    # print(tablename, 'record inserted successfully')
                    msg = tablename + " " + str(eventId) + " update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                               'nUpdate': nUpdate, 'nInsert': nInsert,'nSkip':nSkip, 'nTotal':nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def teamRosterCreateTableSQL(dbcur, tablename):
    # tablename = "TeamRoster"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULLea
            active BIT,
            starter BIT,
            jersey INT,
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            position VARCHAR(128),
            subbedIn BIT,
            subbedOut BIT,
            formationPlace INT,
            hasStats BIT,
            hasPlays BIT,
            updateTime TIMESTAMP,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId, athleteId),
            CONSTRAINT fk_TeamRoster_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_TeamRoster_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_TeamRoster_Id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_TeamRoster_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playerInTeamCreateTableSQL(dbcur, tablename):
    # tablename = "playerInTeam"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            athleteId INT NOT NULL,
            teamId INT NOT NULL,
            seasonType INT NOT NULL,
            seasonYear INT,
            seasonName VARCHAR(256),
            league VARCHAR(256),
            teamName VARCHAR(256),
            playerIndex INT,
            playerDisplayName VARCHAR(256),
            jersey INT,
            positionId INT,
            hasStats BIT,
            timestamp TIMESTAMP,
            updateId INT NOT NULL,
            PRIMARY KEY(athleteId, teamId, seasonType,updateId),
            CONSTRAINT fk_PlayerInTeam_athleteId FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_PlayerInTeam_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_PlayerInTeam_seasonType FOREIGN KEY(seasonType) REFERENCES SeasonType(typeId),
            CONSTRAINT fk_PlayerInTeam_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)


def playerInTeamInsertRecordSQL(osStr,conn, cursor, tablename, df_records,seasonType,teamId):
    # tablename = "playerInTeam"
    (bExist, msg) = playerInTeamCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "total records=",nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    msg = "update " + tablename
    bInsert = False
    try:
        n = 0
        for i, row in df_records.iterrows():
            n += 1
            # print(row)
            nInsert += 1
            cursor.execute(sql3, tuple(row))
            if int(n / 1000) * 1000 == n or n == nTotal:
                print(seasonType, teamId, tablename, "Processsed", n, "out of", nTotal,
                      "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
    except Exception as e:
        conn.rollback()
        print(e)
        print(tablename, 'transaction rolled back')
        # print(tuple(row))
        msg = str(seasonType) + "," + str(teamId)+ " " + tablename + " update error:" + str(e)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    else:
        print(tablename, 'record inserted successfully')
        conn.commit()
        msg = str(seasonType) + "," + str(teamId) + " " + tablename + " update complete"
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)


def teamRosterInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "TeamRoster"
    (bExist, msg) = teamRosterCreateTableSQL(cursor, tablename)
    # print(msg)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records_full.iloc[0]['updateId']
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    #print(len(df_records_full.index))
    #print(df_records_full.shape)
    nSuccess = 0
    bInsert = True
    if osStr == "Windows":
        sql1 = ("SELECT uniformType, uniformColor, homeAway, winner, formation,"
                        "active, starter, jersey, athleteDisplayName, position, formationPlace,"
                        "subbedIn,"
                        "subbedInForAthleteId,subbedInForAthleteJersey,"
                        "subbedInClockValue,subbedInDisplayClock,"
                        "subbedOut,"
                        "subbedOutForAthleteId,subbedOutForAthleteJersey,"
                        "subbedOutClockValue,subbedOutDisplayClock,"
                        "hasStats,hasPlays"
                " FROM " + tablename +
                " WHERE eventId = ? and teamId = ? and athleteId = ?;")
        sql2 = ("UPDATE " + tablename +
                      " SET uniformType = ?,"
                          " uniformColor = ?,"
                          " homeAway = ?,"
                          " winner = ?,"
                          " formation = ?,"
                          " active = ?,"
                          " starter = ?,"
                          " jersey = ?,"
                          " athleteDisplayName = ?,"
                          " position = ?,"
                          " formationPlace = ?,"
                          " subbedIn = ?,"
                          " subbedInForAthleteId = ?,"
                          " subbedInForAthleteJersey = ?,"
                          " subbedInClockValue = ?,"
                          " subbedInDisplayClock = ?,"
                          " subbedOut = ?,"
                          " subbedOutForAthleteId = ?,"
                          " subbedOutForAthleteJersey = ?,"
                          " subbedOutClockValue = ?,"
                          " subbedOutDisplayClock = ?,"
                          " hasStats = ?,"
                          " hasPlays = ?,"
                          " updateTime = ?,"
                          " updateId = ?"
                      " WHERE eventId = ? and teamId = ? and athleteId =?;")
        sql4 = ("SELECT athleteId"
                   " FROM " + tablename +
                   " WHERE eventId = ? and teamId = ?;")
        sql5 = ("DELETE"
                " FROM " + tablename +
                " WHERE eventId = ? and teamId= ? and athleteId = ?;")
    else:
        sql1 = ("SELECT uniformType, uniformColor, homeAway, winner, formation,"
                "active, starter, jersey, athleteDisplayName, position, formationPlace,"
                "subbedIn,"
                "subbedInForAthleteId,subbedInForAthleteJersey,"
                "subbedInClockValue,subbedInDisplayClock,"
                "subbedOut,"
                "subbedOutForAthleteId,subbedOutForAthleteJersey,"
                "subbedOutClockValue,subbedOutDisplayClock,"
                "hasStats,hasPlays"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s and athleteId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET uniformType = %s,"
                " uniformColor = %s,"
                " homeAway = %s,"
                " winner = %s,"
                " formation = %s,"
                " active = %s,"
                " starter = %s,"
                " jersey = %s,"
                " athleteDisplayName = %s,"
                " position = %s,"
                " formationPlace = %s,"
                " subbedIn = %s,"
                " subbedInForAthleteId = %s,"
                " subbedInForAthleteJersey = %s,"
                " subbedInClockValue = %s,"
                " subbedInDisplayClock = %s,"
                " subbedOut = %s,"
                " subbedOutForAthleteId = %s,"
                " subbedOutForAthleteJersey = %s,"
                " subbedOutClockValue = %s,"
                " subbedOutDisplayClock = %s,"
                " hasStats = %s,"
                " hasPlays = %s,"
                " updateTime = %s,"
                " updateId = %s"
                " WHERE eventId = %s and teamId = %s and athleteId =%s;")
        sql4 = ("SELECT athleteId"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId = %s;")
        sql5 = ("DELETE"
                " FROM " + tablename +
                " WHERE eventId = %s and teamId= %s and athleteId = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                athleteList = []
                teamList = []
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                try:
                    # print(tablename,"Total records=", nTotal)
                    for i, row in df_records.iterrows():
                        n += 1
                        # print(n,i,tuple(row))
                        eventId = int(row['eventId'])
                        teamId = int(row['teamId'])
                        if teamId not in teamList:
                            teamList.append(teamId)
                        uniformType = row['uniformType']
                        uniformColor = row['uniformColor']
                        homeAway = row['homeAway']
                        winner = bool(row['winner'])
                        formation = row['formation']
                        active = bool(row['active'])
                        starter = bool(row['starter'])
                        jersey = int(row['jersey'])
                        athleteId = int(row['athleteId'])
                        if athleteId not in athleteList:
                            athleteList.append(athleteId)
                        athleteDisplayName = row['athleteDisplayName']
                        position = row['position']
                        formationPlace = int(row['formationPlace'])
                        subbedIn = bool(row['subbedIn'])
                        subbedInForAthleteId = int(row['subbedInForAthleteId'])
                        subbedInForAthleteJersey = int(row['subbedInForAthleteJersey'])
                        subbedInClockValue = int(row['subbedInClockValue'])
                        subbedInDisplayClock = row['subbedInDisplayClock']
                        subbedOut = bool(row['subbedOut'])
                        subbedOutForAthleteId = int(row['subbedOutForAthleteId'])
                        subbedOutForAthleteJersey = int(row['subbedOutForAthleteJersey'])
                        subbedOutClockValue = int(row['subbedOutClockValue'])
                        subbedOutDisplayClock = row['subbedOutDisplayClock']
                        hasStats = bool(row['hasStats'])
                        hasPlays = bool(row['hasPlays'])
                        updateTime = row['updateTime']
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT active,starter, jersey, athleteDisplayName,
                        #                    position,subbedIn,subbedOut,formationPlace,hasStats,hasPlays
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId} and athleteId = {athleteId}
                        #        """
                        # print(sql1)
                        val = (eventId, teamId, athleteId)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            uniformTypeOld = rs[0]
                            uniformColorOld = rs[1]
                            homeAwayOld = rs[2]
                            winnerOld = rs[3]
                            formationOld = rs[4]
                            activeOld = rs[5]
                            starterOld = rs[6]
                            jerseyOld = rs[7]
                            athleteDisplayNameOld = rs[8]
                            positionOld = rs[9]
                            formationPlaceOld = rs[10]
                            subbedInOld = rs[11]
                            subbedInForAthleteIdOld = rs[12]
                            subbedInForAthleteJerseyOld = rs[13]
                            subbedInClockValueOld = rs[14]
                            subbedInDisplayClockOld = rs[15]
                            subbedOutOld = rs[16]
                            subbedOutForAthleteIdOld = rs[17]
                            subbedOutForAthleteJerseyOld = rs[18]
                            subbedOutClockValueOld = rs[19]
                            subbedOutDisplayClockOld = rs[20]
                            hasStatsOld = rs[21]
                            hasPlaysOld = rs[22]
                            if (uniformType != uniformTypeOld or
                                    uniformColor != uniformColorOld or
                                    homeAway != homeAwayOld or
                                    winner != winnerOld or
                                    formation != formationOld or
                                    active != activeOld or
                                    starter != starterOld or
                                    jersey != jerseyOld or
                                    athleteDisplayName != athleteDisplayNameOld or
                                    position != positionOld or
                                    formationPlace != formationPlaceOld or
                                    subbedIn != subbedInOld or
                                    subbedInForAthleteId != subbedInForAthleteIdOld or
                                    subbedInForAthleteJersey != subbedInForAthleteJerseyOld or
                                    subbedInClockValue != subbedInClockValueOld or
                                    subbedInDisplayClock != subbedInDisplayClockOld or
                                    subbedOut != subbedOutOld or
                                    subbedOutForAthleteId != subbedOutForAthleteIdOld or
                                    subbedOutForAthleteJersey != subbedOutForAthleteJerseyOld or
                                    subbedOutClockValue != subbedOutClockValueOld or
                                    subbedOutDisplayClock != subbedOutDisplayClockOld or
                                    hasStats != hasStatsOld or
                                    hasPlays != hasPlaysOld):
                                # print(1, uniformType, uniformTypeOld, uniformType == uniformTypeOld)
                                # print(2, uniformColor, uniformColorOld, uniformColor == uniformColorOld)
                                # print(3, homeAway, homeAwayOld, homeAway == homeAwayOld)
                                # print(4, winner, winnerOld, winner == winnerOld)
                                # print(5, formation, formationOld, formation == formationOld)
                                # print(6, active, activeOld, active == activeOld)
                                # print(7, starter, starterOld, active == activeOld)
                                # print(8, jersey, jerseyOld, jersey == jerseyOld)
                                # print(9, athleteDisplayName, athleteDisplayNameOld,
                                #      athleteDisplayName == athleteDisplayNameOld)
                                # print(10, position, positionOld, position == positionOld)
                                # print(11, formationPlace, formationPlaceOld, formationPlace == formationPlaceOld)
                                # print(12, subbedIn, subbedInOld, subbedIn == subbedInOld)
                                # print(13, subbedInForAthleteId, subbedInForAthleteIdOld,
                                #      subbedInForAthleteId == subbedInForAthleteIdOld)
                                # print(14, subbedInForAthleteJersey, subbedInForAthleteJerseyOld,
                                #      subbedInForAthleteJersey == subbedInForAthleteJerseyOld)
                                # print(15, subbedInClockValue, subbedInClockValueOld,
                                #      subbedInClockValue == subbedInClockValueOld)
                                # print(16, subbedInDisplayClock, subbedInDisplayClockOld,
                                #      subbedInDisplayClock == subbedInDisplayClockOld)
                                # print(17, subbedOut, subbedOutOld, subbedOut == subbedOutOld)
                                # print(18, subbedOutForAthleteId, subbedOutForAthleteIdOld,
                                #      subbedOutForAthleteId == subbedOutForAthleteIdOld)
                                # print(19, subbedOutForAthleteJersey, subbedOutForAthleteJerseyOld,
                                #      subbedOutForAthleteJersey == subbedOutForAthleteJerseyOld)
                                # print(20, subbedOutClockValue, subbedOutClockValueOld,
                                #      subbedOutClockValue == subbedOutClockValueOld)
                                # print(21, subbedOutDisplayClock, subbedOutDisplayClockOld,
                                #      subbedOutDisplayClock == subbedOutDisplayClockOld)
                                # print(22, hasStats, hasStatsOld, hasStats == hasStatsOld)
                                # print(23, hasPlays, hasPlaysOld, hasPlays == hasPlaysOld)
                                #sql2 = f"""UPDATE {tablename}
                                #                SET active = ?,
                                #                    starter = ?,
                                #                    jersey = ?,
                                #                    athleteDisplayName = ?,
                                #                    position = ?,
                                #                    subbedIn = ?,
                                #                    subbedOut = ?,
                                #                    formationPlace = ?,
                                #                    hasStats = ?,
                                #                    hasPlays = ?,
                                #                    updateTime = ?,
                                #                    updateId = ?
                                #                WHERE eventId = ? and teamId = ? and athleteId =?;
                                #        """
                                # print(sql2)
                                val = (uniformType,
                                       uniformColor,
                                       homeAway,
                                       winner,
                                       formation,
                                       active,
                                       starter,
                                       jersey,
                                       athleteDisplayName,
                                       position,
                                       formationPlace,
                                       subbedIn,
                                       subbedInForAthleteId,
                                       subbedInForAthleteJersey,
                                       subbedInClockValue,
                                       subbedInDisplayClock,
                                       subbedOut,
                                       subbedOutForAthleteId,
                                       subbedOutForAthleteJersey,
                                       subbedOutClockValue,
                                       subbedOutDisplayClock,
                                       hasStats,
                                       hasPlays,
                                       updateTime,
                                       updateId,
                                       eventId,
                                       teamId,
                                       athleteId
                                        )
                                # print(n, "update", eventId, teamId, athleteId, athleteDisplayName, updateId)
                                # print(tuple(row))
                                nUpdate += 1
                                cursor.execute(sql2,val)
                            else:
                                # print(n, "skip", eventId, teamId, athleteId, athleteDisplayName, updateId)
                                #
                                # print(i, tuple(row))
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(n, "insert", eventId, teamId, athleteId, athleteDisplayName, updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                        if int(n / 1000) * 1000 == n or n == nTotalCol:
                            print(tablename, "Processsed", n, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                    for teamId in teamList:
                        #sql4 = f"""SELECT athleteId
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and teamId = {teamId};
                        #        """
                        val = (eventId, teamId)
                        cursor.execute(sql4,val)
                        rs = cursor.fetchall()
                        deleteRosterList = []
                        for rs_row in rs:
                            athleteId = rs_row[0]
                            if athleteId not in athleteList:
                                deleteRosterList.append(athleteId)
                        if len(deleteRosterList) > 0:
                            # print(deleteRosterList)
                            for athleteId in deleteRosterList:
                                #sql5 = f""" DELETE
                                #            FROM {tablename}
                                #            where eventId = {eventId} and teamid={teamId} and athleteId = {athleteId}
                                #        """
                                val = (eventId, teamId, athleteId)
                                cursor.execute(sql5, val)
                                print("delete in db", eventId, athleteId)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename,eventId, 'transaction rolled back')
                    msg = tablename + " " + str(eventId) + " update error:" + str(e)
                    # print(sql1)
                    # print(sql2)
                    # print(sql3)
                    # print(sql4)
                    # print(sql5)
                    # print(val)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                else:
                    conn.commit()
                    # print(tablename, 'record inserted successfully')
                    msg = tablename + " " + str(eventId) + " update complte"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def detailTypesCreateTableSQL(dbcur, tablename):
    # tablename = "DetailTypes"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            id bigint DEFAULT NULL,
            typeId int NOT NULL,
            typeText text,
            updateId INT DEFAULT NULL,            
            CONSTRAINT fk_DetailTypes_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def detailTypesInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "DetailTypes"
    (bExist, msg) = detailTypesCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT id,typeText"
                  " FROM " + tablename +
                  " WHERE typeId = ?;")
        sql2 = ("UPDATE " + tablename +
                  " SET id = ?,"
                       "typeText = ?,"
                       "updateId = ?"
                  " WHERE typeId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT id,typeText"
                " FROM " + tablename +
                " WHERE typeId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET id = %s,"
                "typeText = %s,"
                "updateId = %s"
                " WHERE typeId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    detailId = row['id']
                    typeId = row['typeId']
                    typeText = row['typeText']
                    updateId = row['updateId']
                    # print(i,'row=',year,typeId,name,slug,updateId)
                    #sql1 = f"""SELECT id,typeText
                    #            FROM {tablename}
                    #            WHERE typeId = {typeId};
                    #        """
                    val = (typeId,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        detailIdOld = rs[0]
                        typeTextOld = rs[1]
                        # print(year,',',yearOld, year == yearOld)
                        # print(name,',',nameOld, name == nameOld)
                        # print(slug,',',slugOld, slug == slugOld)
                        if detailId != detailIdOld or typeText != typeTextOld:
                            #sql2 = f"""UPDATE {tablename}
                            #            SET id = {detailId},
                            #               typeText = \"{typeText}\",
                            #               updateId = {updateId}
                            #E           WHERE typeId = {typeId};
                             #E       """
                            # print(sql2)
                            # print(i, "update", detailId, typeId, typeText, updateId)
                            val = (detailId, typeText, updateId, typeId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", detailId, typeId, typeText, updateId)
                            nSkip += 1
                    else:
                        #sql3 = f"""INSERT INTO {tablename}
                        #            VALUES {detailId},{typeId}\"{typeText}\",{updateId};
                        #        """
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", detailId, typeId, typeText, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                                "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                # print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def detailsCreateTableSQL(dbcur, tablename):
    # tablename = "Details"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        print(tablename)
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            detailOrder INT NOT NULL,
            typeId int DEFAULT NULL,
            typeText VARCHAR(100),
            clockValue int DEFAULT NULL,
            clockDisplayValue VARCHAR(100),
            scoringPlay BIT DEFAULT NULL,
            scoreValue int DEFAULT NULL,
            teamId int DEFAULT NULL,
            redCard tinyint(1) DEFAULT NULL,
            yellowCard tinyint(1) DEFAULT NULL,
            penaltyKick tinyint(1) DEFAULT NULL,
            ownGoal tinyint(1) DEFAULT NULL,
            shootout tinyint(1) DEFAULT NULL,
            athletesInvolved int DEFAULT NULL,
            updateId INT DEFAULT NULL,
            PRIMARY KEY (eventId, detailOrder),
            CONSTRAINT fk_Details_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Details_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def detailsInsertRecordSQL(osStr,conn, cursor, tablename, df_records_full):
    # tablename = "Details"
    (bExist, msg) = detailsCreateTableSQL(cursor, tablename)
    # print(msg)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    nSuccess = 0
    updateId = df_records_full.iloc[0]['updateId']
    nTotalCol = df_records_full[df_records_full.columns[0]].count()
    print(tablename, 'Total records=', nTotalCol)
    # print(len(df_records_full.index))
    # print(df_records_full.shape)
    bInsert = True
    if osStr == "Windows":
        sql1 = ("SELECT typeId,typeText,"
                      "  clockValue,"
                      "  clockDisplayValue,"
                      "  scoringPlay, scoreValue, teamId,"
                      "  redCard, yellowCard, penaltyKick, ownGoal,shootout,athletesInvolved"
                " FROM " + tablename +
                " WHERE eventId = ? and detailOrder = ?;")
        sql2 = ("UPDATE " + tablename +
                   " SET typeId = ?,"
                   "     typeText = ?,"
                   "     clockValue = ?,"
                   "     clockDisplayValue = ?,"
                   "     scoringPlay = ?,"
                   "     scoreValue = ?,"
                   "     teamId = ?,"
                   "     redCard = ?,"
                   "     yellowCard = ?,"
                   "     penaltyKick = ?,"
                   "     ownGoal = ?,"
                   "     shootout = ?,"
                   "     athletesInvolved = ?,"
                   "     updateId = ?"
                   " WHERE eventId = ? and "
                   "     detailOrder = ?;")
        sql4 = ("SELECT detailOrder"
                     " FROM " + tablename +
                     " WHERE eventId = ?;")
        sql5 = ("DELETE"
                  " FROM " + tablename +
                  " WHERE eventId = ? and detailOrder = ?;")
    else:
        sql1 = ("SELECT typeId,typeText,"
                "  clockValue,"
                "  clockDisplayValue,"
                "  scoringPlay, scoreValue, teamId,"
                "  redCard, yellowCard, penaltyKick, ownGoal,shootout,athletesInvolved"
                " FROM " + tablename +
                " WHERE eventId = %s and detailOrder = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET typeId = %s,"
                "     typeText = %s,"
                "     clockValue = %s,"
                "     clockDisplayValue = %s,"
                "     scoringPlay = %s,"
                "     scoreValue = %s,"
                "     teamId = %s,"
                "     redCard = %s,"
                "     yellowCard = %s,"
                "     penaltyKick = %s,"
                "     ownGoal = %s,"
                "     shootout = %s,"
                "     athletesInvolved = %s,"
                "     updateId = %s"
                " WHERE eventId = %s and "
                "     detailOrder = %s;")
        sql4 = ("SELECT detailOrder"
                " FROM " + tablename +
                " WHERE eventId = %s;")
        sql5 = ("DELETE"
                " FROM " + tablename +
                " WHERE eventId = %s and detailOrder = %s;")
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            eventList = df_records_full.eventId.unique()
            nEvents = len(eventList)
            # print('nEvents=', nEvents)
            n = 0
            for eventId in eventList:
                df_records = df_records_full.loc[df_records_full['eventId'] == eventId]
                orderList = []
                nTotal = df_records[df_records.columns[0]].count()
                # print(n, eventId,nEvents,nTotal,nTotalCol)
                nCol = len(df_records.axes[1])
                if osStr == "Windows":
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
                else:
                    sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
                try:
                    # print(tablename,"Total records=", nTotal)
                    for i, row in df_records.iterrows():
                        n += 1
                        # print(n,i,tuple(row))
                        eventId = int(row['eventId'])
                        detailOrder = int(row['order'])
                        orderList.append(detailOrder)
                        typeId = int(row['typeId'])
                        typeText = row['typeText']
                        clockValue = int(row['clockValue'])
                        clockDisplayValue = row['clockDisplayValue']
                        scoringPlay = bool(row['scoringPlay'])
                        scoreValue = int(row['scoreValue'])
                        teamId = int(row['teamId'])
                        redCard = bool(row['redCard'])
                        yellowCard = bool(row['yellowCard'])
                        penaltyKick = bool(row['penaltyKick'])
                        ownGoal = bool(row['ownGoal'])
                        shootout = bool(row['shootout'])
                        athleteId = int(row['athletesInvolved'])
                        updateId = int(row['updateId'])
                        # print(i, "from df", id, stat, name, updateId)
                        #sql1 = f"""SELECT typeId,typeText,
                        #                    clockValue,
                        #                    clockDisplayValue,
                        #                    scoringPlay, scoreValue, teamId,
                        #                    redCard, yellowCard, penaltyKick, ownGoal,shootout,athletesInvolved
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId} and detailOrder = {detailOrder}
                        #        """
                        # print(sql1)
                        val = (eventId, detailOrder)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            typeIdOld = rs[0]
                            typeTextOld = rs[1]
                            clockValueOld = rs[2]
                            clockDisplayValueOld = rs[3]
                            scoringPlayOld = rs[4]
                            scoreValueOld = rs[5]
                            teamIdOld = rs[6]
                            redCardOld = rs[7]
                            yellowCardOld = rs[8]
                            penaltyKickOld = rs[9]
                            ownGoalOld = rs[10]
                            shootoutOld = rs[11]
                            athleteIdOld = rs[12]
                            if (typeId != typeIdOld or
                                    typeText != typeTextOld or
                                    clockValue != clockValueOld or
                                    clockDisplayValue != clockDisplayValueOld or
                                    scoringPlay != scoringPlayOld or
                                    scoreValue != scoreValueOld or
                                    teamId != teamIdOld or
                                    redCard != redCardOld or
                                    yellowCard != yellowCardOld or
                                    penaltyKick != penaltyKickOld or
                                    ownGoal != ownGoalOld or
                                    shootout != shootoutOld or
                                    athleteId != athleteIdOld):
                                # print(1, active, activeOld, active == activeOld)
                                # print(2, starter, starterOld, active == activeOld)
                                # print(3, jersey, jerseyOld, jersey == jerseyOld)
                                # print(4, athleteDisplayName, athleteDisplayNameOld, athleteDisplayName == athleteDisplayNameOld)
                                # print(5, position, positionOld, position == positionOld)
                                # print(6, subbedIn, subbedInOld, subbedIn == subbedInOld)
                                # print(7, subbedOut, subbedOutOld, subbedOut == subbedOutOld)
                                # print(8, formationPlace, formationPlaceOld, formationPlace == formationPlaceOld)
                                # print(9, hasStats, hasStatsOld, hasStats == hasStatsOld)
                                # print(10, hasPlays, hasPlaysOld, hasPlays == hasPlaysOld)
                                # sql2 = f"""UPDATE {tablename}
                                #            SET typeId = ?,
                                #                typeText = ?,
                                #                clockValue = ?,
                                #                clockDisplayValue = ?,
                                #                scoringPlay = ?,
                                #                scoreValue = ?,
                                #                teamId = ?,
                                #                redCard = ?,
                                #                yellowCard = ?,
                                #                penaltyKick = ?,
                                #                ownGoal = ?,
                                #                shootout = ?,
                                #                athletesInvolved = ?,
                                #                updateId = ?
                                #            WHERE eventId = ? and
                                #                detailOrder = ?;
                                #        """
                                # print(sql2)
                                val = (typeId,
                                        typeText,
                                        clockValue,
                                        clockDisplayValue,
                                        scoringPlay,
                                        scoreValue,
                                        teamId,
                                        redCard,
                                        yellowCard,
                                        penaltyKick,
                                        ownGoal,
                                        shootout,
                                        athleteId,
                                        updateId,
                                        eventId,
                                        detailOrder
                                        )
                                # print(n, "update", eventId, detailOrder, typeText, teamId, athleteId, updateId)
                                # print(tuple(row))
                                nUpdate += 1
                                cursor.execute(sql2,val)
                            else:
                                # print(n, "skip", eventId, detailOrder, typeText, teamId, athleteId, updateId)
                                #
                                # print(i, tuple(row))
                                msg = "skip"
                                nSkip += 1
                        else:
                            #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(n, "insert", eventId, detailOrder, typeText, teamId, athleteId, updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                        if int(n / 1000) * 1000 == n or n == nTotalCol:
                            print(tablename, "Processsed", n, "out of", nTotalCol,
                                  "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
                    for nOrder in orderList:
                        #sql4 = f"""SELECT detailOrder
                        #            FROM {tablename}
                        #            WHERE eventId = {eventId};
                        #        """
                        val = (eventId,)
                        cursor.execute(sql4, val)
                        rs = cursor.fetchall()
                        deleteOrderList = []
                        for rs_row in rs:
                            nOrder = rs_row[0]
                            if nOrder not in orderList:
                                deleteOrderList.append(nOrder)
                        if len(deleteOrderList) > 0:
                            # print(deleteRosterList)
                            for nOrder in deleteOrderList:
                                #sql5 = f""" DELETE
                                #            FROM {tablename}
                                #            where eventId = {eventId} and detailOrder = {nOrder}
                                #        """
                                val = (eventId,nOrder)
                                cursor.execute(sql5,val)
                                print("delete in db", eventId, nOrder)
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print(tablename, eventId, 'transaction rolled back')
                    msg = tablename +" "+ str(eventId) + " update error:" + str(e)
                    currentTime = datetime.now(timezone.utc)
                    errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
                    nSuccess = 0
                else:
                    conn.commit()
                    # print(tablename, 'record inserted successfully')
                    msg = tablename +" "+ str(eventId) + " update complete"
                    currentTime = datetime.now(timezone.utc)
                    tmpMsg = [{'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                        'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol}]
                    if nSuccess == 0:
                        errMessages.append(tmpMsg)
                        nSuccess = 1
                    else:
                        errMessages = errMessages[:-1] + tmpMsg
            return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records_full)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotalCol})
    return (errMessages)
def playerStatsDBCreateTableSQL(dbcur, tablename):
    # tablename = "PlayStats"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
              id int NOT NULL,
              uid text,
              guid text,
              league text,
              teamId int NOT NULL,
              playerIndex int DEFAULT NULL,
              seasonYear VARCHAR(20) DEFAULT NULL,
              seasonType int NOT NULL,
              seasonName text,
              timestamp timestamp NULL DEFAULT NULL,
              foulsCommitted_category text,
              foulsCommitted_value int DEFAULT NULL,
              foulsCommitted_displayValue text,
              foulsSuffered_category text,
              foulsSuffered_value int DEFAULT NULL,
              foulsSuffered_displayValue text,
              redCards_category text,
              redCards_value int DEFAULT NULL,
              redCards_displayValue text,
              yellowCards_category text,
              yellowCards_value int DEFAULT NULL,
              yellowCards_displayValue text,
              ownGoals_category text,
              ownGoals_value int DEFAULT NULL,
              ownGoals_displayValue text,
              appearances_category text,
              appearances_value int DEFAULT NULL,
              appearances_displayValue text,
              subIns_category text,
              subIns_value int DEFAULT NULL,
              subIns_displayValue text,
              goalAssists_category text,
              goalAssists_value int DEFAULT NULL,
              goalAssists_displayValue text,
              offsides_category text,
              offsides_value int DEFAULT NULL,
              offsides_displayValue text,
              shotsOnTarget_category text,
              shotsOnTarget_value int DEFAULT NULL,
              shotsOnTarget_displayValue text,
              totalShots_category text,
              totalShots_value int DEFAULT NULL,
              totalShots_displayValue text,
              totalGoals_category text,
              totalGoals_value int DEFAULT NULL,
              totalGoals_displayValue text,
              saves_category text,
              saves_value int DEFAULT NULL,
              saves_displayValue text,
              shotsFaced_category text,
              shotsFaced_value int DEFAULT NULL,
              shotsFaced_displayValue text,
              goalsConceded_category text,
              goalsConceded_value int DEFAULT NULL,
              goalsConceded_displayValue text,
              updateId int NOT NULL,
              PRIMARY KEY (id,teamId,seasonType),
              CONSTRAINT fk_PlayerStats_id FOREIGN KEY(id) REFERENCES PlayerDB(id),
              CONSTRAINT fk_PlayerStats_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
              CONSTRAINT fk_PlayerStats_seasonType FOREIGN KEY(seasonType) REFERENCES SeasonType(typeId),
              CONSTRAINT fk_PlayerStats_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def playerStatsDBInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "PlayerStatsDB"
    tablename1 = "SeasonType"
    (bExist,msg) = playerStatsDBCreateTableSQL(cursor,tablename)
    # print(msg)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    # print(len(df_records.index))
    # print(df_records.shape)
    seasonTypeIdList = []
    cursor.execute(f"SELECT typeId FROM {tablename1};")
    rs = cursor.fetchall()
    for row in rs:
        seasonTypeIdList.append(row[0])
    print("seasonType Id list length:", len(seasonTypeIdList))
    bExist = True
    nTotal = df_records[df_records.columns[0]].count()
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT uid,"
                            "guid,"
                            "league,"
                            "playerIndex,"
                            "seasonYear,"
                            "seasonName,"
                            "timestamp,"
                            "foulsCommitted_category,"
                            "foulsCommitted_value,"
                            "foulsCommitted_displayValue,"
                            "foulsSuffered_category,"
                            "foulsSuffered_value,"
                            "foulsSuffered_displayValue,"
                            "redCards_category,"
                            "redCards_value,"
                            "redCards_displayValue,"
                            "yellowCards_category,"
                            "yellowCards_value,"
                            "yellowCards_displayValue,"
                            "ownGoals_category,"
                            "ownGoals_value,"
                            "ownGoals_displayValue,"
                            "appearances_category,"
                            "appearances_value,"
                            "appearances_displayValue,"
                            "subIns_category,"
                            "subIns_value,"
                            "subIns_displayValue,"
                            "goalAssists_category,"
                            "goalAssists_value,"
                            "goalAssists_displayValue,"
                            "offsides_category,"
                            "offsides_value,"
                            "offsides_displayValue,"
                            "shotsOnTarget_category,"
                            "shotsOnTarget_value,"
                            "shotsOnTarget_displayValue,"
                            "totalShots_category,"
                            "totalShots_value,"
                            "totalShots_displayValue,"
                            "totalGoals_category,"
                            "totalGoals_value,"
                            "totalGoals_displayValue,"
                            "saves_category,"
                            "saves_value,"
                            "saves_displayValue,"
                            "shotsFaced_category,"
                            "shotsFaced_value,"
                            "shotsFaced_displayValue,"
                            "goalsConceded_category,"
                            "goalsConceded_value,"
                            "goalsConceded_displayValue"
                    " FROM " + tablename +
                    " WHERE id =? and teamId = ? and seasonType= ?;")
        sql2 = ("UPDATE " + tablename +
                  " SET uid = ?,"
                        "guid = ?,"
                        "league = ?,"
                        "playerIndex = ?,"
                        "seasonYear = ?,"
                        "seasonName = ?,"
                        "timestamp = ?,"
                        "foulsCommitted_category = ?,"
                        "foulsCommitted_value = ?,"
                        "foulsCommitted_displayValue = ?,"
                        "foulsSuffered_category = ?,"
                        "foulsSuffered_value = ?,"
                        "foulsSuffered_displayValue = ?,"
                        "redCards_category = ?,"
                        "redCards_value = ?,"
                        "redCards_displayValue = ?,"
                        "yellowCards_category = ?,"
                        "yellowCards_value = ?,"
                        "yellowCards_displayValue = ?,"
                        "ownGoals_category = ?,"
                        "ownGoals_value = ?,"
                        "ownGoals_displayValue = ?,"
                        "appearances_category = ?,"
                        "appearances_value = ?,"
                        "appearances_displayValue = ?,"
                        "subIns_category = ?,"
                        "subIns_value = ?,"
                        "subIns_displayValue = ?,"
                        "goalAssists_category = ?,"
                        "goalAssists_value = ?,"
                        "goalAssists_displayValue = ?,"
                        "offsides_category = ?,"
                        "offsides_value = ?,"
                        "offsides_displayValue = ?,"
                        "shotsOnTarget_category = ?,"
                        "shotsOnTarget_value = ?,"
                        "shotsOnTarget_displayValue = ?,"
                        "totalShots_category = ?,"
                        "totalShots_value = ?,"
                        "totalShots_displayValue = ?,"
                        "totalGoals_category = ?,"
                        "totalGoals_value = ?,"
                        "totalGoals_displayValue = ?,"
                        "saves_category = ?,"
                        "saves_value = ?,"
                        "saves_displayValue = ?,"
                        "shotsFaced_category = ?,"
                        "shotsFaced_value = ?,"
                        "shotsFaced_displayValue = ?,"
                        "goalsConceded_category = ?,"
                        "goalsConceded_value = ?,"
                        "goalsConceded_displayValue = ?,"
                        "updateId = ?"
                    " WHERE id = ? and "
                         " teamId = ? and "
                         " seasonType = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
        sql4 = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 4 + "?)"
        sql_seasonType = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 4 + "?)"
    else:
        sql1 = ("SELECT uid,"
                            "guid,"
                            "league,"
                            "playerIndex,"
                            "seasonYear,"
                            "seasonName,"
                            "timestamp,"
                            "foulsCommitted_category,"
                            "foulsCommitted_value,"
                            "foulsCommitted_displayValue,"
                            "foulsSuffered_category,"
                            "foulsSuffered_value,"
                            "foulsSuffered_displayValue,"
                            "redCards_category,"
                            "redCards_value,"
                            "redCards_displayValue,"
                            "yellowCards_category,"
                            "yellowCards_value,"
                            "yellowCards_displayValue,"
                            "ownGoals_category,"
                            "ownGoals_value,"
                            "ownGoals_displayValue,"
                            "appearances_category,"
                            "appearances_value,"
                            "appearances_displayValue,"
                            "subIns_category,"
                            "subIns_value,"
                            "subIns_displayValue,"
                            "goalAssists_category,"
                            "goalAssists_value,"
                            "goalAssists_displayValue,"
                            "offsides_category,"
                            "offsides_value,"
                            "offsides_displayValue,"
                            "shotsOnTarget_category,"
                            "shotsOnTarget_value,"
                            "shotsOnTarget_displayValue,"
                            "totalShots_category,"
                            "totalShots_value,"
                            "totalShots_displayValue,"
                            "totalGoals_category,"
                            "totalGoals_value,"
                            "totalGoals_displayValue,"
                            "saves_category,"
                            "saves_value,"
                            "saves_displayValue,"
                            "shotsFaced_category,"
                            "shotsFaced_value,"
                            "shotsFaced_displayValue,"
                            "goalsConceded_category,"
                            "goalsConceded_value,"
                            "goalsConceded_displayValue"
                    " FROM " + tablename +
                    " WHERE id =%s and teamId = %s and seasonType= %s;")
        sql2 = ("UPDATE " + tablename +
                  " SET uid = %s,"
                        "guid = %s,"
                        "league = %s,"
                        "playerIndex = %s,"
                        "seasonYear = %s,"
                        "seasonName = %s,"
                        "timestamp = %s,"
                        "foulsCommitted_category = %s,"
                        "foulsCommitted_value = %s,"
                        "foulsCommitted_displayValue = %s,"
                        "foulsSuffered_category = %s,"
                        "foulsSuffered_value = %s,"
                        "foulsSuffered_displayValue = %s,"
                        "redCards_category = %s,"
                        "redCards_value = %s,"
                        "redCards_displayValue = %s,"
                        "yellowCards_category = %s,"
                        "yellowCards_value = %s,"
                        "yellowCards_displayValue = %s,"
                        "ownGoals_category = %s,"
                        "ownGoals_value = %s,"
                        "ownGoals_displayValue = %s,"
                        "appearances_category = %s,"
                        "appearances_value = %s,"
                        "appearances_displayValue = %s,"
                        "subIns_category = %s,"
                        "subIns_value = %s,"
                        "subIns_displayValue = %s,"
                        "goalAssists_category = %s,"
                        "goalAssists_value = %s,"
                        "goalAssists_displayValue = %s,"
                        "offsides_category = %s,"
                        "offsides_value = %s,"
                        "offsides_displayValue = %s,"
                        "shotsOnTarget_category = %s,"
                        "shotsOnTarget_value = %s,"
                        "shotsOnTarget_displayValue = %s,"
                        "totalShots_category = %s,"
                        "totalShots_value = %s,"
                        "totalShots_displayValue = %s,"
                        "totalGoals_category = %s,"
                        "totalGoals_value = %s,"
                        "totalGoals_displayValue = %s,"
                        "saves_category = %s,"
                        "saves_value = %s,"
                        "saves_displayValue = %s,"
                        "shotsFaced_category = %s,"
                        "shotsFaced_value = %s,"
                        "shotsFaced_displayValue = %s,"
                        "goalsConceded_category = %s,"
                        "goalsConceded_value = %s,"
                        "goalsConceded_displayValue = %s,"
                        "updateId = %s"
                    " WHERE id = %s and "
                         " teamId = %s and "
                         " seasonType = %s;")

        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
        sql4 = "INSERT INTO " + tablename1 + " VALUES (" + "%s," * 4 + "%s)"
        sql_seasonType = "INSERT INTO " + tablename1 + " VALUES (" + "%s," * 4 + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    playerId = int(row['id'])
                    uid = row['uid']
                    guid = row['guid']
                    league = row['league']
                    teamId = int(row['teamId'])
                    playerIndex = int(row['index'])
                    seasonYear = str(row['seasonYear'])
                    seasonType = int(row['seasonType'])
                    seasonName = row['seasonName']
                    timestamp = row['timestamp']
                    foulsCommitted_category = row['foulsCommitted.category']
                    foulsCommitted_value = int(row['foulsCommitted.value'])
                    foulsCommitted_displayValue = row['foulsCommitted.displayValue']
                    foulsSuffered_category = row['foulsSuffered.category']
                    foulsSuffered_value = int(row['foulsSuffered.value'])
                    foulsSuffered_displayValue = row['foulsSuffered.displayValue']
                    redCards_category = row['redCards.category']
                    redCards_value = int(row['redCards.value'])
                    redCards_displayValue = row['redCards.displayValue']
                    yellowCards_category = row['yellowCards.category']
                    yellowCards_value = int(row['yellowCards.value'])
                    yellowCards_displayValue = row['yellowCards.displayValue']
                    ownGoals_category = row['ownGoals.category']
                    ownGoals_value = int(row['ownGoals.value'])
                    ownGoals_displayValue = row['ownGoals.displayValue']
                    appearances_category = row['appearances.category']
                    appearances_value = int(row['appearances.value'])
                    appearances_displayValue = row['appearances.displayValue']
                    subIns_category = row['subIns.category']
                    subIns_value = int(row['subIns.value'])
                    subIns_displayValue = row['subIns.displayValue']
                    goalAssists_category = row['goalAssists.category']
                    goalAssists_value = int(row['goalAssists.value'])
                    goalAssists_displayValue = row['goalAssists.displayValue']
                    offsides_category = row['offsides.category']
                    offsides_value = int(row['offsides.value'])
                    offsides_displayValue = row['offsides.displayValue']
                    shotsOnTarget_category = row['shotsOnTarget.category']
                    shotsOnTarget_value = int(row['shotsOnTarget.value'])
                    shotsOnTarget_displayValue = row['shotsOnTarget.displayValue']
                    totalShots_category = row['totalShots.category']
                    totalShots_value = int(row['totalShots.value'])
                    totalShots_displayValue = row['totalShots.displayValue']
                    totalGoals_category = row['totalGoals.category']
                    totalGoals_value = int(row['totalGoals.value'])
                    totalGoals_displayValue = row['totalGoals.displayValue']
                    saves_category = row['saves.category']
                    saves_value = int(row['saves.value'])
                    saves_displayValue = row['saves.displayValue']
                    shotsFaced_category = row['shotsFaced.category']
                    shotsFaced_value = int(row['shotsFaced.value'])
                    shotsFaced_displayValue = row['shotsFaced.displayValue']
                    goalsConceded_category = row['goalsConceded.category']
                    goalsConceded_value = int(row['goalsConceded.value'])
                    goalsConceded_displayValue = row['goalsConceded.displayValue']
                    updateId = int(row['updateId'])
                    if seasonType not in seasonTypeIdList:
                        # sql_seasonType = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 4 + "?)"
                        cursor.execute(sql_seasonType, tuple([seasonYear, seasonType, seasonName, "", updateId]))
                        seasonTypeIdList.append(seasonType)
                    # print(i, "from df", id, stat, name, updateId)
                    #sql1 = f"""SELECT uid,
                    #                    guid,
                    #                    league,
                    #                    playerIndex,
                    #                    seasonYear,
                    #                    seasonName,
                    #                    timestamp,
                    #                    foulsCommitted_category,
                    #                    foulsCommitted_value,
                    #                    foulsCommitted_displayValue,
                    #                    foulsSuffered_category,
                    #                    foulsSuffered_value,
                    #                    foulsSuffered_displayValue,
                    #                    redCards_category,
                    #                    redCards_value,
                    #                    redCards_displayValue,
                    #                    yellowCards_category,
                    #                    yellowCards_value,
                    #                    yellowCards_displayValue,
                    #                    ownGoals_category,
                    #                    ownGoals_value,
                    #                    ownGoals_displayValue,
                    #                    appearances_category,
                    #                    appearances_value,
                    #                    appearances_displayValue,
                    #                    subIns_category,
                    #                    subIns_value,
                    #                    subIns_displayValue,
                    #                    goalAssists_category,
                    #                    goalAssists_value,
                    #                    goalAssists_displayValue,
                    #                    offsides_category,
                    #                    offsides_value,
                    #                    offsides_displayValue,
                    #                    shotsOnTarget_category,
                    #                    shotsOnTarget_value,
                    #                    shotsOnTarget_displayValue,
                    #                    totalShots_category,
                    #                    totalShots_value,
                    #                    totalShots_displayValue,
                    #                    totalGoals_category,
                    #                    totalGoals_value,
                    #                    totalGoals_displayValue,
                    #                    saves_category,
                    #                    saves_value,
                    #                    saves_displayValue,
                    #                    shotsFaced_category,
                    #                    shotsFaced_value,
                    #                    shotsFaced_displayValue,
                    #                    goalsConceded_category,
                    #                    goalsConceded_value,
                    #                    goalsConceded_displayValue
                    #            FROM {tablename}
                    #            WHERE id = {playerId} and teamId = {teamId} and seasonType={seasonType};
                    #        """
                    # print(sql1)
                    val = (playerId, teamId, seasonType)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        uidOld = rs[0]
                        guidOld = rs[1]
                        leagueOld = rs[2]
                        playerIndexOld = rs[3]
                        seasonYearOld = rs[4]
                        seasonNameOld = rs[5]
                        timestampOld = rs[6].replace(tzinfo=tz.gettz("UTC"))
                        foulsCommitted_categoryOld = rs[7]
                        foulsCommitted_valueOld = rs[8]
                        foulsCommitted_displayValueOld = rs[9]
                        foulsSuffered_categoryOld = rs[10]
                        foulsSuffered_valueOld = rs[11]
                        foulsSuffered_displayValueOld = rs[12]
                        redCards_categoryOld = rs[13]
                        redCards_valueOld = rs[14]
                        redCards_displayValueOld = rs[15]
                        yellowCards_categoryOld = rs[16]
                        yellowCards_valueOld = rs[17]
                        yellowCards_displayValueOld = rs[18]
                        ownGoals_categoryOld = rs[19]
                        ownGoals_valueOld = rs[20]
                        ownGoals_displayValueOld = rs[21]
                        appearances_categoryOld = rs[22]
                        appearances_valueOld = rs[23]
                        appearances_displayValueOld = rs[24]
                        subIns_categoryOld = rs[25]
                        subIns_valueOld = rs[26]
                        subIns_displayValueOld = rs[27]
                        goalAssists_categoryOld = rs[28]
                        goalAssists_valueOld = rs[29]
                        goalAssists_displayValueOld = rs[30]
                        offsides_categoryOld = rs[31]
                        offsides_valueOld = rs[32]
                        offsides_displayValueOld = rs[33]
                        shotsOnTarget_categoryOld = rs[34]
                        shotsOnTarget_valueOld = rs[35]
                        shotsOnTarget_displayValueOld = rs[36]
                        totalShots_categoryOld = rs[37]
                        totalShots_valueOld = rs[38]
                        totalShots_displayValueOld = rs[39]
                        totalGoals_categoryOld = rs[40]
                        totalGoals_valueOld = rs[41]
                        totalGoals_displayValueOld = rs[42]
                        saves_categoryOld = rs[43]
                        saves_valueOld = rs[44]
                        saves_displayValueOld = rs[45]
                        shotsFaced_categoryOld = rs[46]
                        shotsFaced_valueOld = rs[47]
                        shotsFaced_displayValueOld = rs[48]
                        goalsConceded_categoryOld = rs[49]
                        goalsConceded_valueOld = rs[50]
                        goalsConceded_displayValueOld = rs[51]
                        if (uid != uidOld or
                                guid != guidOld or
                                league != leagueOld or
                                playerIndex != playerIndexOld or
                                seasonYear != seasonYearOld or
                                seasonName != seasonNameOld or
                                timestamp != timestampOld or
                                foulsCommitted_category != foulsCommitted_categoryOld or
                                foulsCommitted_value != foulsCommitted_valueOld or
                                foulsCommitted_displayValue != foulsCommitted_displayValueOld or
                                foulsSuffered_category != foulsSuffered_categoryOld or
                                foulsSuffered_value != foulsSuffered_valueOld or
                                foulsSuffered_displayValue != foulsSuffered_displayValueOld or
                                redCards_category != redCards_categoryOld or
                                redCards_value != redCards_valueOld or
                                redCards_displayValue != redCards_displayValueOld or
                                yellowCards_category != yellowCards_categoryOld or
                                yellowCards_value != yellowCards_valueOld or
                                yellowCards_displayValue != yellowCards_displayValueOld or
                                ownGoals_category != ownGoals_categoryOld or
                                ownGoals_value != ownGoals_valueOld or
                                ownGoals_displayValue != ownGoals_displayValueOld or
                                appearances_category != appearances_categoryOld or
                                appearances_value != appearances_valueOld or
                                appearances_displayValue != appearances_displayValueOld or
                                subIns_category != subIns_categoryOld or
                                subIns_value != subIns_valueOld or
                                subIns_displayValue != subIns_displayValueOld or
                                goalAssists_category != goalAssists_categoryOld or
                                goalAssists_value != goalAssists_valueOld or
                                goalAssists_displayValue != goalAssists_displayValueOld or
                                offsides_category != offsides_categoryOld or
                                offsides_value != offsides_valueOld or
                                offsides_displayValue != offsides_displayValueOld or
                                shotsOnTarget_category != shotsOnTarget_categoryOld or
                                shotsOnTarget_value != shotsOnTarget_valueOld or
                                shotsOnTarget_displayValue != shotsOnTarget_displayValueOld or
                                totalShots_category != totalShots_categoryOld or
                                totalShots_value != totalShots_valueOld or
                                totalShots_displayValue != totalShots_displayValueOld or
                                totalGoals_category != totalGoals_categoryOld or
                                totalGoals_value != totalGoals_valueOld or
                                totalGoals_displayValue != totalGoals_displayValueOld or
                                saves_category != saves_categoryOld or
                                saves_value != saves_valueOld or
                                saves_displayValue != saves_displayValueOld or
                                shotsFaced_category != shotsFaced_categoryOld or
                                shotsFaced_value != shotsFaced_valueOld or
                                shotsFaced_displayValue != shotsFaced_displayValueOld or
                                goalsConceded_category != goalsConceded_categoryOld or
                                goalsConceded_value != goalsConceded_valueOld or
                                goalsConceded_displayValue != goalsConceded_displayValueOld):
                            """
                            print(0, uid, uidOld, uid == uidOld)
                            print(1, guid, guidOld, guid == guidOld)
                            print(2, league, leagueOld, league == leagueOld)
                            print(4, playerIndex, playerIndexOld, playerIndex == playerIndexOld)
                            print(5, seasonYear, seasonYearOld, seasonYear == seasonYearOld)
                            print(7, seasonName, seasonNameOld, seasonName == seasonNameOld)
                            print(8, timestamp, timestampOld, timestamp == timestampOld)
                            print(9, foulsCommitted_category, foulsCommitted_categoryOld,
                                  foulsCommitted_category == foulsCommitted_categoryOld)
                            print(10, foulsCommitted_value, foulsCommitted_valueOld,
                                 foulsCommitted_value == foulsCommitted_valueOld)
                            print(11, foulsCommitted_displayValue, foulsCommitted_displayValueOld,
                                 foulsCommitted_displayValue == foulsCommitted_displayValueOld)
                            print(12, foulsSuffered_category, foulsSuffered_categoryOld,
                                 foulsSuffered_category == foulsSuffered_categoryOld)
                            print(13, foulsSuffered_value, foulsSuffered_valueOld,
                                  foulsSuffered_value == foulsSuffered_valueOld)
                            print(14, foulsSuffered_displayValue, foulsSuffered_displayValueOld,
                                  foulsSuffered_displayValue == foulsSuffered_displayValueOld)
                            print(15, redCards_category, redCards_categoryOld,
                                  redCards_category == redCards_categoryOld)
                            print(16, redCards_value, redCards_valueOld, redCards_value == redCards_valueOld)
                            print(17, redCards_displayValue, redCards_displayValueOld,
                                  redCards_displayValue == redCards_displayValueOld)
                            print(18, yellowCards_category, yellowCards_categoryOld,
                                  yellowCards_category == yellowCards_categoryOld)
                            print(19, yellowCards_value, yellowCards_valueOld,
                                  yellowCards_value == yellowCards_valueOld)
                            print(20, yellowCards_displayValue, yellowCards_displayValueOld,
                                  yellowCards_displayValue == yellowCards_displayValueOld)
                            print(21, ownGoals_category, ownGoals_categoryOld,
                                  ownGoals_category == ownGoals_categoryOld)
                            print(22, ownGoals_value, ownGoals_valueOld, ownGoals_value == ownGoals_valueOld)
                            print(23, ownGoals_displayValue, ownGoals_displayValueOld,
                                  ownGoals_displayValue == ownGoals_displayValueOld)
                            print(24, appearances_category, appearances_categoryOld,
                                  appearances_category == appearances_categoryOld)
                            print(25, appearances_value, appearances_valueOld,
                                  appearances_value == appearances_valueOld)
                            print(26, appearances_displayValue, appearances_displayValueOld,
                                  appearances_displayValue == appearances_displayValueOld)
                            print(27, subIns_category, subIns_categoryOld, subIns_category == subIns_categoryOld)
                            print(28, subIns_value, subIns_valueOld, subIns_value == subIns_valueOld)
                            print(29, subIns_displayValue, subIns_displayValueOld,
                                  subIns_displayValue == subIns_displayValueOld)
                            print(30, goalAssists_category, goalAssists_categoryOld,
                                  goalAssists_category == goalAssists_categoryOld)
                            print(31, goalAssists_value, goalAssists_valueOld,
                                  goalAssists_value == goalAssists_valueOld)
                            print(32, goalAssists_displayValue, goalAssists_displayValueOld,
                                  goalAssists_displayValue == goalAssists_displayValueOld)
                            print(33, offsides_category, offsides_categoryOld,
                                  offsides_category == offsides_categoryOld)
                            print(34, offsides_value, offsides_valueOld, offsides_value == offsides_valueOld)
                            print(35, offsides_displayValue, offsides_displayValueOld,
                                  offsides_displayValue == offsides_displayValueOld)
                            print(36, shotsOnTarget_category, shotsOnTarget_categoryOld,
                                  shotsOnTarget_category == shotsOnTarget_categoryOld)
                            print(37, shotsOnTarget_value, shotsOnTarget_valueOld,
                                  shotsOnTarget_value == shotsOnTarget_valueOld)
                            print(38, shotsOnTarget_displayValue, shotsOnTarget_displayValueOld,
                                  shotsOnTarget_displayValue == shotsOnTarget_displayValueOld)
                            print(39, totalShots_category, totalShots_categoryOld,
                                  totalShots_category == totalShots_categoryOld)
                            print(40, totalShots_value, totalShots_valueOld, totalShots_value == totalShots_valueOld)
                            print(41, totalShots_displayValue, totalShots_displayValueOld,
                                  totalShots_displayValue == totalShots_displayValueOld)
                            print(42, totalGoals_category, totalGoals_categoryOld,
                                  totalGoals_category == totalGoals_categoryOld)
                            print(43, totalGoals_value, totalGoals_valueOld, totalGoals_value == totalGoals_valueOld)
                            print(44, totalGoals_displayValue, totalGoals_displayValueOld,
                                  totalGoals_displayValue == totalGoals_displayValueOld)
                            print(45, saves_category, saves_categoryOld, saves_category == saves_categoryOld)
                            print(46, saves_value, saves_valueOld, saves_value == saves_valueOld)
                            print(47, saves_displayValue, saves_displayValueOld,
                                 saves_displayValue == saves_displayValueOld)
                            print(48, shotsFaced_category, shotsFaced_categoryOld,
                                  shotsFaced_category == shotsFaced_categoryOld)
                            print(49, shotsFaced_value, shotsFaced_valueOld, shotsFaced_value == shotsFaced_valueOld)
                            print(50, shotsFaced_displayValue, shotsFaced_displayValueOld,
                                  shotsFaced_displayValue == shotsFaced_displayValueOld)
                            print(51, goalsConceded_category, goalsConceded_categoryOld,
                                  goalsConceded_category == goalsConceded_categoryOld)
                            print(52, goalsConceded_value, goalsConceded_valueOld,
                                  goalsConceded_value == goalsConceded_valueOld)
                            print(53, goalsConceded_displayValue, goalsConceded_displayValueOld,
                                  goalsConceded_displayValue == goalsConceded_displayValueOld)
                            """
                            #sql2 = f"""UPDATE {tablename}
                            #            SET uid = ?,
                            #                guid = ?,
                            #                league = ?,
                            #                playerIndex = ?,
                            #                seasonYear = ?,
                            #                seasonName = ?,
                            #                timestamp = ?,
                            #                foulsCommitted_category = ?,
                            #                foulsCommitted_value = ?,
                            #                foulsCommitted_displayValue = ?,
                            #                foulsSuffered_category = ?,
                            #                foulsSuffered_value = ?,
                            #                foulsSuffered_displayValue = ?,
                            #                redCards_category = ?,
                            #                redCards_value = ?,
                            #                redCards_displayValue = ?,
                            #                yellowCards_category = ?,
                            #                yellowCards_value = ?,
                            #                yellowCards_displayValue = ?,
                            #                ownGoals_category = ?,
                            #                ownGoals_value = ?,
                            #                ownGoals_displayValue = ?,
                            #                appearances_category = ?,
                            #                appearances_value = ?,
                            #                appearances_displayValue = ?,
                            #                subIns_category = ?,
                            #                subIns_value = ?,
                            #                subIns_displayValue = ?,
                            #                goalAssists_category = ?,
                            #                goalAssists_value = ?,
                            #                goalAssists_displayValue = ?,
                            #                offsides_category = ?,
                            #                offsides_value = ?,
                            #                offsides_displayValue = ?,
                            #                shotsOnTarget_category = ?,
                            #                shotsOnTarget_value = ?,
                            #                shotsOnTarget_displayValue = ?,
                            #                totalShots_category = ?,
                            #                totalShots_value = ?,
                            #                totalShots_displayValue = ?,
                            #                totalGoals_category = ?,
                            #                totalGoals_value = ?,
                            #                totalGoals_displayValue = ?,
                            #                saves_category = ?,
                            #                saves_value = ?,
                            #                saves_displayValue = ?,
                            #                shotsFaced_category = ?,
                            #                shotsFaced_value = ?,
                            #                shotsFaced_displayValue = ?,
                            #                goalsConceded_category = ?,
                            #                goalsConceded_value = ?,
                            #                goalsConceded_displayValue = ?,
                            #                updateId = ?
                            #            WHERE id = ? and
                            #                  teamId = ? and
                            #                  seasonType = ?;
                            #             """
                            # print(n, "update", playerId, league,playerIndex, seasonType, updateId)
                            # print(tuple(row))
                            # print(sql2)
                            val = (uid,
                                    guid,
                                    league,
                                    playerIndex,
                                    seasonYear,
                                    seasonName,
                                    timestamp,
                                    foulsCommitted_category,
                                    foulsCommitted_value,
                                    foulsCommitted_displayValue,
                                    foulsSuffered_category,
                                    foulsSuffered_value,
                                    foulsSuffered_displayValue,
                                    redCards_category,
                                    redCards_value,
                                    redCards_displayValue,
                                    yellowCards_category,
                                    yellowCards_value,
                                    yellowCards_displayValue,
                                    ownGoals_category,
                                    ownGoals_value,
                                    ownGoals_displayValue,
                                    appearances_category,
                                    appearances_value,
                                    appearances_displayValue,
                                    subIns_category,
                                    subIns_value,
                                    subIns_displayValue,
                                    goalAssists_category,
                                    goalAssists_value,
                                    goalAssists_displayValue,
                                    offsides_category,
                                    offsides_value,
                                    offsides_displayValue,
                                    shotsOnTarget_category,
                                    shotsOnTarget_value,
                                    shotsOnTarget_displayValue,
                                    totalShots_category,
                                    totalShots_value,
                                    totalShots_displayValue,
                                    totalGoals_category,
                                    totalGoals_value,
                                    totalGoals_displayValue,
                                    saves_category,
                                    saves_value,
                                    saves_displayValue,
                                    shotsFaced_category,
                                    shotsFaced_value,
                                    shotsFaced_displayValue,
                                    goalsConceded_category,
                                    goalsConceded_value,
                                    goalsConceded_displayValue,
                                    updateId,
                                    playerId,
                                    teamId,
                                    seasonType
                                    )
                            nUpdate += 1
                            cursor.execute(sql2,val)
                        else:
                            # print(i, "skip", playerId, fullName, displayName, updateId)
                            # print(n, "skip", playerId, league, playerIndex, seasonType, updateId)
                            # print(i, "skip", playerId, league,playerIndex, updateId)
                            # print(i, tuple(row))
                            msg = "skip"
                            nSkip += 1

                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(n, "insert", playerId, league, playerIndex, seasonType, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                print("1",sql1)
                print("2",sql2)
                print("3",sql3)
                print("val",val)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        cursor.execute(f"SELECT typeId FROM {tablename1};")
        rs1 = cursor.fetchall()
        seasonTypeIdList = []
        k = 0
        for row in rs1:
            seasonTypeIdList.append(row[0])
        # print(seasonTypeList)
        try:
            for i, row in df_records.iterrows():
                k += 1
                tmpSeasonYear = tuple(row)[6]
                tmpSeasonTypeId = int(tuple(row)[7])
                tmpSeasonName = tuple(row)[8]
                tmpUpdateId = int(tuple(row)[55])
                if tmpSeasonTypeId not in seasonTypeIdList:
                    print(tmpSeasonYear, tmpSeasonTypeId, tmpSeasonName, tmpUpdateId)
                    # print(tuple(row))
                    # sql4 = "INSERT INTO " + tablename1 + " VALUES (" + "?," * 4 + "?)"
                    cursor.execute(sql4, tuple([tmpSeasonYear, tmpSeasonTypeId, tmpSeasonName, "", tmpUpdateId]))
                    seasonTypeIdList.append(tmpSeasonTypeId)
                #sql = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                # print(sql)
                nInsert += 1
                cursor.execute(sql3, tuple(row))
                if int(k / 1000) * 1000 == k or i + 1 == nTotal:
                    print(tablename, "Processsed", k, "out of", nTotal,
                          "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename1, 'transaction rolled back')
            msg = tablename1 + " insert error:" + str(e)
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
        else:
            print(tablename1, 'record inserted successfully')
            conn.commit()
            msg = tablename1 + " insert complete"
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            return (errMessages)
    else:
        msg = tablename + "update complete"
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def teamsInLeagueInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Teams"
    (bExist, msg) = teamsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "ntotal = ", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql_team_insert = ("INSERT INTO Teams VALUES (?,?,?,?,?,"
                           "?,?,?,?,?,"
                           "?,?,?,?,?);")
        sql1 = ("SELECT midsizeLeagueName, seasonType, uid,"
                "            location, name," 
                "            abbreviation, displayName, shortDisplayName, slug, nickname,"
                "            color, alternateColor," 
                "            isActive, isAllStar, hasRecord"
                " FROM " + tablename +
                " WHERE year=? and leagueId=? and teamId=?;")
        sql2 = ("UPDATE " + tablename +
                " SET midsizeLeagueName = ?,"
                "    seasonType = ?,"
                "    uid = ?,"
                "    location = ?,"
                "    name = ?," 
                "    abbreviation = ?,"
                "    displayName = ?,"
                "    shortDisplayName = ?,"
                "    slug = ?,"
                "    nickname = ?,"
                "    color = ?,"
                "    alternateColor = ?,"
                "    isActive = ?," 
                "    isAllStar = ?,"
                "    hasRecord = ?,"
                "    timeStamp = ?,"
                "    updateId = ?"
                " WHERE year =? and leagueId = ? and teamId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql_team_insert = ("INSERT INTO Teams VALUES (%s,%s,%s,%s,%s,"
                           "%s,%s,%s,%s,%s,"
                           "%s,%s,%s,%s,%s);")
        sql1 = ("SELECT midsizeLeagueName, seasonType, uid,"
                "            location, name," 
                "            abbreviation, displayName, shortDisplayName, slug, nickname,"
                "            color, alternateColor," 
                "            isActive, isAllStar, hasRecord"
                " FROM " + tablename +
                " WHERE year=%s and leagueId=%s and teamId=%s;")
        sql2 = ("UPDATE " + tablename +
                " SET midsizeLeagueName = %s,"
                "    seasonType = %s,"
                "    uid = %s,"
                "    location = %s,"
                "    name = %s," 
                "    abbreviation = %s,"
                "    displayName = %s,"
                "    shortDisplayName = %s,"
                "    slug = %s,"
                "    nickname = %s,"
                "    color = %s,"
                "    alternateColor = %s,"
                "    isActive = %s," 
                "    isAllStar = %s,"
                "    hasRecord = %s,"
                "    timeStamp = %s,"
                "    updateId = %s"
                " WHERE year =%s and leagueId = %s and teamId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    # print(sql1)
    if bExist:
        teamTablename = "Teams"
        teamIdList = []
        leagueTablename = "Leagues"
        leagueIdList = []
        #sql_team_insert = ("INSERT INTO Teams VALUES (?,?,?,?,?,"
        #                   "?,?,?,?,?,"
        #                   "?,?,?,?,?);")
        cursor.execute(f"SELECT teamId FROM {teamTablename};")
        rsTeam = cursor.fetchall()
        for row in rsTeam:
            teamIdList.append(row[0])
        print("teamId list length:", len(teamIdList))
        cursor.execute(f"SELECT id FROM {leagueTablename};")
        rsLeague = cursor.fetchall()
        for row in rsLeague:
            leagueIdList.append(row[0])
        print("leagueId list length:", len(leagueIdList))
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            # print(msg)
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(tuple(row))
                    year = int(row['year'])
                    leagueId = int(row['leagueId'])
                    midsizeLeagueName = str(row['midsizeLeagueName'])
                    if leagueId in leagueIdList:
                        seasonType = int(row['seasonType'])
                        teamId = int(row['id'])
                        uid = str(row['uid'])
                        location = str(row['location'])
                        name = str(row['name'])
                        abbreviation = str(row['abbreviation'])
                        displayName = str(row['displayName'])
                        shortDisplayName = str(row['shortDisplayName'])
                        slug = str(row['slug'])
                        nickname = str(row['nickname'])
                        color = str(row['color'])
                        alternateColor = str(row['alternateColor'])
                        isActive = bool(row['isActive'])
                        isAllStar = bool(row['isAllStar'])
                        hasRecord = bool(row['hasRecord'])
                        timeStamp = row['timeStamp']
                        updateId = row['updateId']
                        if teamId not in teamIdList:
                            print('insert team', tuple(row))
                            # print(sql_team_insert)
                            cursor.execute(sql_team_insert, tuple([teamId,
                                                                   uid,
                                                                   location,
                                                                   name,
                                                                   abbreviation,
                                                                   displayName,
                                                                   shortDisplayName,
                                                                   color,
                                                                   alternateColor,
                                                                   isActive,
                                                                   "",
                                                                   0,
                                                                   timeStamp,
                                                                   slug,
                                                                   updateId]))
                            teamIdList.append(teamId)
                        #sql1 = f"""SELECT midsizeLeagueName, seasonType, uid,
                        #                    location, name,
                        #                    abbreviation, displayName, shortDisplayName, slug, nickname,
                        #                    color, alternateColor,
                        #                    isActive, isAllStar, hasRecord
                        #            FROM {tablename}
                        #            WHERE year={year} and leagueId={leagueId} and teamId={teamId};
                        #        """
                        # print(sql1)
                        val = (year,leagueId,teamId)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            rs = cursor.fetchone()
                            midsizeLeagueNameOld = str(rs[0])
                            seasonTypeOld = int(rs[1])
                            uidOld = str(rs[2])
                            locationOld = str(rs[3])
                            nameOld = str(rs[4])
                            abbreviationOld = str(rs[5])
                            displayNameOld = str(rs[6])
                            shortDisplayNameOld = str(rs[7])
                            slugOld = str(rs[8])
                            nicknameOld = str(rs[9])
                            colorOld = str(rs[10])
                            alternateColorOld = str(rs[11])
                            isActiveOld = bool(rs[12])
                            isAllStarOld = bool(rs[13])
                            hasRecordOld = bool(rs[14])
                            if (midsizeLeagueName != midsizeLeagueNameOld or
                                seasonType != seasonTypeOld or
                                uid != uidOld or
                                location != locationOld or
                                name != nameOld or
                                abbreviation != abbreviationOld or
                                displayName != displayNameOld or
                                shortDisplayName != shortDisplayNameOld or
                                slug != slugOld or
                                nickname != nicknameOld or
                                color != colorOld or
                                alternateColor != alternateColorOld or
                                isActive != isActiveOld or
                                isAllStar != isAllStarOld or
                                hasRecord != hasRecordOld):
                                #sql2 = f"""UPDATE {tablename}
                                #        SET midsizeLeagueName = ?,
                                #            seasonType = ?,
                                #            uid = ?,
                                #            location = ?,
                                #            name = ?,
                                #            abbreviation = ?,
                                #            displayName = ?,
                                #            shortDisplayName = ?,
                                #            slug = ?,
                                #            nickname = ?,
                                #            color = ?,
                                #            alternateColor = ?,
                                #            isActive = ?,
                                #            isAllStar = ?,
                                #            hasRecord = ?,
                                #            timeStamp = ?,
                                #            updateId = ?
                                #        WHERE year =? and leagueId = ? and teamId = ?;
                                #    """
                                # print(sql2)
                                val = (midsizeLeagueName,
                                        seasonType,
                                        uid,
                                        location,
                                        name,
                                        abbreviation,
                                        displayName,
                                        shortDisplayName,
                                        slug,
                                        nickname,
                                        color,
                                        alternateColor,
                                        isActive,
                                        isAllStar,
                                        hasRecord,
                                        timeStamp,
                                        updateId,
                                        year, leagueId, teamId)
                                # print(i, "update", teamId, name, displayName, isActive, updateId)
                                nUpdate += 1
                                cursor.execute(sql2,val)
                                # print(tuple(row))
                                # print(sql)
                            else:
                                # print(i, "skip", teamId, name, displayName, isActive, updateId)
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(i, "insert", teamId, name, displayName, venueId, isActive, updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                    else:
                        print("skipped. leagueId not in DB", leagueId, year,midsizeLeagueName)
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        print("Insert")
        try:
            for i, row in df_records.iterrows():
                nInsert += 1
                year = int(row['year'])
                leagueId = int(row['leagueId'])
                midsizeLeagueName = str(row['midsizeLeagueName'])
                if leagueId in leagueIdList:
                    teamId = int(row['id'])
                    uid = str(row['uid'])
                    location = str(row['location'])
                    name = str(row['name'])
                    abbreviation = str(row['abbreviation'])
                    displayName = str(row['displayName'])
                    shortDisplayName = str(row['shortDisplayName'])
                    slug = str(row['slug'])
                    color = str(row['color'])
                    alternateColor = str(row['alternateColor'])
                    isActive = bool(row['isActive'])
                    timeStamp = row['timeStamp']
                    updateId = row['updateId']
                    if teamId not in teamIdList:
                        print('insert team', tuple(row))
                        print(sql_team_insert)
                        cursor.execute(sql_team_insert, tuple([teamId,
                                                               uid,
                                                               location,
                                                               name,
                                                               abbreviation,
                                                               displayName,
                                                               shortDisplayName,
                                                               color,
                                                               alternateColor,
                                                               isActive,
                                                               "",
                                                               0,
                                                               timeStamp,
                                                               slug,
                                                               updateId]))
                        teamIdList.append(teamId)
                    sql = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                    # print(sql)
                    # print(tuple(row))
                    cursor.execute(sql, tuple(row))
                else:
                    print("skipped. leagueId not in DB", leagueId, year,midsizeLeagueName)
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename, 'transaction rolled back')
            msg = tablename + " insert error:" + str(e)
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
        else:
            print(tablename, 'record inserted successfully')
            conn.commit()
            msg = tablename + " insert complete"
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def standingsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "Teams"
    (bExist, msg) = teamsCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "ntotal = ", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql_team_insert = ("INSERT INTO Teams VALUES (?,?,?,?,?,"
                          "?,?,?,?,?,"
                          "?,?,?,?,?);")
        sql1 = ("SELECT midsizeLeagueName,"
                "            seasonType,"                                     
                "            gamesPlayed,"
                "            losses,"
                "            pointDifferential,"
                "            points,"
                "            pointsAgainst,"
                "            pointsFor,"
                "            streak,"
                "            ties,"
                "            wins,"
                "            awayGamesPlayed,"
                "            awayLosses,"
                "            awayPointsAgainst,"
                "            awayPointsFor,"
                "            awayTies,"
                "            awayWins,"
                "            deductions,"
                "            homeGamesPlayed,"
                "            homeLosses,"
                "            homePointsAgainst,"
                "            homePointsFor,"
                "            homeTies,"
                "            homeWins,"
                "            ppg,"
                "            teamRank,"
                "            rankChange" 
                " FROM " + tablename +
                " WHERE year=? and leagueId=? and teamId=?;")
        sql2 = ("UPDATE " + tablename +
                " SET midsizeLeagueName = ?,"
                "    seasonType = ?,"
                "    gamesPlayed = ?,"
                "    losses = ?,"
                "    pointDifferential = ?,"
                "    points = ?,"
                "    pointsAgainst = ?,"
                "    pointsFor = ?,"
                "    streak = ?,"
                "    ties = ?,"
                "    wins = ?,"
                "    awayGamesPlayed = ?,"
                "    awayLosses = ?,"
                "    awayPointsAgainst = ?,"
                "    awayPointsFor = ?,"
                "    awayTies = ?,"
                "    awayWins = ?,"
                "    deductions = ?,"
                "    homeGamesPlayed = ?,"
                "    homeLosses = ?,"
                "    homePointsAgainst = ?,"
                "    homePointsFor = ?,"
                "    homeTies = ?,"
                "    homeWins = ?,"
                "    ppg = ?,"
                "    teamRank = ?,"
                "    rankChange = ?," 
                "    timeStamp = ?,"
                "    updateId = ?"
                " WHERE year =? and leagueId = ? and teamId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql_team_insert = ("INSERT INTO Teams VALUES (%s,%s,%s,%s,%s,"
                          "%s,%s,%s,%s,%s,"
                          "%s,%s,%s,%s,%s);")
        sql1 = ("SELECT midsizeLeagueName,"
                "            seasonType,"                                     
                "            gamesPlayed,"
                "            losses,"
                "            pointDifferential,"
                "            points,"
                "            pointsAgainst,"
                "            pointsFor,"
                "            streak,"
                "            ties,"
                "            wins,"
                "            awayGamesPlayed,"
                "            awayLosses,"
                "            awayPointsAgainst,"
                "            awayPointsFor,"
                "            awayTies,"
                "            awayWins,"
                "            deductions,"
                "            homeGamesPlayed,"
                "            homeLosses,"
                "            homePointsAgainst,"
                "            homePointsFor,"
                "            homeTies,"
                "            homeWins,"
                "            ppg,"
                "            teamRank,"
                "            rankChange" 
                " FROM " + tablename +
                " WHERE year=%s and leagueId=%s and teamId=%s;")
        sql2 = ("UPDATE " + tablename +
                " SET midsizeLeagueName = %s,"
                "    seasonType = %s,"
                "    gamesPlayed = %s,"
                "    losses = %s,"
                "    pointDifferential = %s,"
                "    points = %s,"
                "    pointsAgainst = %s,"
                "    pointsFor = %s,"
                "    streak = %s,"
                "    ties = %s,"
                "    wins = %s,"
                "    awayGamesPlayed = %s,"
                "    awayLosses = %s,"
                "    awayPointsAgainst = %s,"
                "    awayPointsFor = %s,"
                "    awayTies = %s,"
                "    awayWins = %s,"
                "    deductions = %s,"
                "    homeGamesPlayed = %s,"
                "    homeLosses = %s,"
                "    homePointsAgainst = %s,"
                "    homePointsFor = %s,"
                "    homeTies = %s,"
                "    homeWins = %s,"
                "    ppg = %s,"
                "    teamRank = %s,"
                "    rankChange = %s," 
                "    timeStamp = %s,"
                "    updateId = %s"
                " WHERE year =%s and leagueId = %s and teamId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        teamTablename = "Teams"
        teamIdList = []
        leagueTablename = "Leagues"
        leagueIdList = []
        #sql_team_insert = ("INSERT INTO Teams VALUES (?,?,?,?,?,"
        #                   "?,?,?,?,?,"
        #                   "?,?,?,?,?);")
        cursor.execute(f"SELECT teamId FROM {teamTablename};")
        rsTeam = cursor.fetchall()
        for row in rsTeam:
            teamIdList.append(row[0])
        print("teamId list length:", len(teamIdList))
        cursor.execute(f"SELECT id FROM {leagueTablename};")
        rsLeague = cursor.fetchall()
        for row in rsLeague:
            leagueIdList.append(row[0])
        print("leagueId list length:", len(leagueIdList))
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            # print(msg)
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(tuple(row))
                    year = int(row['year'])
                    leagueId = int(row['leagueId'])
                    midsizeLeagueName = str(row['midsizeLeagueName'])
                    if leagueId in leagueIdList:
                        seasonType = int(row['seasonType'])
                        teamId = int(row['teamId'])
                        gamesPlayed = int(row['gamesPlayed'])
                        losses = int(row['losses'])
                        pointDifferential = int(row['pointDifferential'])
                        points = int(row['points'])
                        pointsAgainst = int(row['pointsAgainst'])
                        pointsFor = int(row['pointsFor'])
                        streak = int(row['streak'])
                        ties = int(row['ties'])
                        wins = int(row['wins'])
                        awayGamesPlayed = int(row['awayGamesPlayed'])
                        awayLosses = int(row['awayLosses'])
                        awayPointsAgainst = int(row['awayPointsAgainst'])
                        awayPointsFor = int(row['awayPointsFor'])
                        awayTies = int(row['awayTies'])
                        awayWins = int(row['awayWins'])
                        deductions = int(row['deductions'])
                        homeGamesPlayed = int(row['homeGamesPlayed'])
                        homeLosses = int(row['homeLosses'])
                        homePointsAgainst = int(row['homePointsAgainst'])
                        homePointsFor = int(row['homePointsFor'])
                        homeTies = int(row['homeTies'])
                        homeWins = int(row['homeWins'])
                        ppg = int(row['ppg'])
                        teamRank = int(row['rank'])
                        rankChange = int(row['rankChange'])
                        timeStamp = row['timeStamp']
                        updateId = int(row['updateId'])
                        if teamId not in teamIdList:
                            print('insert team', tuple(row))
                            print(sql_team_insert)
                            cursor.execute(sql_team_insert, tuple([teamId,
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   "",
                                                                   True,
                                                                   "",
                                                                   0,
                                                                   timeStamp,
                                                                   "",
                                                                   updateId]))
                            teamIdList.append(teamId)
                        #sql1 = f"""SELECT midsizeLeagueName,
                        #                    seasonType,
                        #                    gamesPlayed,
                        #                    losses,
                        #                    pointDifferential,
                        #                    points,
                        #                    pointsAgainst,
                        #                    pointsFor,
                        #                    streak,
                        #                    ties,
                        #                    wins,
                        #                    awayGamesPlayed,
                        #                    awayLosses,
                        #                    awayPointsAgainst,
                        #                    awayPointsFor,
                        #                    awayTies,
                        #                    awayWins,
                        #                    deductions,
                        #                    homeGamesPlayed,
                        #                    homeLosses,
                        #                    homePointsAgainst,
                        #                    homePointsFor,
                        #                    homeTies,
                        #                    homeWins,
                        #                    ppg,
                        #                    teamRank,
                        #                    rankChange
                        #            FROM {tablename}
                        #            WHERE year={year} and leagueId={leagueId} and teamId={teamId};
                        "        """
                        # print(sql1)
                        val = (year,leagueId,teamId)
                        cursor.execute(sql1, val)
                        if cursor.rowcount == 1:
                            # print(rs)
                            rs = cursor.fetchone()
                            midsizeLeagueNameOld = str(rs[0])
                            seasonTypeOld = int(rs[1])
                            gamesPlayedOld = int(rs[2])
                            lossesOld = int(rs[3])
                            pointDifferentialOld = int(rs[4])
                            pointsOld = int(rs[5])
                            pointsAgainstOld = int(rs[6])
                            pointsForOld = int(rs[7])
                            streakOld = int(rs[8])
                            tiesOld = int(rs[9])
                            winsOld = int(rs[10])
                            awayGamesPlayedOld = int(rs[11])
                            awayLossesOld = int(rs[12])
                            awayPointsAgainstOld = int(rs[13])
                            awayPointsForOld = int(rs[14])
                            awayTiesOld = int(rs[15])
                            awayWinsOld = int(rs[16])
                            deductionsOld = int(rs[17])
                            homeGamesPlayedOld = int(rs[18])
                            homeLossesOld = int(rs[19])
                            homePointsAgainstOld = int(rs[20])
                            homePointsForOld = int(rs[21])
                            homeTiesOld = int(rs[22])
                            homeWinsOld = int(rs[23])
                            ppgOld = int(rs[24])
                            teamRankOld = int(rs[25])
                            rankChangeOld = int(rs[26])
                            if (midsizeLeagueName != midsizeLeagueNameOld or
                                    seasonType != seasonTypeOld or
                                    gamesPlayed != gamesPlayedOld or
                                    losses != lossesOld or
                                    pointDifferential != pointDifferentialOld or
                                    points != pointsOld or
                                    pointsAgainst != pointsAgainstOld or
                                    pointsFor != pointsForOld or
                                    streak != streakOld or
                                    ties != tiesOld or
                                    wins != winsOld or
                                    awayGamesPlayed != awayGamesPlayedOld or
                                    awayLosses != awayLossesOld or
                                    awayPointsAgainst != awayPointsAgainstOld or
                                    awayPointsFor != awayPointsForOld or
                                    awayTies != awayTiesOld or
                                    awayWins != awayWinsOld or
                                    deductions != deductionsOld or
                                    homeGamesPlayed != homeGamesPlayedOld or
                                    homeLosses != homeLossesOld or
                                    homePointsAgainst != homePointsAgainstOld or
                                    homePointsFor != homePointsForOld or
                                    homeTies != homeTiesOld or
                                    homeWins != homeWinsOld or
                                    ppg != ppgOld or
                                    teamRank != teamRankOld or
                                    rankChange != rankChangeOld):
                                #sql2 = f"""UPDATE {tablename}
                                #        SET midsizeLeagueName = ?,
                                #            seasonType = ?,
                                #            gamesPlayed = ?,
                                #            losses = ?,
                                #            pointDifferential = ?,
                                #            points = ?,
                                #            pointsAgainst = ?,
                                #            pointsFor = ?,
                                #            streak = ?,
                                #            ties = ?,
                                #            wins = ?,
                                #            awayGamesPlayed = ?,
                                #            awayLosses = ?,
                                #            awayPointsAgainst = ?,
                                #            awayPointsFor = ?,
                                #            awayTies = ?,
                                #            awayWins = ?,
                                #            deductions = ?,
                                #            homeGamesPlayed = ?,
                                #            homeLosses = ?,
                                #            homePointsAgainst = ?,
                                #            homePointsFor = ?,
                                #            homeTies = ?,
                                #            homeWins = ?,
                                #            ppg = ?,
                                #            teamRank = ?,
                                #            rankChange = ?,
                                #            timeStamp = ?,
                                #            updateId = ?
                                #        WHERE year =? and leagueId = ? and teamId = ?;
                                #    """
                                # print(sql2)
                                val = (midsizeLeagueName,
                                           seasonType,
                                           gamesPlayed,
                                           losses,
                                           pointDifferential,
                                           points,
                                           pointsAgainst,
                                           pointsFor,
                                           streak,
                                           ties,
                                           wins,
                                           awayGamesPlayed,
                                           awayLosses,
                                           awayPointsAgainst,
                                           awayPointsFor,
                                           awayTies,
                                           awayWins,
                                           deductions,
                                           homeGamesPlayed,
                                           homeLosses,
                                           homePointsAgainst,
                                           homePointsFor,
                                           homeTies,
                                           homeWins,
                                           ppg,
                                           teamRank,
                                           rankChange,
                                           timeStamp,
                                           updateId,
                                           year, leagueId, teamId)
                                # print(i, "update", teamId, name, displayName, isActive, updateId)
                                nUpdate += 1
                                cursor.execute(sql2,val)
                                # print(tuple(row))
                                # print(sql)
                            else:
                                # print(i, "skip", teamId, name, displayName, isActive, updateId)
                                msg = "skip"
                                nSkip += 1
                        else:
                            # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                            # print(sql3)
                            # print(tuple(row))
                            # print(sql)
                            # print(i, "insert", teamId, name, displayName, venueId, isActive, updateId)
                            nInsert += 1
                            cursor.execute(sql3, tuple(row))
                    else:
                        print("skipped. leagueId not in DB", leagueId, year,midsizeLeagueName)
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        print("Insert")
        try:
            for i, row in df_records.iterrows():
                nInsert += 1
                year = int(row['year'])
                leagueId = int(row['leagueId'])
                midsizeLeagueName = str(row['midsizeLeagueName'])
                if leagueId in leagueIdList:
                    teamId = int(row['teamId'])
                    timeStamp = row['timeStamp']
                    updateId = row['updateId']
                    if teamId not in teamIdList:
                        print('insert team', tuple(row))
                        print(sql_team_insert)
                        cursor.execute(sql_team_insert, tuple([teamId,
                                                               "",
                                                               "",
                                                               "",
                                                               "",
                                                               "",
                                                               "",
                                                               "",
                                                               "",
                                                               True,
                                                               "",
                                                               0,
                                                               timeStamp,
                                                               "",
                                                               updateId]))
                        teamIdList.append(teamId)
                    #sql = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                    # print(sql)
                    # print(tuple(row))
                    cursor.execute(sql3, tuple(row))
                else:
                    print("skipped. leagueId not in DB", leagueId, year,midsizeLeagueName)
        except Exception as e:
            conn.rollback()
            print(e)
            print(tablename, 'transaction rolled back')
            msg = tablename + " insert error:" + str(e)
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
        else:
            print(tablename, 'record inserted successfully')
            conn.commit()
            msg = tablename + " insert complete"
            currentTime = datetime.now(timezone.utc)
            errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playerDBTMInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "PlayerDB"
    (bExist, msg) = playerDBCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT first_name,"
                   "    last_name,"
                   "    name,"
                   "    last_season,"
                   "    current_club_id,"
                   "    player_code,"
                   "    country_of_birth,"
                   "    city_of_birth,"
                   "    country_of_citizenship,"
                   "    date_of_birth,"
                   "    sub_position,"
                   "    position,"
                   "    foot,"
                   "    height_in_cm,"
                   "    contract_expiration_date,"
                   "    agent_name,"
                   "    image_url,"
                   "    url,"
                   "    current_club_domestic_competition_id,"
                   "    current_club_name,"
                   "    market_value_in_eur,"
                   "    highest_market_value_in_eur"
                   " FROM " + tablename +
                   " WHERE player_id = ?;")
        sql2 = (
                   "UPDATE PlayerDBTM" 
                   " SET " 
                       "first_name=?,"
                       "last_name=?,"
                       "name = ?,"
                       "last_season  = ?,"
                       "current_club_id = ?,"
                       "player_code = ?,"
                       "country_of_birth = ?,"
                       "city_of_birth = ?,"
                       "country_of_citizenship  = ?,"
                       "date_of_birth = ?,"
                       "sub_position = ?,"
                       "position = ?,"
                       "foot = ?,"
                       "height_in_cm = ?,"
                       "contract_expiration_date = ?,"
                       "agent_name = ?,"
                       "image_url = ?,"
                       "url = ?,"
                       "current_club_domestic_competition_id = ?,"
                       "current_club_name = ?,"
                       "market_value_in_eur = ?,"
                       "highest_market_value_in_eur = ?,"
                       "updateId = ?"
                   " WHERE player_id = ?;"
        )
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT first_name,"
                "    last_name,"
                "    name,"
                "    last_season,"
                "    current_club_id,"
                "    player_code,"
                "    country_of_birth,"
                "    city_of_birth,"
                "    country_of_citizenship,"
                "    date_of_birth,"
                "    sub_position,"
                "    position,"
                "    foot,"
                "    height_in_cm,"
                "    contract_expiration_date,"
                "    agent_name,"
                "    image_url,"
                "    url,"
                "    current_club_domestic_competition_id,"
                "    current_club_name,"
                "    market_value_in_eur,"
                "    highest_market_value_in_eur"
                " FROM " + tablename +
                " WHERE player_id = %s;")
        sql2 = (
            "UPDATE PlayerDBTM"
            " SET "
            "first_name=%s,"
            "last_name=%s,"
            "name = %s,"
            "last_season  = %s,"
            "current_club_id = %s,"
            "player_code = %s,"
            "country_of_birth = %s,"
            "city_of_birth = %s,"
            "country_of_citizenship  = %s,"
            "date_of_birth = %s,"
            "sub_position = %s,"
            "position = %s,"
            "foot = %s,"
            "height_in_cm = %s,"
            "contract_expiration_date = %s,"
            "agent_name = %s,"
            "image_url = %s,"
            "url = %s,"
            "current_club_domestic_competition_id = %s,"
            "current_club_name = %s,"
            "market_value_in_eur = %s,"
            "highest_market_value_in_eur = %s,"
            "updateId = %s"
            " WHERE player_id = %s;"
        )
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            print(len(df_records.index))
            print(df_records.shape)
            try:
                print(tablename,"Total records=", nTotal)
                n=0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(n,i,tuple(row))
                    player_id = int(row['player_id'])
                    first_name = row['first_name']
                    last_name = row['last_name']
                    name = row['name']
                    last_season = row['last_season']
                    current_club_id = row['current_club_id']
                    player_code = row['player_code']
                    country_of_birth = row['country_of_birth']
                    city_of_birth = row['city_of_birth']
                    country_of_citizenship = row['country_of_citizenship']
                    date_of_birth = row['date_of_birth']
                    sub_position = row['sub_position']
                    position = row['position']
                    foot = row['foot']
                    height_in_cm = row['height_in_cm']
                    contract_expiration_date = row['contract_expiration_date']
                    agent_name = row['agent_name']
                    image_url = row['image_url']
                    url = row['url']
                    current_club_domestic_competition_id = row['current_club_domestic_competition_id']
                    current_club_name = row['current_club_name']
                    market_value_in_eur = row['market_value_in_eur']
                    highest_market_value_in_eur = row['highest_market_value_in_eur']
                    updateId = row['updateId']
                    val = (player_id,)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        rs = cursor.fetchone()
                        # print(rs)
                        first_name_old = rs[0]
                        last_name_old = rs[1]
                        name_old = rs[2]
                        last_season_old = rs[3]
                        current_club_id_old = rs[4]
                        player_code_old = rs[5]
                        country_of_birth_old = rs[6]
                        city_of_birth_old = rs[7]
                        country_of_citizenship_old = rs[8]
                        date_of_birth_old = rs[9]
                        sub_position_old = rs[10]
                        position_old = rs[11]
                        foot_old = rs[12]
                        height_in_cm_old = rs[13]
                        contract_expiration_date_old = rs[14]
                        if contract_expiration_date_old is not None:
                            contract_expiration_date_old = contract_expiration_date_old.replace(tzinfo=tz.gettz("UTC"))
                        agent_name_old = rs[15]
                        image_url_old = rs[16]
                        url_old = rs[17]
                        current_club_domestic_competition_id_old = rs[18]
                        current_club_name_old = rs[19]
                        market_value_in_eur_old = rs[20]
                        highest_market_value_in_eur_old = rs[21]
                        # print(tuple(row))
                        # print(rs)
                        """
                        print(1, first_name != first_name_old)
                        print(2, last_name != last_name_old)
                        print(3, name != name_old)
                        print(4, last_season != last_season_old)
                        print(5, current_club_id != current_club_id_old)
                        print(6, player_code != player_code_old)
                        print(7, country_of_birth != country_of_birth_old)
                        print(8, city_of_birth != city_of_birth_old)
                        print(9, country_of_citizenship != country_of_citizenship_old)
                        print(10,date_of_birth != date_of_birth_old)
                        print(11,sub_position != sub_position_old)
                        print(12,position != position_old)
                        print(13,foot != foot_old)
                        print(14,height_in_cm != height_in_cm_old)
                        print(15,contract_expiration_date, contract_expiration_date_old)
                        print(16,agent_name != agent_name_old)
                        print(17,image_url != image_url_old)
                        print(18,url != url_old)
                        print(19,current_club_domestic_competition_id != current_club_domestic_competition_id_old)
                        print(20,current_club_name, current_club_name_old)
                        print(21,market_value_in_eur, market_value_in_eur_old)
                        print(22,highest_market_value_in_eur, highest_market_value_in_eur_old)
                        """
                        if (first_name != first_name_old or
                                last_name != last_name_old or
                                name != name_old or
                                last_season != last_season_old or
                                current_club_id != current_club_id_old or
                                player_code != player_code_old or
                                country_of_birth != country_of_birth_old or
                                city_of_birth != city_of_birth_old or
                                country_of_citizenship != country_of_citizenship_old or
                                date_of_birth != date_of_birth_old or
                                sub_position != sub_position_old or
                                position != position_old or
                                foot != foot_old or
                                height_in_cm != height_in_cm_old or
                                contract_expiration_date != contract_expiration_date_old or
                                agent_name != agent_name_old or
                                image_url != image_url_old or
                                url != url_old or
                                current_club_domestic_competition_id != current_club_domestic_competition_id_old or
                                current_club_name != current_club_name_old or
                                market_value_in_eur != market_value_in_eur_old or
                                highest_market_value_in_eur != highest_market_value_in_eur_old):
                            """
                            print(1, uid != uidOld)
                            print(2, guid != guidOld)
                            print(3, firstName != firstNameOld)
                            print(4, lastName != lastNameOld)
                            print(5, fullName != fullNameOld)
                            print(6, displayName != displayNameOld)
                            print(7, shortName != shortNameOld)
                            print(8, weight != weightOld)
                            print(9, displayWeight != displayWeightOld)
                            print(10, height != heightOld)
                            print(11, displayHeight != displayHeightOld)
                            print(12, age != ageOld)
                            print(13, dateOfBirth != dateOfBirthOld)
                            print(14, gender != genderOld)
                            print(15, citizenship != citizenshipOld)
                            print(15, citizenship,citizenshipOld)
                            print(16, slug != slugOld)
                            print(17, jersey, jerseyOld, jersey != jerseyOld)
                            print(18, status != statusOld)
                            print(19, profiled != profiledOld)
                            print(20, timestamp != timestampOld)
                            print(21, birthPlaceCountry != birthPlaceCountryOld)
                            print(22, citizenshipCountryAltId != citizenshipCountryAltIdOld)
                            print(22, citizenshipCountryAltId, citizenshipCountryAltIdOld)
                            print(23, citizenshipCountryAbbr != citizenshipCountryAbbrOld)
                            print(24, birthCountryAltId != birthCountryAltIdOld)
                            print(24, birthCountryAltId, birthCountryAltIdOld)
                            print(25, birthCountryAbbr != birthCountryAbbrOld)
                            print(26, flagHref != flagHrefOld)
                            print(27, flagAlt != flagAltOld)
                            print(28, positionId != positionIdOld)
                            print(29, positionName != positionNameOld)
                            print(30, positionDisplayName != positionDisplayNameOld)
                            print(31, positionAbbr != positionAbbrOld)
                            print(32, middleName != middleNameOld)
                            print(33, headshotHref != headshotHrefOld)
                            print(34, headshotAlt != headshotAltOld)
                            print(35, birthPlaceCity != birthPlaceCityOld)
                            print(36, nickname != nicknameOld)
                            """
                            update_tuple = (
                                            first_name,
                                            last_name,
                                            name,
                                            last_season,
                                            current_club_id,
                                            player_code,
                                            country_of_birth,
                                            city_of_birth,
                                            country_of_citizenship,
                                            date_of_birth,
                                            sub_position,
                                            position,
                                            foot,
                                            height_in_cm,
                                            contract_expiration_date,
                                            agent_name,
                                            image_url,
                                            url,
                                            current_club_domestic_competition_id,
                                            current_club_name,
                                            market_value_in_eur,
                                            highest_market_value_in_eur,
                                            updateId,
                                            player_id)
                            # print(sql2)
                            # print(update_tuple)
                            # print(n, "update", playerId, fullName, displayHeight,
                            #      timestamp, timestampOld, timestamp == timestampOld, gender, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,update_tuple)
                        else:
                            # print(i, "skip", playerId, fullName,displayName, timestamp)
                            # print(i, tuple(row))
                            msg = "skip"
                            nSkip += 1
                    else:
                        #sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(n, "insert", playerId, fullName, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId':updateId,'table':tablename,'time':currentTime, 'msg':msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)
def playerIdTMInsertRecordSQL(osStr, conn, cursor, tablename, df_records):
    # tablename = "seasonType"
    (bExist, msg) = seasonTypeCreateTableSQL(cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename,"Total records=", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT player_id_TM, fuzzyScore"
                " FROM "+ tablename +
                " WHERE playerId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET player_id_TM=?,"
                "fuzzyScore=?,"
                "updateId=?"
                " WHERE playerId=?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT player_id_TM,fuzzyScore"
                " FROM " + tablename +
                " WHERE playerId = %s;")
        sql2 = ("UPDATE " + tablename +
                " SET player_id_TM = %s,"
                "fuzzyScore= %s,"
                "updateId= %s"
                " WHERE playerId= %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    # print(row)
                    n += 1
                    playerId = int(row['playerId'])
                    player_id_TM = int(row['player_id_TM'])
                    fuzzyScore = int(row['fuzzyScore'])
                    updateId = int(row['updateId'])
                    val = (playerId,)
                    # print('val1',val)
                    cursor.execute(sql1,val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        player_id_TM_old = int(rs[0])
                        fuzzyScore_old = int(rs[1])
                        if (player_id_TM != player_id_TM_old or
                            fuzzyScore != fuzzyScore_old):
                            val = (player_id_TM, fuzzyScore, updateId, playerId)
                            # print(sql2)
                            # print(i, "update", year, typeId, name, slug, updateId)
                            nUpdate += 1
                            # print('val2', val)
                            cursor.execute(sql2,val)
                        # print(tuple(row))
                        # print(sql)
                        else:
                            # print(i, "skip", year, typeId, name, slug, updateId)
                            msg=("skip")
                            nSkip += 1
                    else:
                        #sql3 = f"""INSERT INTO {tablename}
                        #            VALUES {year},{typeId},\"{name}\",\"{slug}\",{updateId};
                        #        """
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(tuple(row))
                        # print(sql)
                        nInsert += 1
                        # print(i, "insert", year, typeId, name, slug, updateId)
                        val = tuple(row)
                        # print('val3', val)
                        cursor.execute(sql3, val)
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                msg = tablename + " update error:" + str(e)
                print(val)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                currentTime = datetime.now(timezone.utc)
                msg = tablename + " update complete"
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)

def insertRecordSQL(osStr,conn, cursor, tablename, df_records):
    currentTime = datetime.now(timezone.utc)
    print(currentTime)
    nTotal = df_records[df_records.columns[0]].count()
    # print(row)
    nCol = len(df_records.axes[1])
    print("column count", nCol)
    if osStr == "Windows":
        sql = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    try:
        n = 0
        for i, row in df_records.iterrows():
            n += 1
            if int(n / 1000) * 1000 == n or n == nTotal:
                print(tablename, "Processsed", i + 1, "out of", nTotal)
            # print(tuple(row))
            # print(sql)
            cursor.execute(sql, tuple(row))
    except Exception as e:
        conn.rollback()
        print(e)
        print(tablename, 'transaction rolled back')
        print(tuple(row))
        msg = e
    else:
        print(tablename, 'record inserted successfully')
        conn.commit()
        msg = tablename + " insert complete"
        return (msg)
    return (msg)
def insertRecordFromDictSQL(osStr,conn, cursor, tablename, records_dict, updateId):
    currentTime = datetime.now(timezone.utc)
    print(currentTime)
    nTotal = len(records_dict.keys())
    # print(row)
    nCol = 3
    print("column count", nCol)
    if osStr == "Windows":
        sql = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    try:
        n = 0
        for key in records_dict.keys():
            n += 1
            if int(n / 1000) * 1000 == n or n == nTotal:
                print(tablename, "Processsed", n, "out of", nTotal)
            # print(tuple(row))
            # print(sql)
            val = (key,records_dict[key],updateId)
            cursor.execute(sql, val)
    except Exception as e:
        conn.rollback()
        print(e)
        print('val', val)
        print(tablename, 'transaction rolled back')
        msg = e
    else:
        print(tablename, 'record inserted successfully')
        conn.commit()
        msg = tablename + " insert complete"
        return (msg)
    return (msg)
def rosterAppearanceByTeam(osStr, conn, cursor, tablename):
    currentTime = datetime.now(timezone.utc)
    print(currentTime)

    # Define the query
    query = """
    SELECT z.*
    FROM (
        SELECT 
        row_number() OVER (PARTITION BY y.Id, y.year ORDER BY y.games DESC) AS Rn, 
        y.Id, y.player, y.games, y.starter, y.year, y.team, teamId, y.midsizeName, y.leagueId
        FROM (
            SELECT 
            MAX(Id) AS Id, 
            MAX(player) AS player,
            SUM(n) AS games, 
            SUM(starter) AS starter,
            year, 
            teamId, 
            MAX(team) AS team, 
            MAX(midsizeName) AS midsizeName,
            MAX(leagueId) AS leagueId
            FROM (
                SELECT 
                MAX(r.athleteDisplayName) AS player,
                MAX(r.athleteId) AS Id, 
                MAX(t.teamId) AS teamId, 
                MAX(t.name) AS team,
                SUM(r.starter) AS starter,
                s.year AS year, 
                COUNT(r.eventId) AS n, 
                s.name AS seasonName,
                MAX(l.id) AS leagueId,
                MAX(l.midsizeName) AS midsizeName
                FROM excel4soccer.TeamRoster r
                INNER JOIN Fixtures f ON f.eventId = r.eventId
                INNER JOIN SeasonType s ON s.typeId = f.seasonType
                INNER JOIN Leagues l ON l.id = f.leagueId
                INNER JOIN Teams t ON t.teamId = r.teamId
                GROUP BY r.athleteId, s.year, s.typeId, t.teamId
            ) x
            GROUP BY Id, year, leagueId, teamId
            ORDER BY Id, year
        ) y
        ORDER BY y.year
    ) z
    WHERE teamId = 360 AND year = 2024
    ORDER BY z.year, Id, z.Rn;
    """

    # Execute the query
    df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return(df)
def eventSnapshotsCreateTableSQL(conn,dbcur, tablename):
    # tablename = "EventSnapshots"
    bExist = checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename + " already exists"
    else:
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            eventErr INT,
            snapshotTime TIMESTAMP,
            hasHeader BIT,
            seasonYear INT,
            seasonType INT,
            seasonName VARCHAR(255),
            hasCompetitions BIT,
            matchDate DATETIME,
            hasStatus BIT,
            statusId INT,
            statusName VARCHAR(255),
            competitors INT,
            homeTeamId INT,
            homeTeamName VARCHAR(255),
            homeTeamScore INT,
            homeTeamRecord INT,
            awayTeamId INT,
            awayTeamName VARCHAR(255),
            awayTeamScore INT,
            awayTeamRecord INT,
            details INT,
            leagueId INT,
            leagueName VARCHAR(255),
            midsizeName VARCHAR(40),
            hasBoxscore BIT,
            nHomeStats INT,
            nAwayStats INT,
            hasGameInfo BIT,
            hasOdds BIt,
            hasRosters BIT,
            nHomePlayers INT,
            nAwayPlayers INT,
            keyEvents INT,
            commentary INT,
            standings INT,
            updateId INT,
            PRIMARY KEY(eventId),
            CONSTRAINT fk_EventSnapshots_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_EventSnapshots_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table ' + tablename
        else:
            conn.commit()
            msg = tablename + ' is created!'
    return (bExist, msg)
def eventSnapshotsInsertRecordSQL(osStr,conn, cursor, tablename, df_records):
    # tablename = "TeamUniform"
    (bExist, msg) = eventSnapshotsCreateTableSQL(conn,cursor, tablename)
    # print(msg)
    nTotal = df_records[df_records.columns[0]].count()
    print(tablename, "ntotal = ", nTotal)
    errMessages = []
    nUpdate = 0
    nInsert = 0
    nSkip = 0
    updateId = df_records.iloc[0]['updateId']
    bInsert = True
    nCol = len(df_records.axes[1])
    if osStr == "Windows":
        sql1 = ("SELECT eventErr,snapshotTime,hasHeader,"
                "        seasonYear,seasonType,seasonName,"
                "        hasCompetitions,matchDate,hasStatus,"
                "        statusId,statusName,competitors,"
                "        homeTeamId,homeTeamName,homeTeamScore,homeTeamRecord,"
                "        awayTeamId,awayTeamName,awayTeamScore,awayTeamRecord,"
                "        details,leagueId,leagueName,midsizeName,"
                "        hasBoxscore,nHomeStats,nAwayStats,hasGameInfo,hasOdds,hasRosters,"
                "        nHOmePlayers,nAwayPlayers,keyEvents,commentary,standings,updateId"
                " FROM " + tablename +
                " WHERE eventId = ?;")
        sql2 = ("UPDATE " + tablename +
                " SET eventErr = ?,"
                "     snapshotTime = ?,"
                "     hasHeader = ?,"
                "     seasonYear = ?,"
                "     seasonType = ?,"
                "     seasonName = ?,"
                "     hasCompetitions = ?,"
                "     matchDate = ?,"
                "     hasStatus = ?,"
                "     statusId = ?,"
                "     statusName = ?,"
                "     competitors = ?,"
                "     homeTeamId = ?,"
                "     homeTeamName = ?,"
                "     homeTeamScore = ?,"
                "     homeTeamRecord = ?,"
                "     awayTeamId = ?,"
                "     awayTeamName = ?,"
                "     awayTeamScore = ?,"
                "     awayTeamRecord = ?,"
                "     details = ?,"
                "     leagueId = ?,"
                "     leagueName = ?,"
                "     midsizeName = ?,"
                "     hasBoxscore = ?,"
                "     nHomeStats = ?,"
                "     nAwayStats = ?,"
                "     hasGameInfo = ?,"
                "     hasOdds = ?,"
                "     hasRosters = ?,"
                "     nHomePlayers = ?,"
                "     nAwayPlayers = ?,"
                "     keyEvents = ?,"
                "     commentary = ?,"
                "     standings = ?,"
                "     updateId = ?"
                " WHERE eventId = ?;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (nCol - 1) + "?)"
    else:
        sql1 = ("SELECT eventErr,snapshotTime,hasHeader,"
                "        seasonYear,seasonType,seasonName,"
                "        hasCompetitions,matchDate,hasStatus,"
                "        statusId,statusName,competitors,"
                "        homeTeamId,homeTeamName,homeTeamScore,homeTeamRecord,"
                "        awayTeamId,awayTeamName,awayTeamScore,awayTeamRecord,"
                "        details,leagueId,leagueName,midsizeName,"
                "        hasBoxscore,nHomeStats,nAwayStats,hasGameInfo,hasOdds,hasRosters,"
                "        nHOmePlayers,nAwayPlayers,keyEvents,commentary,standings,updateId"
                " FROM " + tablename +
                " WHERE eventId = %s")
        sql2 = ("UPDATE " + tablename +
                " SET eventErr = %s,"
                "     snapshotTime = %s,"
                "     hasHeader = %s,"
                "     seasonYear = %s,"
                "     seasonType = %s,"
                "     seasonName = %s,"
                "     hasCompetitions = %s,"
                "     matchDate = %s,"
                "     hasStatus = %s,"
                "     statusId = %s,"
                "     statusName = %s,"
                "     competitors = %s,"
                "     homeTeamId = %s,"
                "     homeTeamName = %s,"
                "     homeTeamScore = %s,"
                "     homeTeamRecord = %s,"
                "     awayTeamId = %s,"
                "     awayTeamName = %s,"
                "     awayTeamScore = %s,"
                "     awayTeamRecord = %s,"
                "     details = %s,"
                "     leagueId = %s,"
                "     leagueName = %s,"
                "     midsizeName = %s,"
                "     hasBoxscore = %s,"
                "     nHomeStats = %s,"
                "     nAwayStats = %s,"
                "     hasGameInfo = %s,"
                "     hasOdds = %s,"
                "     hasRosters = %s,"
                "     nHomePlayers = %s,"
                "     nAwayPlayers = %s,"
                "     keyEvents = %s,"
                "     commentary = %s,"
                "     standings = %s,"
                "     updateId = %s"
                " WHERE eventId = %s;")
        sql3 = "INSERT INTO " + tablename + " VALUES  (" + "%s," * (nCol - 1) + "%s)"
    if bExist:
        cursor.execute(f"SELECT * FROM {tablename} ORDER BY updateID DESC LIMIT 1;")
        if cursor.rowcount > 0:
            msg = "update " + tablename
            bInsert = False
            try:
                n = 0
                for i, row in df_records.iterrows():
                    n += 1
                    # print(row)
                    eventId = row['eventId']
                    eventErr = row['eventErr']
                    snapshotTime = row['snapshotTime']
                    hasHeader = row['hasHeader']
                    seasonYear = row['seasonYear']
                    seasonType = row['seasonType']
                    seasonName = row['seasonName']
                    hasCompetitions = row['hasCompetitions']
                    matchDate = row['matchDate']
                    hasStatus = row['hasStatus']
                    statusId = row['statusId']
                    statusName = row['statusName']
                    competitors = row['competitors']
                    homeTeamId = row['homeTeamId']
                    homeTeamName = row['homeTeamName']
                    homeTeamScore = row['homeTeamScore']
                    homeTeamRecord = row['homeTeamRecord']
                    awayTeamId = row['awayTeamId']
                    awayTeamName = row['awayTeamName']
                    awayTeamScore = row['awayTeamScore']
                    awayTeamRecord = row['awayTeamRecord']
                    details = row['details']
                    leagueId = row['leagueId']
                    leagueName = row['leagueName']
                    midsizeName = row['midsizeName']
                    hasBoxscore = row['hasBoxscore']
                    nHomeStats = row['nHomeStats']
                    nAwayStats = row['nAwayStats']
                    hasGameInfo = row['hasGameInfo']
                    hasOdds = row['hasOdds']
                    hasRosters = row['hasRosters']
                    nHomePlayers = row['nHomePlayers']
                    nAwayPlayers = row['nAwayPlayers']
                    keyEvents = row['keyEvents']
                    commentary = row['commentary']
                    standings = row['standings']
                    updateId = row['updateId']
                    val = (eventId,)
                    cursor.execute(sql1, val)
                    if cursor.rowcount == 1:
                        # print(rs)
                        rs = cursor.fetchone()
                        eventErrOld = rs[0]
                        snapshotTimeOld = rs[1].replace(tzinfo=tz.gettz("UTC"))
                        hasHeaderOld = rs[2]
                        seasonYearOld = rs[3]
                        seasonTypeOld = rs[4]
                        seasonNameOld = rs[5]
                        hasCompetitionsOld = rs[6]
                        matchDateOld = rs[7].replace(tzinfo=tz.gettz("UTC"))
                        hasStatusOld = rs[8]
                        statusIdOld = rs[9]
                        statusNameOld = rs[10]
                        competitorsOld = rs[11]
                        homeTeamIdOld = rs[12]
                        homeTeamNameOld = rs[13]
                        homeTeamScoreOld = rs[14]
                        homeTeamRecordOld = rs[15]
                        awayTeamIdOld = rs[16]
                        awayTeamNameOld = rs[17]
                        awayTeamScoreOld = rs[18]
                        awayTeamRecordOld = rs[19]
                        detailsOld = rs[20]
                        leagueIdOld = rs[21]
                        leagueNameOld = rs[22]
                        midsizeNameOld = rs[23]
                        hasBoxscoreOld = rs[24]
                        nHomeStatsOld = rs[25]
                        nAwayStatsOld = rs[26]
                        hasGameInfoOld = rs[27]
                        hasOddsOld = rs[28]
                        hasRostersOld = rs[29]
                        nHomePlayersOld = rs[30]
                        nAwayPlayersOld = rs[31]
                        keyEventsOld = rs[32]
                        commentaryOld = rs[33]
                        standingsOld = rs[34]
                        if (eventErr != eventErrOld or
                                snapshotTime != snapshotTimeOld or
                                hasHeader != hasHeaderOld or
                                seasonYear != seasonYearOld or
                                seasonType != seasonTypeOld or
                                seasonName != seasonNameOld or
                                hasCompetitions != hasCompetitionsOld or
                                matchDate != matchDateOld or
                                hasStatus != hasStatusOld or
                                statusId != statusIdOld or
                                statusName != statusNameOld or
                                competitors != competitorsOld or
                                homeTeamId != homeTeamIdOld or
                                homeTeamName != homeTeamNameOld or
                                homeTeamScore != homeTeamScoreOld or
                                homeTeamRecord != homeTeamRecordOld or
                                awayTeamId != awayTeamIdOld or
                                awayTeamName != awayTeamNameOld or
                                awayTeamScore != awayTeamScoreOld or
                                awayTeamRecord != awayTeamRecordOld or
                                details != detailsOld or
                                leagueId != leagueIdOld or
                                leagueName != leagueNameOld or
                                midsizeName != midsizeNameOld or
                                hasBoxscore != hasBoxscoreOld or
                                nHomeStats != nHomeStatsOld or
                                nAwayStats != nAwayStatsOld or
                                hasGameInfo != hasGameInfoOld or
                                hasOdds != hasOddsOld or
                                hasRosters != hasRostersOld or
                                nHomePlayers != nHomePlayersOld or
                                nAwayPlayers != nAwayPlayersOld or
                                keyEvents != keyEventsOld or
                                commentary != commentaryOld or
                                standings != standingsOld):
                            """
                            print()
                            print(eventErr == eventErrOld, eventErr, ",", eventErrOld)
                            print(snapshotTime == snapshotTimeOld, snapshotTime, ",", snapshotTimeOld)
                            print(hasHeader == hasHeaderOld, hasHeader, ",", hasHeaderOld)
                            print(seasonYear == seasonYearOld, seasonYear, ",", seasonYearOld)
                            print(seasonType == seasonTypeOld, seasonType, ",", seasonTypeOld)
                            print(seasonName == seasonNameOld, seasonName, ",", seasonNameOld)
                            print(hasCompetitions == hasCompetitionsOld, hasCompetitions, ",", hasCompetitionsOld)
                            print(matchDate == matchDateOld, matchDate, ",", matchDateOld)
                            print(hasStatus == hasStatusOld, hasStatus, ",", hasStatusOld)
                            print(statusId == statusIdOld, statusId, ",", statusIdOld)
                            print(statusName == statusNameOld, statusName, ",", statusNameOld)
                            print(competitors == competitorsOld, competitors, ",", competitorsOld)
                            print(homeTeamId == homeTeamIdOld, homeTeamId, ",", homeTeamIdOld)
                            print(homeTeamName == homeTeamNameOld, homeTeamName, ",", homeTeamNameOld)
                            print(homeTeamScore == homeTeamScoreOld, homeTeamScore, ",", homeTeamScoreOld)
                            print(homeTeamRecord == homeTeamRecordOld, homeTeamRecord, ",", homeTeamRecordOld)
                            print(awayTeamId == awayTeamIdOld, awayTeamId, ",", awayTeamIdOld)
                            print(awayTeamName == awayTeamNameOld, awayTeamName, ",", awayTeamNameOld)
                            print(awayTeamScore == awayTeamScoreOld, awayTeamScore, ",", awayTeamScoreOld)
                            print(awayTeamRecord == awayTeamRecordOld, awayTeamRecord, ",", awayTeamRecordOld)
                            print(details == detailsOld, details, ",", detailsOld)
                            print(leagueId == leagueIdOld, leagueId, ",", leagueIdOld)
                            print(leagueName == leagueNameOld, leagueName, ",", leagueNameOld)
                            print(midsizeName == midsizeNameOld, midsizeName, ",", midsizeNameOld)
                            print(hasBoxscore == hasBoxscoreOld, hasBoxscore, ",", hasBoxscoreOld)
                            print(nHomeStats == nHomeStatsOld, nHomeStats, ",", nHomeStatsOld)
                            print(nAwayStats == nAwayStatsOld, nAwayStats, ",", nAwayStatsOld)
                            print(hasGameInfo == hasGameInfoOld, hasGameInfo, ",", hasGameInfoOld)
                            print(hasOdds == hasOddsOld, hasOdds, ",", hasOddsOld)
                            print(hasRosters == hasRostersOld, hasRosters, ",", hasRostersOld)
                            print(nHomePlayers == nHomePlayersOld, nHomePlayers, ",", nHomePlayersOld)
                            print(nAwayPlayers == nAwayPlayersOld, nAwayPlayers, ",", nAwayPlayersOld)
                            print(keyEvents == keyEventsOld, keyEvents, ",", keyEventsOld)
                            print(commentary == commentaryOld, commentary, ",", commentaryOld)
                            print(standings == standingsOld, standings, ",", standingsOld)
                            print()
                            """
                            val = (eventErr,
                                    snapshotTime,
                                    hasHeader,
                                    seasonYear,
                                    seasonType,
                                    seasonName,
                                    hasCompetitions,
                                    matchDate,
                                    hasStatus,
                                    statusId,
                                    statusName,
                                    competitors,
                                    homeTeamId,
                                    homeTeamName,
                                    homeTeamScore,
                                    homeTeamRecord,
                                    awayTeamId,
                                    awayTeamName,
                                    awayTeamScore,
                                    awayTeamRecord,
                                    details,
                                    leagueId,
                                    leagueName,
                                    midsizeName,
                                    hasBoxscore,
                                    nHomeStats,
                                    nAwayStats,
                                    hasGameInfo,
                                    hasOdds,
                                    hasRosters,
                                    nHomePlayers,
                                    nAwayPlayers,
                                    keyEvents,
                                    commentary,
                                    standings,
                                    updateId,
                                   eventId)
                            # print(i, "update", eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                            #            uniformColor, uniformAlternateColor, updateId)
                            nUpdate += 1
                            cursor.execute(sql2,val)
                            # print(tuple(row))
                            # print(sql)
                        else:
                            msg = ("skip")
                            nSkip += 1
                    else:
                        # sql3 = "INSERT INTO " + tablename + " VALUES  (" + "?," * (len(row) - 1) + "?)"
                        # print(sql3)
                        # print(tuple(row))
                        # print(sql)
                        # print(i, "insert", eventId, teamId, teamOrder, teamColor, teamAlternateColor,
                        #       uniformColor, uniformAlternateColor, updateId)
                        nInsert += 1
                        cursor.execute(sql3, tuple(row))
                    if int(n / 1000) * 1000 == n or n == nTotal:
                        print(tablename, "Processsed", n, "out of", nTotal,
                              "updated rows:", nUpdate, "inserted rows:", nInsert, "skipped rows:", nSkip)
            except Exception as e:
                conn.rollback()
                print(e)
                print(tablename, 'transaction rolled back')
                print(n)
                print(tuple(row))
                msg = tablename + " update error:" + str(e)
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
            else:
                print(tablename, 'record inserted successfully')
                conn.commit()
                msg = tablename + " update complete"
                currentTime = datetime.now(timezone.utc)
                errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                                    'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
                return (errMessages)
    if bInsert:  # insert rows into Table
        msg = insertRecordSQL(osStr,conn, cursor, tablename, df_records)
        currentTime = datetime.now(timezone.utc)
        errMessages.append({'updateId': updateId, 'table': tablename, 'time': currentTime, 'msg': msg,
                            'nUpdate': nUpdate, 'nInsert': nInsert, 'nSkip': nSkip, 'nTotal': nTotal})
    return (errMessages)

def importJsonToDf(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    df = pd.json_normalize(tableJson)
    return df


def importJsonToDf2(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    outputTable = []
    for tableKey in tableJson:
        rowJson = tableJson[tableKey]
        outputTable.append(rowJson)

    df = pd.json_normalize(outputTable)
    return (df)


def importJsonToDf3(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    outputTable = []
    i = 0
    for tableKey in tableJson:
        i += 1
        value = tableJson[tableKey]
        outputTable.append({'index': i, 'name': tableKey, 'value': value})

    df = pd.json_normalize(outputTable)
    return (df)
def importCsvToDf(filename):
    # dataframes=[]
    print(filename)
    df = pd.read_csv(filename,sep=",",encoding = "UTF-8")
    print('Data imported')
    return(df)
def db_url(uid, pwd, dbServer, dbName, queryDict):
    conn_url = URL.create(
        "mysql+pyodbc",
        username=uid,
        password=pwd,
        host=dbServer,
        database=dbName,
        query=queryDict
    )
    return (conn_url)

"""
def importJsonToDf(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    df = pd.json_normalize(tableJson)
    return df


def importJsonToDf2(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    outputTable = []
    for tableKey in tableJson:
        rowJson = tableJson[tableKey]
        outputTable.append(rowJson)

    df = pd.json_normalize(outputTable)
    return (df)


def importJsonToDf3(filename):
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        tableJson = [{"error": -1}]
    outputTable = []
    i = 0
    for tableKey in tableJson:
        i += 1
        value = tableJson[tableKey]
        outputTable.append({'index': i, 'name': tableKey, 'value': value})

    df = pd.json_normalize(outputTable)
    return (df)


def db_url(uid, pwd, dbServer, dbName, queryDict):
    conn_url = URL.create(
        "mysql+pyodbc",
        username=uid,
        password=pwd,
        host=dbServer,
        database=dbName,
        query=queryDict
    )
    return (conn_url)
"""
