import json
import pandas as pd
import sqlConn
from datetime import datetime
from dateutil import tz
import numpy as np

def Insert_Status_Season(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/fixtures/'
    directory3 = rootDir2 + 'tables/boxscore/'
    directory4 = rootDir2 + 'tables/gameInfo/'
    directory5 = rootDir2 + 'tables/teams/'
    directory6 = rootDir2 + 'tables/roster/'

    #filename1=directory3+'seasons.json'
    filename1=directory2+'seasons.json'   # moved to fixtures
    filename2=directory2+'statusType.json'
    filename3=directory3+'leagues.json'
    filename4=directory4+'venueTb.json'
    filename5=directory5+'positionDB.json'
    filename6=directory6+'statsTypes.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df_seasonType = sqlConn.importJsonToDf(filename1)
    if not(df_seasonType.empty):
        b1 = True
        # print('df_seasonType')
        # print(df_seasonType.info())
        df_seasonType["year"] = df_seasonType["year"].astype("int")
        df_seasonType["type"] = df_seasonType["type"].astype("int")
    b2 = False
    df_statusType = sqlConn.importJsonToDf2(filename2)
    if not(df_statusType.empty):
        b2 = True
        df_statusType["id"] = df_statusType["id"].astype("int")
        # print('df_statusType')
        # print(df_statusType.info())
    b3 = False
    df_leagues = sqlConn.importJsonToDf(filename3)
    if not(df_leagues.empty):
        b3 = True
        defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        defaultTimeZone = tz.gettz("UTC")
        defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
        # print(defaultTime)

        df_leagues['id'] = df_leagues['id'].astype("int")
        df_leagues['alternateId'] = df_leagues['alternateId'].astype("int")
        df_leagues['seasonTypeId'] = df_leagues['seasonTypeId'].astype("int")
        df_leagues['logoUrl1LastUpdated'] = pd.to_datetime(df_leagues['logoUrl1LastUpdated'],utc=True)
        df_leagues['logoUrl2LastUpdated'] = pd.to_datetime(df_leagues['logoUrl2LastUpdated'],utc=True)
        df_leagues['logoUrl1LastUpdated'] = df_leagues['logoUrl1LastUpdated'].fillna(defaultTime)
        df_leagues['logoUrl2LastUpdated'] = df_leagues['logoUrl2LastUpdated'].fillna(defaultTime)
        df_leagues['updateTime'] = pd.to_datetime(df_leagues['updateTime'],utc=True)
        # print(df_leagues.info())
    b4 = False
    df_venue = sqlConn.importJsonToDf(filename4)
    if not(df_venue.empty):
        b4 = True
        # print(filename4)
        # print("venue")
        # print(df_venue.info()
        df_tmp=pd.DataFrame({"id":[0],
                             "fullName": ["none"],
                             "shortName":["none"],
                             "capacity":[0],
                             "city":["none"],
                             "country":["none"],
                             "address.city":["none"],
                             "address.country":["none"]
                             })
        # print("tmp venue")
        # print(df_tmp.info())
        df_venue = pd.concat([df_tmp,df_venue],ignore_index=True)
        df_venue = df_venue.drop(columns="images")
        df_venue = df_venue.drop(columns="city")
        df_venue = df_venue.drop(columns="country")
        df_venue = df_venue.replace('','none')
        df_venue["id"] = df_venue["id"].astype("int")
        df_venue['shortName'] = df_venue['shortName'].fillna('none').astype("str")
        df_venue['address.city'] = df_venue['address.city'].fillna('none').astype("str")
        df_venue['address.country'] = df_venue['address.country'].fillna('none').astype("str")
        # print('df_venue')
        # print(df_venue.info())
    b5 = False
    df_positionType = sqlConn.importJsonToDf2(filename5)
    if not(df_positionType.empty):
        b5 = True
        df_positionType['id'] = df_positionType['id'].astype("int")
        df_positionType['name'] = df_positionType['name'].astype("str")
        # print('df_positionType')
        # print(df_positionType.info())
    b6 = False
    df_statType = sqlConn.importJsonToDf2(filename6)
    if not(df_statType.empty):
        b6 = True
        if 'name' in df_statType:
            df_statType['name'] = df_statType['name'].astype("str")
        else:
            df_statType['name'] = ""
        # print('df_statType')
        # print(df_statType.info())
    if b1 or b2 or b3 or b4 or b5 or b6:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return("01 Status_Season_Update_Complete", err)
    if b1:
        tableName1 = 'SeasonType'
        (updateId1, updateTime1) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName1,dataSet)
        df_seasonType["updateId"] = updateId1
        # print(tableName1,'updatetime:',updateTime1, 'updateId:',updateId1)
        msg = sqlConn.seasonTypeInsertRecordSQL(osStr, conn, cursor,tableName1,df_seasonType)
        #print(msg)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)
    print()
    if b2:
        tableName2 = 'StatusType'
        (updateId2, updateTime2) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName2,dataSet)
        df_statusType["updateId"] = updateId2
        # print(tableName2,'updatetime:',updateTime2, 'updateId:',updateId2)
        msg = sqlConn.statusTypeInsertRecordSQL(osStr,conn,cursor,tableName2,df_statusType)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)
    print()
    if b3:
        tableName3 = 'Leagues'
        (updateId3, updateTime3) = sqlConn.getUpdateIdSQL(osStr,conn, cursor,tableName3,dataSet)
        df_leagues["updateId"] =updateId3
        # print(tableName3,'updatetime:',updateTime3, 'updateId:',updateId3)
        msg = sqlConn.leaguesInsertRecordSQL(osStr,conn,cursor,tableName3,df_leagues)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)
    print()
    if b4:
        tableName4 = 'Venues'
        (updateId4, updateTime4) = sqlConn.getUpdateIdSQL(osStr,conn, cursor,tableName4,dataSet)
        df_venue["updateId"] =updateId4
        # print(tableName4,'updatetime:',updateTime4, 'updateId:',updateId4)
        msg = sqlConn.venuesInsertRecordSQL(osStr,conn,cursor,tableName4,df_venue)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName4 + " update insertion error"
        else:
            msg = tableName4 + " database insertion successful"
        print(msg)
    print()
    if b5:
        tableName5 = 'PositionType'
        (updateId5, updateTime5) = sqlConn.getUpdateIdSQL(osStr,conn, cursor,tableName5,dataSet)
        df_positionType["updateId"] =updateId5
        print(tableName5,'updatetime:',updateTime5, 'updateId:',updateId5)
        msg = sqlConn.positionTypeInsertRecordSQL(osStr,conn, cursor,tableName5,df_positionType)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName5 + " update insertion error"
        else:
            msg = tableName5 + " database insertion successful"
        print(msg)
    print()
    if b6:
        tableName6 = 'StatType'
        (updateId6, updateTime6) = sqlConn.getUpdateIdSQL(osStr,conn, cursor,tableName6,dataSet)
        df_statType["updateId"] =updateId6
        print(tableName6,'updatetime:',updateTime6, 'updateId:',updateId6)
        msg = sqlConn.statTypeInsertRecordSQL(osStr,conn, cursor,tableName6,df_statType)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName6 + " update insertion error"
        else:
            msg = tableName6 + " database insertion successful"
        print(msg)

    conn.close()
    return("01 Status_Season_Update_Complete", err)

def Insert_Teams(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/boxscore/'
    directory3 = rootDir2 + 'tables/fixtures/'

    filename1=directory2+'teams.json'
    filename2=directory3+'teams.json'

    filename3=directory2+'statLabel.json'
    filename4=directory3+'statLabel.json'
    filename5=directory2+'teamStats.json'
    filename6=directory2+'teamUniform.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df_teams1 = sqlConn.importJsonToDf(filename1)
    if not (df_teams1.empty):
        b1 = True
        df_teams1=df_teams1.rename(
            columns={"id": "teamId"}
        )
        df_teams1['teamId'] = df_teams1['teamId'].astype("int")
        # print("teams1")
        # print(df_teams1.info())
    b2 = False
    df_teams2 = sqlConn.importJsonToDf(filename2)
    if not (df_teams2.empty):
        b2 = True
        df_teams2=df_teams2.rename(
            columns={"id": "teamId"}
        )
        df_teams2['teamId'] = df_teams2['teamId'].astype("int")
        df_teams2['updateTime'] = pd.to_datetime(df_teams2['updateTime'],utc=True)
        # print("teams2")
        # print(df_teams2.info())
    b3 = b1 and b2
    if b3:
        defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        defaultTimeZone = tz.gettz("UTC")
        defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
        # print(defaultTime)

        df_teams_tmp = df_teams1[["teamId", "slug"]]
        df_teams = pd.merge(df_teams2, df_teams_tmp, on="teamId", how="outer").sort_values('teamId')
        # df_teams = df_teams.replace('',np.nan)
        df_teams['uid'] = df_teams['uid'].fillna("").astype("object")
        df_teams['location'] = df_teams['location'].fillna("").astype("object")
        df_teams['name'] = df_teams['name'].fillna("").astype("object")
        df_teams['abbreviation'] = df_teams['abbreviation'].fillna("").astype("object")
        df_teams['displayName'] = df_teams['displayName'].fillna("").astype("object")
        df_teams['shortDisplayName'] = df_teams['shortDisplayName'].fillna("").astype("object")
        df_teams['color'] = df_teams['color'].fillna("").astype("object")
        df_teams['alternateColor'] = df_teams['alternateColor'].fillna("").astype("object")
        df_teams['isActive'] = df_teams['isActive'].fillna(False).astype("bool")
        df_teams['logo'] = df_teams['logo'].fillna("").astype("object")
        df_teams['venueId'] = df_teams['venueId'].fillna(0).astype("int")
        df_teams['updateTime'] = df_teams['updateTime'].fillna(defaultTime)
        df_teams['slug'] = df_teams['slug'].fillna("").astype("object")

        df_teams_cols = ['teamId','uid','location','name','abbreviation', 'displayName', 'shortDisplayName',
                         'color','alternateColor','isActive','logo','venueId','updateTime','slug']
        df_teams=df_teams[df_teams_cols]

        # print("teams")
        # print(df_teams.info())
        # print(df_teams.to_string())
    b4 = False
    df_teamStatName = sqlConn.importJsonToDf3(filename3)
    if not (df_teamStatName.empty):
        b4 = True
        df_teamStatName=df_teamStatName.rename(
            columns={"index": "id",
                     "name": "stat",
                     "value": "name"}
        )
        df_teamStatName["id"] = df_teamStatName["id"].astype("int")
        # print(df_teamStatName.info())
    b5 = False
    df_teamStatName2 = sqlConn.importJsonToDf3(filename4)
    if not (df_teamStatName2.empty):
        b5 = True
        df_teamStatName2=df_teamStatName2.rename(
            columns={"index": "id",
                     "name": "stat",
                     "value": "statAbbreviation"}
        )
        df_teamStatName2["id"] = df_teamStatName2["id"].astype("int")
        # print(df_teamStatName2.info())
    if b3 or b4 or b5:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("02 Teams Complete", err)

    #dataSet="test2"
    if b3:
        tableName1 = 'Teams'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName1,dataSet)
        df_teams["updateId"] =updateId
        msg = sqlConn.teamsInsertRecordSQL(osStr,conn,cursor,tableName1,df_teams)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)
    print()
    if b4:
        tableName2 = 'TeamStatName'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName2,dataSet)
        df_teamStatName["updateId"] = updateId
        msg = sqlConn.teamStatNameInsertRecordSQL(osStr,conn,cursor,tableName2,df_teamStatName)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)
    print()
    if b5:
        tableName3 = 'TeamStatName2'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName3,dataSet)
        df_teamStatName2["updateId"] = updateId
        msg = sqlConn.teamStatName2InsertRecordSQL(osStr,conn,cursor,tableName3,df_teamStatName2)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

    conn.close()
    return("02 Teams Complete", err)
def Insert_Fixtures(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/fixtures/'

    filename=directory2+'fixtures.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df_fixtures = sqlConn.importJsonToDf(filename)
    if not (df_fixtures.empty):
        b1 = True
        #df_fixtures = df_fixtures.replace('',np.nan)
        df_fixtures['eventId'] = df_fixtures['eventId'].astype("int")
        df_fixtures['leagueId'] = df_fixtures['leagueId'].astype("int")
        df_fixtures['statusId'] = df_fixtures['statusId'].astype("int")
        df_fixtures['period'] = df_fixtures['period'].replace('unknown',0)
        df_fixtures['homeTeamScore'] = df_fixtures['homeTeamScore'].astype("int")
        df_fixtures['awayTeamScore'] = df_fixtures['awayTeamScore'].astype("int")
        df_fixtures['homeTeamShootoutScore'] = pd.to_numeric(df_fixtures['homeTeamShootoutScore']).fillna(0)
        df_fixtures['awayTeamShootoutScore'] = pd.to_numeric(df_fixtures['awayTeamShootoutScore']).fillna(0)
        #df_fixtures['homeTeamShootoutScore'] = df_fixtures['homeTeamShootoutScore'].fillna(0).astype("int")
        #df_fixtures['awayTeamShootoutScore'] = df_fixtures['awayTeamShootoutScore'].fillna(0).astype("int")
        df_fixtures['homeTeamId'] = df_fixtures['homeTeamId'].astype("int")
        df_fixtures['awayTeamId'] = df_fixtures['awayTeamId'].astype("int")
        df_fixtures['homeTeamForm'] = df_fixtures['homeTeamForm'].fillna("").astype(object)
        df_fixtures['awayTeamForm'] = df_fixtures['awayTeamForm'].fillna("").astype(object)
        df_fixtures['hasStats'] = df_fixtures['hasStats'].astype("bool")
        df_fixtures['venueId'] = df_fixtures['venueId'].fillna(-1).astype("int")
        df_fixtures['displayClock'] = df_fixtures['displayClock'].fillna("").astype(object)
        df_fixtures = df_fixtures.sort_values('eventId')
        df_fixtures['date'] = pd.to_datetime(df_fixtures['date'])
        df_fixtures['startDate'] = pd.to_datetime(df_fixtures['startDate'],utc=True)
        df_fixtures['updateTime'] = pd.to_datetime(df_fixtures['updateTime'],utc=True)
        # print(df_fixtures.info())
        df_fixtures_col = [
            'eventId',
            'leagueId',
            'uid',
            'attendance',
            'date',
            'startDate',
            'neutralSite',
            'conferenceCompetition',
            'boxscoreAvailable',
            'commentaryAvailable',
            'recent',
            'boxscoreSource',
            'playByPlaySource',
            'seasonType',
            'statusId',
            'clock',
            'displayClock',
            'period',
            'venueId',
            'homeTeamId',
            'homeTeamUid',
            'homeTeamOrder',
            'homeTeamWinner',
            'homeTeamScore',
            'homeTeamShootoutScore',
            'homeTeamForm',
            'awayTeamId',
            'awayTeamUid',
            'awayTeamOrder',
            'awayTeamWinner',
            'awayTeamScore',
            'awayTeamShootoutScore',
            'awayTeamForm',
            'hasStats',
            'homeYellowCard',
            'homeRedCard',
            'awayYellowCard',
            'awayRedCard',
            'updateTime'
        ]
        df_fixtures=df_fixtures[df_fixtures_col]

        inx=df_fixtures['eventId']
        # dup=inx.duplicated(keep='last')
        mask1=inx.duplicated(keep=False) & (df_fixtures["statusId"] == 6)

        # print(df_fixtures[mask1].to_string())

        df_fixtures.drop(df_fixtures[mask1].index, inplace = True)

        # mask1=inx.duplicated(keep=False) & (df_fixtures["statusId"] == 6)
        # print(df_fixtures[mask1].to_string())
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'Fixtures'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor,tableName1,dataSet)
        df_fixtures["updateId"] =updateId
        msg = sqlConn.fixturesInsertRecordSQL(osStr,conn,cursor,tableName1,df_fixtures)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor,msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return ("03 Fixtures Complete", err)
def Insert_TeamStats(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/boxscore/'
    directory3 = rootDir2 + 'tables/fixtures/'

    filename5 = directory2 + 'teamStats.json'
    filename6 = directory2 + 'teamUniform.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df_teamStats = sqlConn.importJsonToDf(filename5)
    if not(df_teamStats.empty):
        b1 = True
        df_teamStats = df_teamStats.replace('', np.nan)
        # dup=inx.duplicated(keep='last')
        mask1 = df_teamStats.duplicated(keep='last')
        df_teamStats.drop(df_teamStats[mask1].index, inplace=True)

        df_teamStats['eventId'] = df_teamStats['eventId'].astype("int")
        df_teamStats['teamId'] = df_teamStats['teamId'].astype("int")
        df_teamStats['hasStats'] = df_teamStats['hasStats'].astype("bool")
        if 'foulsCommitted' in df_teamStats:
            df_teamStats['foulsCommitted'] = df_teamStats['foulsCommitted'].fillna(-1).astype("int")
        else:
            df_teamStats['foulsCommitted'] = -1
        if 'yellowCards' in df_teamStats:
            df_teamStats['yellowCards'] = df_teamStats['yellowCards'].fillna(-1).astype("int")
        else:
            df_teamStats['yellowCards'] = -1
        if 'redCards' in df_teamStats:
            df_teamStats['redCards'] = df_teamStats['redCards'].fillna(-1).astype("int")
        else:
            df_teamStats['redCards'] = -1
        if 'offsides' in df_teamStats:
            df_teamStats['offsides'] = df_teamStats['offsides'].fillna(-1).astype("int")
        else:
            df_teamStats['offsides'] = -1
        if 'wonCorners' in df_teamStats:
            df_teamStats['wonCorners'] = df_teamStats['wonCorners'].fillna(-1).astype("int")
        else:
            df_teamStats['wonCorners'] = -1
        if 'saves' in df_teamStats:
            df_teamStats['saves'] = df_teamStats['saves'].fillna(-1).astype("int")
        else:
            df_teamStats['saves'] = -1
        if 'possessionPct' in df_teamStats:
            df_teamStats['possessionPct'] = df_teamStats['possessionPct'].fillna(-1).astype("float")
        else:
            df_teamStats['possessionPct'] = -1
        if 'totalShots' in df_teamStats:
            df_teamStats['totalShots'] = df_teamStats['totalShots'].fillna(-1).astype("int")
        else:
            df_teamStats['totalShots'] = -1
        if 'shotsOnTarget' in df_teamStats:
            df_teamStats['shotsOnTarget'] = df_teamStats['shotsOnTarget'].fillna(-1).astype("int")
        else:
            df_teamStats['shotsOnTarget'] = -1
        if 'shotPct' not in df_teamStats.columns:
            df_teamStats.insert(14, 'shotPct', -1)
        df_teamStats['shotPct'] = df_teamStats['shotPct'].fillna(-1).astype("float")
        if 'penaltyKickGoals' not in df_teamStats.columns:
            df_teamStats.insert(15, 'penaltyKickGoals', -1)
        df_teamStats['penaltyKickGoals'] = df_teamStats['penaltyKickGoals'].fillna(-1).astype("int")
        if 'penaltyKickShots' not in df_teamStats.columns:
            df_teamStats.insert(16, 'penaltyKickShots', -1)
        df_teamStats['penaltyKickShots'] = df_teamStats['penaltyKickShots'].fillna(-1).astype("int")
        if 'accuratePasses' not in df_teamStats.columns:
            df_teamStats.insert(17, 'accuratePasses', -1)
        df_teamStats['accuratePasses'] = df_teamStats['accuratePasses'].fillna(-1).astype("int")
        if 'totalPasses' not in df_teamStats.columns:
            df_teamStats.insert(18, 'totalPasses', -1)
        df_teamStats['totalPasses'] = df_teamStats['totalPasses'].fillna(-1).astype("int")
        if 'passPct' not in df_teamStats.columns:
            df_teamStats.insert(19, 'passPct', -1)
        df_teamStats['passPct'] = df_teamStats['passPct'].fillna(-1).astype("float")
        if 'accurateCrosses' not in df_teamStats.columns:
            df_teamStats.insert(20, 'accurateCrosses', -1)
        df_teamStats['accurateCrosses'] = df_teamStats['accurateCrosses'].fillna(-1).astype("int")
        if 'totalCrosses' not in df_teamStats.columns:
            df_teamStats.insert(21, 'totalCrosses', -1)
        df_teamStats['totalCrosses'] = df_teamStats['totalCrosses'].fillna(-1).astype("int")
        if 'crossPct' not in df_teamStats.columns:
            df_teamStats.insert(22, 'crossPct', -1)
        df_teamStats['crossPct'] = df_teamStats['crossPct'].fillna(-1).astype("float")
        if 'totalLongBalls' not in df_teamStats.columns:
            df_teamStats.insert(23, 'totalLongBalls', -1)
        df_teamStats['totalLongBalls'] = df_teamStats['totalLongBalls'].fillna(-1).astype("int")
        if 'accurateLongBalls' not in df_teamStats.columns:
            df_teamStats.insert(24, 'accurateLongBalls', -1)
        df_teamStats['accurateLongBalls'] = df_teamStats['accurateLongBalls'].fillna(-1).astype("int")
        if 'longballPct' not in df_teamStats.columns:
            df_teamStats.insert(25, 'longballPct', -1)
        df_teamStats['longballPct'] = df_teamStats['longballPct'].fillna(-1).astype("float")
        if 'blockedShots' not in df_teamStats.columns:
            df_teamStats.insert(26, 'blockedShots', -1)
        df_teamStats['blockedShots'] = df_teamStats['blockedShots'].fillna(-1).astype("int")
        if 'effectiveTackles' not in df_teamStats.columns:
            df_teamStats.insert(27, 'effectiveTackles', -1)
        df_teamStats['effectiveTackles'] = df_teamStats['effectiveTackles'].fillna(-1).astype("int")
        if 'totalTackles' not in df_teamStats.columns:
            df_teamStats.insert(28, 'totalTackles', -1)
        df_teamStats['totalTackles'] = df_teamStats['totalTackles'].fillna(-1).astype("int")
        if 'tacklePct' not in df_teamStats.columns:
            df_teamStats.insert(29, 'tacklePct', -1)
        df_teamStats['tacklePct'] = df_teamStats['tacklePct'].fillna(-1).astype("float")
        if 'interceptions' not in df_teamStats.columns:
            df_teamStats.insert(30, 'interceptions', -1)
        df_teamStats['interceptions'] = df_teamStats['interceptions'].fillna(-1).astype("int")
        if 'effectiveClearance' not in df_teamStats.columns:
            df_teamStats.insert(31, 'effectiveClearance', -1)
        df_teamStats['effectiveClearance'] = df_teamStats['effectiveClearance'].fillna(-1).astype("int")
        if 'totalClearance' not in df_teamStats.columns:
            df_teamStats.insert(32, 'totalClearance', -1)
        df_teamStats['totalClearance'] = df_teamStats['totalClearance'].fillna(-1).astype("int")
        if 'goalDifference' not in df_teamStats.columns:
            df_teamStats.insert(33, 'goalDifference', -1)
        df_teamStats['goalDifference'] = df_teamStats['goalDifference'].fillna(-1).astype("int")
        if 'totalGoals' not in df_teamStats.columns:
            df_teamStats.insert(34, 'totalGoals', -1)
        df_teamStats['totalGoals'] = df_teamStats['totalGoals'].fillna(-1).astype("int")
        if 'goalAssists' not in df_teamStats.columns:
            df_teamStats.insert(35, 'goalAssists', -1)
        df_teamStats['goalAssists'] = df_teamStats['goalAssists'].fillna(-1).astype("int")
        if 'goalsConceded' not in df_teamStats.columns:
            df_teamStats.insert(36, 'goalsConceded', -1)
        df_teamStats['goalsConceded'] = df_teamStats['goalsConceded'].fillna(-1).astype("int")

        df_teamStats['updateTime'] = pd.to_datetime(df_teamStats['updateTime'], utc=True)

        # print(df_teamStats.info())
    b2 = False
    df_teamUniform = sqlConn.importJsonToDf(filename6)
    if not(df_teamUniform.empty):
        b2 = True
        df_teamUniform['id'] = df_teamUniform['id'].astype(int)
        df_teamUniform['teamId'] = df_teamUniform['teamId'].astype(int)
        #print(df_teamUniform.info())
    if b1 or b2:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("04 TeamStats Complete", err)
    # dataSet="test2"
    if b1:
        tableName4 = 'TeamStats'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName4, dataSet)
        df_teamStats["updateId"] = updateId
        msg = sqlConn.teamStatsInsertRecordSQL(osStr,conn,cursor, tableName4, df_teamStats)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName4 + " update insertion error"
        else:
            msg = tableName4 + " database insertion successful"
        print(msg)
    print()
    if b2:
        tableName5 = 'TeamUniform'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName5, dataSet)
        df_teamUniform["updateId"] = updateId
        msg = sqlConn.teamUniformInsertRecordSQL(osStr,conn,cursor, tableName5, df_teamUniform)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName5 + " update insertion error"
        else:
            msg = tableName5 + " database insertion successful"
        print(msg)

    conn.close()
    return ("04 TeamStats Complete", err)
def Insert_GameInfo(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/gameInfo/'

    filename1 = directory2 + 'attendance.json'
    filename2 = directory2 + 'officials.json'
    filename3 = directory2 + 'venue.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df_attendance = sqlConn.importJsonToDf(filename1)
    b1 = False
    if not(df_attendance.empty):
        b1 = True
        df_attendance['eventId'] = df_attendance['eventId'].astype("int")
        df_attendance['attendance'] = df_attendance['attendance'].astype("int")
        # print(df_attendance.info())

    df_officials = sqlConn.importJsonToDf(filename2)
    b2 = False
    if not(df_officials.empty):
        b2 = True
        df_officials['eventId'] = df_officials['eventId'].astype("int")
        df_officials['order'] = df_officials['order'].astype("int")
        df_officials_col=['eventId','fullName','displayName','order']
        df_officials=df_officials[df_officials_col]
        # print(df_officials.info())
        # mask = df_officials.duplicated(subset = ['eventId', 'order'], keep=False)
        # print(df_officials[mask])
        # df_officials.drop(df_officials[mask].index, inplace = True)

    df_venueDB = sqlConn.importJsonToDf(filename3)
    b3 = False
    if not(df_officials.empty):
        b3 = True
        df_venueDB["eventId"] = df_venueDB["eventId"].astype("int")
        df_venueDB["id"] = df_venueDB["id"].astype("int")
        # print(df_venueDB.info())

    if b1 or b2 or b3:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("06 GameInfo Complete", err)

    if b1:
        tableName1 = 'Attendance'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df_attendance["updateId"] = updateId
        msg = sqlConn.attendanceInsertRecordSQL(osStr,conn,cursor, tableName1, df_attendance)
        # print(msg)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

    print()
    if b2:
        tableName2 = 'Officials'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
        df_officials['updateId'] = updateId
        msg = sqlConn.officialsInsertRecordSQL(osStr,conn,cursor, tableName2, df_officials)
        # print(msg)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)

    print()
    if b3:
        tableName3 = 'VenueDB'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
        df_venueDB["updateId"] = updateId
        msg = sqlConn.venueDBInsertRecordSQL(osStr,conn,cursor, tableName3, df_venueDB)
        # print(msg)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

    conn.close()
    return("06 GameInfo Complete", err)
def get_TeamsInLeague(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver = mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    err = 0
    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)

    sql1 = f"""
            SELECT 
                year,
                leagueId,
                midsizeLeagueName,
                seasonType,
                teamId,
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
                updateId
            FROM
                TeamsInLeague 
            WHERE seasonType = {seasonType}
            ORDER BY teamId;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    teamsInLeague = []
    if cursor.rowcount > 0:
        rs = cursor.fetchall()
        for row in rs:
            year = row[0]
            leagueId = row[1]
            midsizeLeagueName = row[2]
            seasonType = row[3]
            teamId = row[4]
            uid = row[5]
            location = row[6]
            name = row[7]
            abbreviation = row[8]
            displayName = row[9]
            shortDisplayName = row[10]
            slug = row[11]
            nickname = row[12]
            color = row[13]
            alternateColor = row[14]
            isActive = row[15]
            isAllStar = row[16]
            hasRecord = row[17]
            timeStamp = row[18]
            updateId = row[19]
            teamsInLeague.append(
                {"year":year,
                    "leagueId":leagueId,
                    "midsizeLeagueName":midsizeLeagueName,
                    "seasonType":seasonType,
                    "teamId":teamId,
                    "uid":uid,
                     "location": location,
                     "name": name,
                     "abbreviation": abbreviation,
                     "displayName": displayName,
                     "shortDisplayName": shortDisplayName,
                     "slug": slug,
                     "nickname": nickname,
                     "color": color,
                     "alternateColor": alternateColor,
                     "isActive": isActive,
                     "isAllStar": isAllStar,
                     "hasRecord": hasRecord,
                     "timeStamp":timeStamp,
                     "updateId":updateId
                }
            )
    else:
        err = -1
    conn.close()
    return(teamsInLeague, err)
def get_Standings(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver = mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    err = 0
    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)

    sql1 = f"""
            SELECT 
                year,
                leagueId,
                midsizeLeagueName,
                seasonType,
                teamId,
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
                updateId
            FROM
                Standings
            WHERE seasonType = {seasonType}
            ORDER BY teamRank, teamId;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    standings = []
    if cursor.rowcount > 0:
        rs = cursor.fetchall()
        for row in rs:
            year = row[0]
            leagueId = row[1]
            midsizeLeagueName = row[2]
            seasonType = row[3]
            teamId = row[4]
            gamesPlayed = row[5]
            losses = row[6]
            pointDifferential = row[7]
            points = row[8]
            pointsAgainst = row[9]
            pointsFor = row[10]
            streak = row[11]
            ties = row[12]
            wins = row[13]
            awayGamesPlayed = row[14]
            awayLosses = row[15]
            awayPointsAgainst = row[16]
            awayPointsFor = row[17]
            awayTies = row[18]
            awayWins = row[19]
            deductions = row[20]
            homeGamesPlayed = row[21]
            homeLosses = row[22]
            homePointsAgainst = row[23]
            homePointsFor = row[24]
            homeTies = row[25]
            homeWins = row[26]
            ppg = row[27]
            teamRank = row[28]
            rankChange = row[29]
            timeStamp = row[30]
            updateId = row[31]
            standings.append(
                {"year":year,
                    "leagueId":leagueId,
                    "midsizeLeagueName":midsizeLeagueName,
                    "seasonType":seasonType,
                    "teamId":teamId,
                    "gamesPlayed":gamesPlayed,
                    "losses":losses,
                    "pointDifferential":pointDifferential,
                    "points":points,
                    "pointsAgainst":pointsAgainst,
                    "pointsFor":pointsFor,
                    "streak":streak,
                    "ties":ties,
                    "wins":wins,
                    "awayGamesPlayed":awayGamesPlayed,
                    "awayLosses":awayLosses,
                    "awayPointsAgainst":awayPointsAgainst,
                    "awayPointsFor":awayPointsFor,
                    "awayTies":awayTies,
                    "awayWins":awayWins,
                    "deductions":deductions,
                    "homeGamesPlayed":homeGamesPlayed,
                    "homeLosses":homeLosses,
                    "homePointsAgainst":homePointsAgainst,
                    "homePointsFor":homePointsFor,
                    "homeTies":homeTies,
                    "homeWins":homeWins,
                    "ppg":ppg,
                    "rank":teamRank,
                    "rankChange":rankChange,
                    "timeStamp":timeStamp,
                    "updateId":updateId
                }
            )
    else:
        err = -1
    conn.close()
    return(standings, err)

def transpose_table(rowsInTable):
    tableT = {}
    for row in rowsInTable:
        for key in row:
            if key not in tableT:
                tableT[key] = []
            tableT[key].append(row[key])
    return(tableT)

def compare_table(table1,table2, excludedKeys):
    # excludedKeys = ["timeStamp","updateId"]
    table1T = transpose_table(table1)
    table2T = transpose_table(table2)
    bDiff = False
    for key in table1T:
        list1 = table1T[key]
        list2 = table2T[key]
        #print(key, list1)
        #print(key, list2)
        if key not in excludedKeys:
            if list1 != list2:
                print("diff",key, list1)
                print("diff",key, list2)
                print()
                bDiff = True
                break
    return(bDiff)
def Insert_Standings(rootDir, rootDir2,dataSet, dbConnect):
    # directory1 = rootDir
    directory2 = rootDir2 + 'tables/standings/'

    filename1 = directory2 + 'teamsInLeague.json'
    filename2 = directory2 + 'teamStandingsInLeague.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    defaultTimeZone = tz.gettz("UTC")
    defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
    # print(defaultTime)

    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    teamTablename = "Teams"
    teamIdList = []
    leagueTablename = "Leagues"
    leagueIdList = []
    cursor.execute(f"SELECT teamId FROM {teamTablename};")
    rsTeam = cursor.fetchall()
    for row in rsTeam:
        teamIdList.append(row[0])
    # print("teamId list length:", len(teamIdList))
    cursor.execute(f"SELECT id FROM {leagueTablename};")
    rsLeague = cursor.fetchall()
    for row in rsLeague:
        leagueIdList.append(row[0])
    conn.close()
    # print("leagueId list length:", len(leagueIdList))

    #
    # Insert TeamsInLeague
    #
    with open(filename1, "r") as file:
        teamsInLeague = json.load(file)
    file.close()

    print(len(teamsInLeague))
    if len(teamsInLeague)> 0:
        teamsDict ={}
        for team in teamsInLeague:
            seasonType = team['seasonType']
            if seasonType not in teamsDict:
                teamsDict[seasonType]=[]
            teamsDict[seasonType].append(team)
        i = 0
        nTotSeasons=len(teamsDict)
        for seasonType in teamsDict:
            bUpdate = False
            bInsert = False
            tmpTeam = teamsDict[seasonType]
            sortedTeamsInLeagueOld,err = get_TeamsInLeague(dbConnect,seasonType)
            i += 1
            # print(tmpStandings)
            df1 = pd.json_normalize(tmpTeam)
            if 'nickname' not in df1:
                df1['nickname'] = ""
            # print(df1.info())
            # df1 = df1.replace('',np.nan)
            df1['year'] = df1['year'].fillna(0).astype("int")
            df1['leagueId'] = df1['leagueId'].fillna(0).astype("int")
            df1['midsizeLeagueName'] = df1['midsizeLeagueName'].fillna("").astype("object")
            df1['seasonType'] = df1['seasonType'].fillna(0).astype("int")
            df1['id'] = df1['id'].fillna(0).astype("int")
            df1['uid'] = df1['uid'].fillna("").astype("object")
            df1['location'] = df1['location'].fillna("").astype("object")
            df1['name'] = df1['name'].fillna("").astype("object")
            df1['abbreviation'] = df1['abbreviation'].fillna("").astype("object")
            df1['displayName'] = df1['displayName'].fillna("").astype("object")
            df1['shortDisplayName'] = df1['shortDisplayName'].fillna("").astype("object")
            df1['slug'] = df1['slug'].fillna("").astype(object)
            df1['nickname'] = df1['nickname'].fillna("").astype(object)
            df1['color'] = df1['color'].fillna("").astype("object")
            df1['alternateColor'] = df1['alternateColor'].fillna("").astype("object")
            df1['isActive'] = df1['isActive'].fillna(False).astype("bool")
            df1['isAllStar'] = df1['isAllStar'].fillna(False).astype("bool")
            df1['hasRecord'] = df1['hasRecord'].fillna(False).astype("bool")
            df1['timeStamp'] = pd.to_datetime(df1['timeStamp'], utc=True)
            df1['timeStamp'] = df1['timeStamp'].fillna(defaultTime)

            df1_cols = ['year', 'leagueId', 'midsizeLeagueName', 'seasonType',
                        'id', 'uid', 'location',
                        'name', 'abbreviation', 'displayName', 'shortDisplayName', 'slug', 'nickname',
                        'color', 'alternateColor', 'isActive', 'isAllStar', 'hasRecord', 'timeStamp']
            df1 = df1[df1_cols]
            # rename "id" to "teamId"
            df1 = df1.rename(
                columns={"id": "teamId"}
            )
            # print(df1.info())

            teamsInLeagueList = df1.to_dict('records')
            sortedTeamsInLeague = sorted(teamsInLeagueList, key=lambda x: x['teamId'])
            if err < 0:
                bInsert = True
            else:
                excludedKeys = ["timeStamp", "updateId"]
                bDiff = compare_table(sortedTeamsInLeague,sortedTeamsInLeagueOld,excludedKeys)
                if bDiff:
                    bUpdate = True
            #if (bUpdate or bInsert) and seasonType in seasonList:
            print("TeamsInLeague seasonType:", seasonType,"Processed", i, "out of", nTotSeasons, "update", bUpdate, "insert", bInsert)
            if bUpdate or bInsert:
                print("new",len(sortedTeamsInLeague))
                print("old",len(sortedTeamsInLeagueOld))
                n = 0
                for tmpList in sortedTeamsInLeague:
                   # print("new",tmpList)
                   # print("old", sortedStandingsOld[n])
                   n+=1
                if osStr == "Windows":
                    (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
                elif osStr == "Linux":
                    (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
                else:
                    (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
                tableName1 = 'TeamsInLeague'
                print(seasonType, tableName1)
                (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
                df1["updateId"] = updateId
                # msg = sqlConn.teamsInLeagueInsertRecordSQL(osStr,conn,cursor, tableName1, df1,
                #                                           teamIdList,leagueIdList,seasonType,bUpdate,bInsert)
                msg = sqlConn.standings_teamsInsertRecordSQL(osStr,conn,cursor, tableName1, df1,
                                                        teamIdList,leagueIdList,seasonType,bUpdate,bInsert)
                msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
                if errFlag < 0:
                    err = errFlag
                    msg = tableName1 + " update insertion error"
                else:
                    msg = tableName1 + " database insertion successful"
                print(msg)
                conn.close()

    #
    # Insert Standings
    #
    with open(filename2, "r") as file:
        standings = json.load(file)
    file.close()

    #seasonList = [12654,12655,12826,12358]
    #print(len(standings))
    if len(standings)> 0:
        standingsDict ={}
        for teamRecord in standings:
            seasonType = teamRecord['seasonType']
            if seasonType not in standingsDict:
                standingsDict[seasonType]=[]
            standingsDict[seasonType].append(teamRecord)
        i = 0
        nTotSeasons=len(standingsDict)
        for seasonType in standingsDict:
            bUpdate = False
            bInsert = False
            tmpStandings = standingsDict[seasonType]
            sortedStandingsOld,err = get_Standings(dbConnect,seasonType)
            i += 1
            # print(tmpStandings)
            df2 = pd.json_normalize(tmpStandings)
            # df2 = df2.replace('',np.nan)
            df2['year'] = df2['year'].astype("int")
            df2['leagueId'] = df2['leagueId'].astype("int")
            df2['midsizeLeagueName'] = df2['midsizeLeagueName'].fillna("").astype("object")
            df2['seasonType'] = df2['seasonType'].astype("int")
            df2['teamId'] = df2['teamId'].astype("int")
            df2['gamesPlayed'] = df2['gamesPlayed'].fillna(0).astype("int")
            df2['losses'] = df2['losses'].fillna(0).astype("int")
            df2['pointDifferential'] = df2['pointDifferential'].fillna(0).astype("int")
            df2['points'] = df2['points'].fillna(0).astype("int")
            df2['pointsAgainst'] = df2['pointsAgainst'].fillna(0).astype("int")
            df2['pointsFor'] = df2['pointsFor'].fillna(0).astype("int")
            df2['streak'] = df2['streak'].fillna(0).astype("int")
            df2['ties'] = df2['ties'].fillna(0).astype("int")
            df2['wins'] = df2['wins'].fillna(0).astype("int")
            df2['awayGamesPlayed'] = df2['awayGamesPlayed'].fillna(0).astype("int")
            df2['awayLosses'] = df2['awayLosses'].fillna(0).astype("int")
            df2['awayPointsAgainst'] = df2['awayPointsAgainst'].fillna(0).astype("int")
            df2['awayPointsFor'] = df2['awayPointsFor'].fillna(0).astype("int")
            df2['awayTies'] = df2['awayTies'].fillna(0).astype("int")
            df2['awayWins'] = df2['awayWins'].fillna(0).astype("int")
            df2['deductions'] = df2['deductions'].fillna(0).astype("int")
            df2['homeGamesPlayed'] = df2['homeGamesPlayed'].fillna(0).astype("int")
            df2['homeLosses'] = df2['homeLosses'].fillna(0).astype("int")
            df2['homePointsAgainst'] = df2['homePointsAgainst'].fillna(0).astype("int")
            df2['homePointsFor'] = df2['homePointsFor'].fillna(0).astype("int")
            df2['homeTies'] = df2['homeTies'].fillna(0).astype("int")
            df2['homeWins'] = df2['homeWins'].fillna(0).astype("int")
            df2['ppg'] = df2['ppg'].fillna(0).astype("int")
            df2['rank'] = df2['rank'].fillna(0).astype("int")
            df2['rankChange'] = df2['rankChange'].fillna(0).astype("int")
            df2['timeStamp'] = pd.to_datetime(df2['timeStamp'], utc=True)
            df2['timeStamp'] = df2['timeStamp'].fillna(defaultTime)

            df2_cols = ['year', 'leagueId', 'midsizeLeagueName',
                        'seasonType',
                        'teamId',
                        'gamesPlayed',
                        'losses',
                        'pointDifferential',
                        'points',
                        'pointsAgainst',
                        'pointsFor',
                        'streak',
                        'ties',
                        'wins',
                        'awayGamesPlayed',
                        'awayLosses',
                        'awayPointsAgainst',
                        'awayPointsFor',
                        'awayTies',
                        'awayWins',
                        'deductions',
                        'homeGamesPlayed',
                        'homeLosses',
                        'homePointsAgainst',
                        'homePointsFor',
                        'homeTies',
                        'homeWins',
                        'ppg',
                        'rank',
                        'rankChange',
                        'timeStamp']
            df2 = df2[df2_cols]
            standingsList = df2.to_dict('records')
            sortedStandings = sorted(standingsList, key=lambda x: (x['rank'], x['teamId']))
            if err < 0:
                bInsert = True
            else:
                excludedKeys = ["timeStamp", "updateId"]
                bDiff = compare_table(sortedStandings,sortedStandingsOld,excludedKeys)
                if bDiff:
                    bUpdate = True
            #if (bUpdate or bInsert) and seasonType in seasonList:
            print("Sstandings seasonType:", seasonType,"Processed", i, "out of", nTotSeasons, "update", bUpdate, "insert", bInsert)
            if bUpdate or bInsert:
                print("new",len(sortedStandings))
                print("old",len(sortedStandingsOld))
                n = 0
                for tmpList in sortedStandings:
                   # print("new",tmpList)
                   # print("old", sortedStandingsOld[n])
                   n+=1
                if osStr == "Windows":
                    (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
                elif osStr == "Linux":
                    (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
                else:
                    (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
                tableName2 = 'Standings'
                print(seasonType, tableName2)
                (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
                df2["updateId"] = updateId
                msg = sqlConn.standings_teamsInsertRecordSQL(osStr,conn,cursor, tableName2, df2,
                                                        teamIdList,leagueIdList,seasonType,bUpdate,bInsert)
                msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
                if errFlag < 0:
                    err = errFlag
                    msg = tableName2 + " update insertion error"
                else:
                    msg = tableName2 + " database insertion successful"
                print(msg)
                conn.close()
    return("16 Standings Complete", err)
def Insert_KeyEvents_01(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/keyEvents/'

    filename1 = directory2 + 'keyEventType.json'
    filename2 = directory2 + 'keyEventSource.json'
    filename3 = directory2 + 'plays.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df1 = sqlConn.importJsonToDf2(filename1)
    if not(df1.empty):
        b1 = True
        df1['id'] = df1['id'].astype("int")
        # print(df1.info())
    b2 = False
    df2 = sqlConn.importJsonToDf3(filename2)
    if not(df2.empty):
        b2 = True
        df2 = df2.rename(
            columns={"index": "index",
                     "name": "id",
                     "value": "name"}
        )
        df2['id'] = df2['id'].astype("int")
        # print(df2.info())
    defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    defaultTimeZone = tz.gettz("UTC")
    defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
    #print(defaultTime)
    b3 = False
    df3 = sqlConn.importJsonToDf(filename3)
    if not (df3.empty):
        b3 = True
        df3['eventId'] = df3['eventId'].astype("int")
        df3['order'] = df3['order'].astype("int")
        df3['id'] = df3['id'].astype("int")
        df3['typeId'] = df3['typeId'].astype("int")
        # df3['text'] = df3['text'].fillna("").astype("str")
        # df3['shotText'] = df3['shortText'].fillna("").astype("str")
        df3['period'] = df3['period'].astype("int")
        df3['clockValue'] = df3['clockValue'].replace('', np.nan)
        df3['clockValue'] = df3['clockValue'].fillna(0).astype("int")
        df3['teamId'] = df3['teamId'].replace('', np.nan)
        df3['teamId'] = df3['teamId'].fillna(0).astype("int")
        df3['teamDisplayName'] = df3['teamDisplayName'].fillna("").astype("object")
        #df3['teamDisplayName'] = df3['teamDisplayName'].astype(object)
        #df3['teamDisplayName'] = df3['teamDisplayName'].where(pd.notnull(df3['teamDisplayName']), None)
        df3['text'] = df3['text'].fillna("").astype(object)
        #df3['text'] = df3['text'].where(pd.notnull(df3['text']), None)
        df3['shortText'] = df3['shortText'].fillna("").astype(object)
        #df3['shortText'] = df3['shortText'].where(pd.notnull(df3['shortText']), None)
        if 'wallclock' not in df3.columns:
            df3.insert(14, "wallclock", defaultTime)
        df3['wallclock'] = pd.to_datetime(df3['wallclock'], utc=True)
        df3['wallclock'] = df3['wallclock'].fillna(defaultTime)
        if 'goalPositionX' not in df3.columns:
            df3.insert(15, 'goalPositionX', 0)
        if 'goalPositionY' not in df3.columns:
            df3.insert(16, 'goalPositionY', 0)
        if 'fieldPositionX' not in df3.columns:
            df3.insert(17, 'fieldPositionX', 0)
        if 'fieldPositionY' not in df3.columns:
            df3.insert(18, 'fieldPositionY', 0)
        if 'fieldPosition2X' not in df3.columns:
            df3.insert(19, 'fieldPosition2X', 0)
        if 'fieldPosition2Y' not in df3.columns:
            df3.insert(20, 'fieldPosition2Y', 0)
        df3['goalPositionX'] = df3['goalPositionX'].fillna(0).astype("float")
        df3['goalPositionY'] = df3['goalPositionY'].fillna(0).astype("float")
        df3['fieldPositionX'] = df3['fieldPositionX'].fillna(0).astype("float")
        df3['fieldPositionY'] = df3['fieldPositionY'].fillna(0).astype("float")
        df3['fieldPosition2X'] = df3['fieldPosition2X'].fillna(0).astype("float")
        df3['fieldPosition2Y'] = df3['fieldPosition2Y'].fillna(0).astype("float")
        df3['sourceId'] = df3['sourceId'].astype("int")

        df3_col = ['eventId', 'order', 'id', 'typeId',
                   'text', 'shortText',
                   'period',
                   'clockValue', 'clockDisplayValue',
                   'teamDisplayName',
                   'teamId',
                   'sourceId', 'scoringPlay', 'shootout',
                   'wallclock', 'goalPositionX', 'goalPositionY',
                   'fieldPositionX', 'fieldPositionY',
                   'fieldPosition2X', 'fieldPosition2Y']
        df3 = df3[df3_col]
        # print(df3.info())
    if b1 or b2 or b3:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("06 KeyEvents 01 Complete", err)

    # dataSet="test2"
    if b1:
        tableName1 = 'KeyEventType'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.keyEventTypeInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)
    print()
    if b2:
        tableName2 = 'KeyEventSource'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
        df2["updateId"] = updateId
        msg = sqlConn.keyEventSourceInsertRecordSQL(osStr,conn,cursor, tableName2, df2)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)
    print()
    if b3:
        tableName3 = 'Plays'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
        df3["updateId"] = updateId
        msg = sqlConn.playsInsertRecordSQL(osStr,conn,cursor, tableName3, df3)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

    conn.close()
    return("06 KeyEvents 01 Complete", err)
def Insert_KeyEvents_02(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/keyEvents/'

    filename3 = directory2 + 'keyEvent.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df3 = sqlConn.importJsonToDf(filename3)
    if not(df3.empty):
        df3['eventId'] = df3['eventId'].astype("int")
        df3['order'] = df3['order'].astype("int")
        df3['id'] = df3['id'].astype("int")
        df3['typeId'] = df3['typeId'].astype("int")
        df3['period'] = df3['period'].astype("int")
        df3['clockValue'] = df3['clockValue'].astype("int")
        if 'teamId' in df3:
            df3['teamId'] = df3['teamId'].fillna(-1).astype("int")
        else:
            df3['teamId'] = -1
        if 'teamDisplayName' in df3:
            df3['teamDisplayName'] = df3['teamDisplayName'].fillna("").astype(object)
        else:
            df3['teamDisplayName'] = ""
        if 'text' in df3:
            df3['text'] = df3['text'].fillna("").astype(object)
        else:
            df3['text'] = ""
        if 'shortText' in df3:
            df3['shortText'] = df3['shortText'].fillna("").astype(object)
        else:
            df3['shortText'] = ""
        if 'sourceId' in df3:
            df3['sourceId'] = df3['sourceId'].astype("int")
        else:
            df3['sourceId'] = -1
        if 'goalPositionX' not in df3.columns:
            df3.insert(14, 'goalPositionX', 0)
        if 'goalPositionY' not in df3.columns:
            df3.insert(15, 'goalPositionY', 0)
        if 'fieldPositionX' not in df3.columns:
            df3.insert(16, 'fieldPositionX', 0)
        if 'fieldPositionY' not in df3.columns:
            df3.insert(17, 'fieldPositionY', 0)
        if 'fieldPosition2X' not in df3.columns:
            df3.insert(18, 'fieldPosition2X', 0)
        if 'fieldPosition2Y' not in df3.columns:
            df3.insert(19, 'fieldPosition2Y', 0)
        df3['goalPositionX'] = df3['goalPositionX'].fillna(0).astype("float")
        df3['goalPositionY'] = df3['goalPositionY'].fillna(0).astype("float")
        df3['fieldPositionX'] = df3['fieldPositionX'].fillna(0).astype("float")
        df3['fieldPositionY'] = df3['fieldPositionY'].fillna(0).astype("float")
        df3['fieldPosition2X'] = df3['fieldPosition2X'].fillna(0).astype("float")
        df3['fieldPosition2Y'] = df3['fieldPosition2Y'].fillna(0).astype("float")
        df3['goalPositionX'] = df3['goalPositionX'].fillna(0).astype("float")
        df3['goalPositionY'] = df3['goalPositionY'].fillna(0).astype("float")
        df3['fieldPositionX'] = df3['fieldPositionX'].fillna(0).astype("float")
        df3['fieldPositionY'] = df3['fieldPositionY'].fillna(0).astype("float")
        df3['fieldPosition2X'] = df3['fieldPosition2X'].fillna(0).astype("float")
        df3['fieldPosition2Y'] = df3['fieldPosition2Y'].fillna(0).astype("float")

        df3_col = ['eventId', 'order', 'id', 'typeId',
                   'period',
                   'clockValue', 'clockDisplayValue',
                   'scoringPlay',
                   'sourceId',
                   'shootout',
                   'text', 'shortText',
                   'teamId',
                   'teamDisplayName',
                   'goalPositionX', 'goalPositionY',
                   'fieldPositionX', 'fieldPositionY',
                   'fieldPosition2X', 'fieldPosition2Y']
        df3 = df3[df3_col]
        # print(df3.info())

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName3 = 'KeyEvents'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
        df3["updateId"] = updateId
        msg = sqlConn.keyEventsInsertRecordSQL(osStr,conn,cursor, tableName3, df3)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

        conn.close()
    return ("05 KeyEvents 02 Complete", err)
def Insert_KeyEvents_03(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/keyEvents/'

    filename3 = directory2 + 'commentary.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df3 = sqlConn.importJsonToDf(filename3)
    if not(df3.empty):
        df3 = df3.rename(
            columns={"sequence": "order",
                     "timeValue": "clockValue",
                     "timeDisplayValue": "clockDisplayValue"}
        )
        df3 = df3.replace('', np.nan)
        df3['eventId'] = df3['eventId'].astype("int")
        df3['order'] = df3['order'].astype("int")
        df3['clockValue'] = df3['clockValue'].astype("int")
        df3['clockDisplayValue'] = df3['clockDisplayValue'].fillna("").astype(object)
        df3['text'] = df3['text'].fillna("").astype(object)
        df3['id'] = df3['id'].fillna(-1).astype("int")
        mask = df3.duplicated(subset=['eventId', 'order'], keep="last")
        df3.drop(df3[mask].index, inplace=True)
        # print(df3[mask].to_string())
        # print(df3.info())

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName3 = 'Commentary'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
        df3["updateId"] = updateId
        msg = sqlConn.commentaryInsertRecordSQL(osStr,conn,cursor, tableName3, df3)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

        conn.close()
    return("08 KeyEvents 03 Complete", err)
def Insert_Athletes(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/roster/'
    directory3 = rootDir2 + 'tables/teams/'

    filename1 = directory2 + 'athletes.json'
    filename2 = directory3 + 'playerDB.json'
    filename3 = directory3 + 'playerInTeamDB.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b1 = False
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        b1 = True
        df1['id'] = df1['id'].astype("int")
        cols = [c for c in df1.columns if c.lower()[:4] != 'posi']
        df1 = df1[cols]
        cols = [c for c in df1.columns if c.lower()[:4] != 'head']
        df1 = df1[cols]
        df1['uid'] = df1['uid'].fillna("").astype(object)
        df1['guid'] = df1['guid'].fillna("").astype(object)
        df1['lastName'] = df1['lastName'].fillna("").astype(object)
        df1['fullName'] = df1['fullName'].fillna("").astype(object)
        df1['displayName'] = df1['displayName'].fillna("").astype(object)
        df1['updateTime'] = pd.to_datetime(df1['updateTime'], utc=True)
        if 'jersey' in df1:
            df1 = df1.drop(columns=['jersey'])
        df1 = df1.sort_values(by=['id']).reset_index(drop=True)

        df1_col = ['id',
                   'uid',
                   'guid',
                   'lastName',
                   'fullName',
                   'displayName',
                   'updateTime']
        df1 = df1[df1_col]
        # inx=df1['id']
        # mask1=inx.duplicated(keep="first")
        # print(df1[mask1])
        # df1.drop(df1[mask1].index, inplace = True)
        # print(df1.info())

        # mask = df1.duplicated(subset = ['id'], keep=False)
        # print(df1[mask])
        # df1.drop(df1[mask].index, inplace = True)
    b2 = False
    df2 = sqlConn.importJsonToDf(filename2)
    if not(df2.empty):
        b2 = True
        df2['id'] = df2['id'].astype("int")
        # df2 = df2.replace('',np.nan)
        df2['uid'] = df2['uid'].fillna("").astype(object)
        df2['guid'] = df2['guid'].fillna("").astype(object)
        df2['lastName'] = df2['lastName'].fillna("").astype(object)
        df2['firstName'] = df2['firstName'].fillna("").astype(object)
        df2['fullName'] = df2['fullName'].fillna("").astype(object)
        df2['weight'] = df2['weight'].fillna(0).astype("float")
        df2['displayWeight'] = df2['displayWeight'].fillna("").astype("str")
        df2['height'] = df2['height'].fillna(0).astype("float")
        df2['displayHeight'] = df2['displayHeight'].fillna("None").astype("str")
        df2['jersey'] = df2['jersey'].fillna(-1).astype("int")
        df2['age'] = df2['age'].fillna(-1).astype("int")
        df2['gender'] = df2['gender'].fillna("").astype(object)
        df2['citizenship'] = df2['citizenship'].fillna("").astype(object)
        df2['birthPlace.country'] = df2['birthPlace.country'].fillna("").astype(object)
        if 'birthPlace.city' in df2:
            df2['birthPlace.city'] = df2['birthPlace.city'].fillna("").astype(object)
        else:
            df2['birthPlace.city'] = ""
        df2['citizenshipCountry.alternateId'] = df2['citizenshipCountry.alternateId'].fillna(-1).astype("int")
        df2['citizenshipCountry.abbreviation'] = df2['citizenshipCountry.abbreviation'].fillna("").astype(object)
        df2['birthCountry.alternateId'] = df2['birthCountry.alternateId'].fillna(-1).astype("int")
        df2['birthCountry.abbreviation'] = df2['birthCountry.abbreviation'].fillna("").astype(object)
        df2['middleName'] = df2['middleName'].fillna("").astype(object)
        if 'nickname' in df2:
            df2['nickname'] = df2['nickname'].fillna("").astype(object)
        else:
            df2['nickname'] = ""
        df2['position.id'] = df2['position.id'].fillna(-1).astype("int")
        df2['position.name'] = df2['position.name'].fillna("").astype(object)
        df2['position.displayName'] = df2['position.displayName'].fillna("").astype(object)
        df2['position.abbreviation'] = df2['position.abbreviation'].fillna("").astype(object)
        df2['dateOfBirth'] = df2['dateOfBirth'].fillna("").astype(object)
        df2['headshot.href'] = df2['headshot.href'].fillna("").astype(object)
        df2['headshot.alt'] = df2['headshot.alt'].fillna("").astype(object)
        df2['flag.href'] = df2['flag.href'].fillna("").astype(object)
        df2['flag.alt'] = df2['flag.alt'].fillna("").astype(object)
        # print(df2.to_string())
        # df2['dateOfBirth'] = pd.to_datetime(df2['dateOfBirth'],utc=True)
        df2['timestamp'] = pd.to_datetime(df2['timestamp'], utc=True)
        # print(df2.info())
        df2 = df2.drop(columns=['flag.rel', 'seasons.$ref', 'leagues.$ref'])
        df2 = df2.drop(columns=['transactions.$ref', 'events.$ref', 'defaultLeague.$ref', 'defaultTeam.$ref'])
        df2 = df2.drop(columns=['injuries', 'teamId', 'teamName'])
        df2 = df2.drop(columns=['seasonYear', 'seasonType', 'seasonName', 'index', 'league'])
        # df2=df2.rename(columns={"teamId":"defaultTeamId",
        #                        "teamName":"defaultTeamName"})
        df2 = df2.sort_values(by=['id']).reset_index(drop=True)

        df2_col = ['id',
                   'uid',
                   'guid',
                   'firstName',
                   'middleName',
                   'lastName',
                   'fullName',
                   'displayName',
                   'shortName',
                   'nickname',
                   'weight',
                   'displayWeight',
                   'height',
                   'displayHeight',
                   'age',
                   'dateOfBirth',
                   'gender',
                   'citizenship',
                   'slug',
                   'jersey',
                   'status',
                   'profiled',
                   'timestamp',
                   'birthPlace.city',
                   'birthPlace.country',
                   'birthCountry.alternateId',
                   'birthCountry.abbreviation',
                   'citizenshipCountry.alternateId',
                   'citizenshipCountry.abbreviation',
                   'flag.href',
                   'flag.alt',
                   'position.id',
                   'position.name',
                   'position.displayName',
                   'position.abbreviation',
                   'headshot.href',
                   'headshot.alt']
        df2 = df2[df2_col]

        inx = df2['id']
        mask1 = inx.duplicated(keep="first")
        #print(df2[mask1])
        df2.drop(df2[mask1].index, inplace=True)
        # print(df2.info())

    b3 = False
    df3 = sqlConn.importJsonToDf(filename3)
    if not(df3.empty):
        b3 = True
        df3['athleteId'] = df3['athleteId'].astype("int")
        df3['teamId'] = df3['teamId'].astype("int")
        df3['seasonType'] = df3['seasonType'].astype("int")
        df3['seasonYear'] = df3['seasonYear'].astype("int")
        df3['seasonName'] = df3['seasonName'].astype(object)
        df3['league'] = df3['league'].astype(object)
        df3['teamName'] = df3['teamName'].astype(object)
        df3['playerIndex'] = df3['playerIndex'].astype("int")
        df3['playerDisplayName'] = df3['playerDisplayName'].astype(object)
        df3['jersey'] = df3['jersey'].astype("int")
        df3['positionId'] = df3['positionId'].astype("int")
        df3['hasStats'] = df3['hasStats'].astype(bool)
        df3['timestamp'] = pd.to_datetime(df3['timestamp'], utc=True)
    if b1 or b2 or b3:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("09 Athletes Complete", err)
    if b1:
        tableName1 = 'Athletes'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.athletesInsertRecordSQL(osStr,conn,cursor, tableName1, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)
    print()
    if b2:
        tableName2 = 'PlayerDB'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
        df2["updateId"] = updateId
        msg = sqlConn.playerDBInsertRecordSQL(osStr,conn,cursor, tableName2, df2)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)

    conn.close()
    return ("09 Athletes Complete", err)
def Insert_PlayerInTeam(rootDir, rootDir2,dataSet, dbConnect):
    directory3 = rootDir2 + 'tables/teams/'
    filename3 = directory3 + 'playerInTeamDB.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    with open(filename3, "r") as file:
        playerInTeamDB = json.load(file)
    file.close()

    playerInTeamDict = {}
    for player in playerInTeamDB:
        athleteId = int(player['athleteId'])
        teamId = int(player['teamId'])
        seasonType = player['seasonType']
        if (teamId,seasonType) not in playerInTeamDict.keys():
            playerInTeamDict[(teamId,seasonType)] = []
        playerInTeamDict[(teamId,seasonType)].append(player)

    tableName3 = 'PlayerInTeam'
    i=0
    nTeams = len(playerInTeamDict)
    nNewTeam = 0
    nChangedTeam = 0
    for key in playerInTeamDict:
        b3 = True
        bNewTeam = False
        i += 1
        teamId = key[0]
        seasonType = key[1]
        playerInTeam = playerInTeamDict[key]
        playerList = []
        for player in playerInTeam:
            playerList.append(int(player['athleteId']))

        if osStr == "Windows":
            sql1 = ("SELECT athleteId"
                    " FROM " + tableName3 +
                    " INNER JOIN"
                    " (SELECT MAX(updateId) as currentUpdateId"
                    " FROM " + tableName3 +
                    " WHERE teamId = ? and seasonType = ?) a"
                    " ON updateId = a.currentUpdateId"
                    " ORDER BY athleteId;")
        else:
            sql1 = ("SELECT athleteId"
                    " FROM " + tableName3 +
                    " INNER JOIN"
                    " (SELECT MAX(updateId) as currentUpdateId"
                    " FROM " + tableName3 +
                    " WHERE teamId = %s and seasonType = %s) a"
                    " ON updateId = a.currentUpdateId"
                    " ORDER BY athleteId;")
        #
        # 11/2/2024
        # remove duplicates in playerList and sort playerList
        #
        newPlayerList=sorted(list(set(playerList)))
        nPlayers = len(newPlayerList)

        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)

        val = (teamId, seasonType)
        cursor.execute(sql1, val)
        nPlayersDB = cursor.rowcount
        rs = cursor.fetchall()
        playerListDB = []
        for tmpId in rs:
            playerListDB.append(tmpId[0])
        conn.close()
        #print(rs)
        #print(playerListDB)
        #print(newPlayerList)
        #print(playerListDB == newPlayerList)
        if len(playerListDB) == 0:
            bNewTeam = True
            b3 = True
            nNewTeam += 1
        elif playerListDB == newPlayerList:
            b3 = False
        # print("new Team:",bNewTeam,"changed Team:",b3, i, "of", nTeams, nPlayers,nPlayersDB, 'teamId=',teamId, 'seasonType=', seasonType)
        # print(df3.info())
        if b3:
            nChangedTeam += 1
            df3 = pd.json_normalize(playerInTeam)
            df3['athleteId'] = df3['athleteId'].astype("int")
            df3['teamId'] = df3['teamId'].astype("int")
            df3['seasonType'] = df3['seasonType'].astype("int")
            df3['seasonYear'] = df3['seasonYear'].astype("int")
            df3['seasonName'] = df3['seasonName'].astype(object)
            df3['league'] = df3['league'].astype(object)
            df3['teamName'] = df3['teamName'].astype(object)
            df3['playerIndex'] = df3['playerIndex'].astype("int")
            df3['playerDisplayName'] = df3['playerDisplayName'].astype(object)
            df3['jersey'] = df3['jersey'].astype("int")
            df3['positionId'] = df3['positionId'].astype("int")
            df3['hasStats'] = df3['hasStats'].astype(bool)
            df3['timestamp'] = pd.to_datetime(df3['timestamp'], utc=True)
            # 11/2/2024
            # drop duplicated rows
            # duplicated rows can happen when there are two seasonTypes from fixtures, but the downloaded Teamroster
            # has the same seasonType
            #
            mask = df3.duplicated(subset=['seasonType', 'teamId','athleteId'], keep="last")
            df3.drop(df3[mask].index, inplace=True)
            if osStr == "Windows":
                (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
            elif osStr == "Linux":
                (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
            else:
                (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
            (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
            #print(updateId,updateTime)
            df3["updateId"] = updateId
            #print(tableName3)
            msg = sqlConn.playerInTeamInsertRecordSQL(osStr,conn,cursor, tableName3, df3,seasonType,teamId)
            msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
            if errFlag < 0:
                err = errFlag
                msg = tableName3 + " update insertion error"
            else:
                msg = tableName3 + " database insertion successful"
            conn.close()
    print()
    print("Insert PlayerInTeam Complete")
    print("        total teams=",nTeams)
    print("    new total teams=", nNewTeam)
    print("changed total teams=", nChangedTeam)
    return ("09 PlayerInTeam Complete", err)
def Insert_Participants(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/keyEvents/'

    filename2 = directory2 + 'playParticipants.json'
    filename3 = directory2 + 'keyEventParticipants.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    b2 = False
    df2 = sqlConn.importJsonToDf(filename2)
    if not(df2.empty):
        b2 = True
        df2['eventId'] = df2['eventId'].astype("int")
        df2['id'] = df2['id'].astype("int")
        df2['order'] = df2['order'].astype("int")
        # print(df2.info())
    b3 = False
    df3 = sqlConn.importJsonToDf(filename3)
    if not(df3.empty):
        b3 = True
        df3['eventId'] = df3['eventId'].astype("int")
        df3['id'] = df3['id'].astype("int")
        df3['order'] = df3['order'].astype("int")
        # print(df3[mask].to_string())
        # print(df3.info())
    if b2 or b3:
        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)
    else:
        return ("10 Participants Complete", err)
    if b2:
        tableName2 = 'PlayParticipants'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
        df2["updateId"] = updateId
        msg = sqlConn.playParticipantsInsertRecordSQL(osStr,conn,cursor, tableName2, df2)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)
    print()
    if b3:
        tableName3 = 'KeyEventParticipants'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName3, dataSet)
        df3["updateId"] = updateId
        msg = sqlConn.keyEventParticipantsInsertRecordSQL(osStr,conn,cursor, tableName3, df3)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName3 + " update insertion error"
        else:
            msg = tableName3 + " database insertion successful"
        print(msg)

    conn.close()
    return ("10 Participants Complete", err)
def Insert_Roster_01(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/roster/'

    filename1 = directory2 + 'playerPlays.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        df1['eventId'] = df1['eventId'].astype("int")
        df1['teamId'] = df1['teamId'].astype("int")
        df1['order'] = df1['order'].astype("int")
        df1['athleteId'] = df1['athleteId'].astype("int")
        df1['hasPlays'] = df1['hasPlays'].fillna("").astype("bool")
        df1['scoringPlay'] = df1['scoringPlay'].fillna("").astype("bool")
        df1['substitution'] = df1['substitution'].fillna("").astype("bool")
        df1['redCard'] = df1['redCard'].fillna("").astype("bool")
        df1['yellowCard'] = df1['yellowCard'].fillna("").astype("bool")
        df1['penaltyKick'] = df1['penaltyKick'].fillna("").astype("bool")
        df1['ownGoal'] = df1['ownGoal'].fillna("").astype("bool")
        df1['updateTime'] = pd.to_datetime(df1['updateTime'], utc=True)
        df1['didScore'] = df1['didScore'].fillna("").astype("bool")
        if 'didAssist' in df1:
            df1['didAssist'] = df1['didAssist'].fillna("").astype("bool")
        else:
            df1['didAssist'] = False
        df1_col = ['eventId',
                   'teamId',
                   'homeAway',
                   'athleteId',
                   'athleteDisplayName',
                   'hasPlays',
                   'clockDisplayValue',
                   'clockValue',
                   'order',
                   'scoringPlay',
                   'substitution',
                   'redCard',
                   'yellowCard',
                   'penaltyKick',
                   'ownGoal',
                   'didScore',
                   'updateTime',
                   'didAssist']

        df1 = df1[df1_col]
        # print(df1.info())

        mask = df1.duplicated(subset=['eventId', 'teamId', 'athleteId'], keep="last")
        # print(df1[mask].to_string())
        df1.drop(df1[mask].index, inplace=True)

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'PlayerPlays'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.playerPlaysInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return("11 Roster 01 Complete", err)
def Insert_Roster_02(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/roster/'

    filename1 = directory2 + 'playerStats.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        df1 = df1.replace('', np.nan)
        df1['eventId'] = df1['eventId'].astype("int")
        df1['teamId'] = df1['teamId'].astype("int")
        df1['athleteId'] = df1['athleteId'].astype("int")
        df1['appearances'] = df1['appearances'].astype("int")
        df1['foulsCommitted'] = df1['foulsCommitted'].astype("int")
        df1['foulsSuffered'] = df1['foulsSuffered'].astype("int")
        df1['ownGoals'] = df1['ownGoals'].astype("int")
        df1['redCards'] = df1['redCards'].astype("int")
        df1['subIns'] = df1['subIns'].astype("int")
        df1['yellowCards'] = df1['yellowCards'].astype("int")
        df1['goalAssists'] = df1['goalAssists'].astype("int")
        df1['shotsOnTarget'] = df1['shotsOnTarget'].astype("int")
        df1['totalGoals'] = df1['totalGoals'].astype("int")
        df1['totalShots'] = df1['totalShots'].astype("int")
        df1['goalsConceded'] = df1['goalsConceded'].astype("int")
        df1['saves'] = df1['saves'].fillna(0).astype("int")
        df1['shotsFaced'] = df1['shotsFaced'].astype("int")
        df1['offsides'] = df1['offsides'].fillna(0).astype("int")
        df1['hasStats'] = df1['hasStats'].astype("bool")

        df1['updateTime'] = pd.to_datetime(df1['updateTime'])
        # print(df1.info())

        mask = df1.duplicated(subset=['eventId', 'teamId', 'athleteId'], keep="last")
        # print(df1[mask].to_string())
        df1.drop(df1[mask].index, inplace=True)

        # mask = df_officials.duplicated(subset = ['eventId', 'order'], keep=False)
        # print(df_officials[mask])
        # df_officials.drop(df_officials[mask].index, inplace = True)

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'PlayerStats'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.playerStatsInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return("12 Roster 02 Complete", err)
def Insert_Roster_03(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/roster/'

    filename1 = directory2 + 'teamRoster.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        # df1 = df1.replace('',np.nan)
        df1['eventId'] = df1['eventId'].astype("int")
        df1['teamId'] = df1['teamId'].astype("int")
        df1['homeAway'] = df1['homeAway'].fillna("").astype(object)
        df1['winner'] = df1['winner'].fillna("").astype("bool")
        df1['formation'] = df1['formation'].fillna("").astype(object)
        df1['uniformType'] = df1['uniformType'].fillna("").astype(object)
        df1['uniformColor'] = df1['uniformColor'].fillna("").astype(object)
        df1['athleteId'] = df1['athleteId'].fillna(999999).astype("int")
        df1['position'] = df1['position'].fillna("").astype(object)
        df1['formationPlace'] = df1['formationPlace'].fillna(0).astype("int")
        df1['hasStats'] = df1['hasStats'].astype("bool")
        df1['hasPlays'] = df1['hasPlays'].astype("bool")

        if 'subbedIn' in df1:
            df1['subbedIn'] = df1['subbedIn'].astype("bool")
        else:
            df1['subbedIn'] = False
        if 'subbedInForAthleteId' in df1:
            df1['subbedInForAthleteId'] = df1['subbedInForAthleteId'].fillna(0).astype("int")
        else:
            df1['subbedInForAthleteId'] = 0
        if 'subbedInForAthleteJersey' in df1:
            df1['subbedInForAthleteJersey'] = df1['subbedInForAthleteJersey'].fillna(-1).astype("int")
        else:
            df1['subbedInForAthleteJersey'] = -1

        if 'subbedOut' in df1:
            df1['subbedOut'] = df1['subbedOut'].astype("bool")
        else:
            df1['subbedOut'] = False
        if 'subbedOutForAthleteId' in df1:
            df1['subbedOutForAthleteId'] = df1['subbedOutForAthleteId'].fillna(0).astype("int")
        else:
            df1['subbedOutForAthleteId'] = 0
        if 'subbedOutForAthleteJersey' in df1:
            df1['subbedOutForAthleteJersey'] = df1['subbedOutForAthleteJersey'].fillna(-1).astype("int")
        else:
            df1['subbedOutForAthleteJersey'] = -1

        df1['jersey'] = df1['jersey'].replace("TM",-1)
        df1['jersey'] = df1['jersey'].fillna(-1).astype("int")

        if 'subbedIn.clock.value' in df1:
            df1['subbedIn.clock.value'] = df1['subbedIn.clock.value'].fillna(0).astype("int")
        else:
            df1['subbedIn.clock.value'] = 0
        if 'subbedOut.clock.value' in df1:
            df1['subbedOut.clock.value'] = df1['subbedOut.clock.value'].fillna(0).astype("int")
        else:
            df1['subbedOut.clock.value'] = 0
        if 'subbedIn.clock.displayValue' in df1:
            df1['subbedIn.clock.displayValue'] = df1['subbedIn.clock.displayValue'].fillna("").astype(object)
        else:
            df1['subbedIn.clock.displayValue'] = ""
        if 'subbedOut.clock.displayValue' in df1:
            df1['subbedOut.clock.displayValue'] = df1['subbedOut.clock.displayValue'].fillna("").astype(object)
        else:
            df1['subbedOut.clock.displayValue'] = ""
        df1['updateTime'] = pd.to_datetime(df1['updateTime'])
        #if 'subbedOutForAthleteId' in df1:
        #    df1.drop(columns=['subbedOutForAthleteId'])
        #if 'subbedOutForAthleteJersey' in df1:
        #    df1.drop(columns=['subbedOutForAthleteJersey'])
        # drop columns "subbedIn.*"
        # df1.drop(list(df1.filter(regex='subbedIn.')), axis=1, inplace=True)
        # drop columns "subbedOut.*"
        # df1.drop(list(df1.filter(regex='subbedOut.')), axis=1, inplace=True)
        df1.rename(columns={'subbedIn.clock.value':'subbedInClockValue'},inplace=True)
        df1.rename(columns={'subbedIn.clock.displayValue':'subbedInDisplayClock'},inplace=True)
        df1.rename(columns={'subbedOut.clock.value':'subbedOutClockValue'},inplace=True)
        df1.rename(columns={'subbedOut.clock.displayValue':'subbedOutDisplayClock'},inplace=True)

        df1_cols = ['eventId',
                    'teamId',
                    'uniformType',
                    'uniformColor',
                    'homeAway',
                    'winner',
                    'formation',
                    'active',
                    'starter',
                    'jersey',
                    'athleteId',
                    'athleteDisplayName',
                    'position',
                    'formationPlace',
                    'subbedIn',
                    'subbedInForAthleteId',
                    'subbedInForAthleteJersey',
                    'subbedInClockValue',
                    'subbedInDisplayClock',
                    'subbedOut',
                    'subbedOutForAthleteId',
                    'subbedOutForAthleteJersey',
                    'subbedOutClockValue',
                    'subbedOutDisplayClock',
                    'hasStats',
                    'hasPlays',
                    'updateTime']
        df1 = df1[df1_cols]

        mask = df1.duplicated(subset=['eventId', 'teamId', 'athleteId'], keep="last")
        # print(df1[mask].to_string())
        df1.drop(df1[mask].index, inplace=True)
        # print(df1.info())

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'TeamRoster'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.teamRosterInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return("13 Roster 03 Complete", err)
def Insert_Details(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/fixtures/'

    filename1 = directory2 + 'detailTypes.json'
    filename2 = directory2 + 'details.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df_detailTypes = sqlConn.importJsonToDf3(filename1)
    if not(df_detailTypes.empty):
        # print(df_detailTypes.to_string())
        df_detailTypes = df_detailTypes.rename(
            columns={"index": "id",
                     "name": "typeId",
                     "value": "typeText"}
        )
        df_detailTypes["typeId"] = df_detailTypes["typeId"].astype("int")
        # print(df_detailTypes.info())

        df_details = sqlConn.importJsonToDf(filename2)
        df_details["eventId"] = df_details["eventId"].astype("int")
        df_details["order"] = df_details["order"].astype("int")
        df_details["typeId"] = df_details["typeId"].astype("int")
        # df_details["active"] = df_details["active"].astype("bool")
        df_details["clockValue"] = df_details["clockValue"].astype("int")
        df_details["scoreValue"] = df_details["scoreValue"].astype("int")
        df_details["teamId"] = df_details["teamId"].fillna(-1).astype("int")
        df_details["athletesInvolved"] = df_details["athletesInvolved"].fillna(-1).astype("int")
        df_details_cols = ['eventId',
                           'order',
                           'typeId',
                           'typeText',
                           'clockValue',
                           'clockDisplayValue',
                           'scoringPlay',
                           'scoreValue',
                           'teamId',
                           'redCard',
                           'yellowCard',
                           'penaltyKick',
                           'ownGoal',
                           'shootout',
                           'athletesInvolved']
        df_details = df_details[df_details_cols]
        # print(df_details.info())

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'DetailTypes'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df_detailTypes["updateId"] = updateId
        msg = sqlConn.detailTypesInsertRecordSQL(osStr,conn,cursor, tableName1, df_detailTypes)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        tableName2 = 'Details'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
        df_details["updateId"] = updateId
        msg = sqlConn.detailsInsertRecordSQL(osStr,conn,cursor, tableName2, df_details)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName2 + " update insertion error"
        else:
            msg = tableName2 + " database insertion successful"
        print(msg)

        conn.close()
    return("14 Details Complete", err)
def Insert_PlayerStats(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/teams/'

    filename1 = directory2 + 'playerStatDB.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        df1 = df1.replace('', np.nan)
        # print(df1.info())
        df1['id'] = df1['id'].astype("int")
        df1['teamId'] = df1['teamId'].astype("int")
        df1['seasonType'] = df1['seasonType'].astype("int")
        df1['seasonYear'] = df1['seasonYear'].astype(object)
        df1['index'] = df1['index'].astype("int")

        df1['timestamp'] = pd.to_datetime(df1['timestamp'], utc=True)
        # print(df1.info())

        mask = df1.duplicated(subset=['id', 'teamId', 'seasonType'], keep="last")
        # print(df1[mask].to_string())
        df1.drop(df1[mask].index, inplace=True)

        # mask = df_officials.duplicated(subset = ['eventId', 'order'], keep=False)
        # print(df_officials[mask])
        # df_officials.drop(df_officials[mask].index, inplace = True)

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'PlayerStatsDB'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.playerStatsDBInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return("15 PlayerStats Complete", err)

def Insert_EventSnapshots(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/'

    filename1 = directory2 + 'eventSnapshots.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    df1 = sqlConn.importJsonToDf(filename1)
    if not(df1.empty):
        df1 = df1.replace('',np.nan)
        # print(df1.info())
        df1['eventId'] = df1['eventId'].astype("int")
        df1['eventErr'] = df1['eventErr'].astype("int")
        df1['hasHeader'] = df1['hasHeader'].astype("bool")
        df1['seasonYear'] = df1['seasonYear'].astype("int")
        df1['seasonType'] = df1['seasonType'].fillna(-1).astype("int")
        df1['seasonName'] = df1['seasonName'].fillna("").astype("object")
        df1['hasCompetitions'] = df1['hasCompetitions'].astype("bool")
        df1['hasStatus'] = df1['hasStatus'].astype("bool")
        df1['statusId'] = df1['statusId'].astype("int")
        df1['statusName'] = df1['statusName'].astype("object")
        df1['competitors'] = df1['competitors'].astype("int")
        df1['homeTeamId'] = df1['homeTeamId'].astype("int")
        df1['homeTeamName'] = df1['homeTeamName'].fillna("").astype("object")
        df1['homeTeamScore'] = df1['homeTeamScore'].astype("int")
        df1['homeTeamRecord'] = df1['homeTeamRecord'].fillna(-1).astype("int")
        df1['awayTeamId'] = df1['awayTeamId'].astype("int")
        df1['awayTeamName'] = df1['awayTeamName'].fillna("").astype("object")
        df1['awayTeamScore'] = df1['awayTeamScore'].astype("int")
        df1['awayTeamRecord'] = df1['awayTeamRecord'].fillna(-1).astype("int")
        df1['details'] = df1['details'].astype("int")
        df1['leagueId'] = df1['leagueId'].astype("int")
        df1['leagueName'] = df1['leagueName'].astype("object")
        df1['midsizeName'] = df1['midsizeName'].astype("object")
        df1['hasBoxscore'] = df1['hasBoxscore'].astype("bool")
        df1['nHomeStats'] = df1['nHomeStats'].astype("int")
        df1['nAwayStats'] = df1['nAwayStats'].astype("int")
        df1['hasGameInfo'] = df1['hasGameInfo'].astype("bool")
        df1['hasOdds'] = df1['hasOdds'].astype("bool")
        df1['hasRosters'] = df1['hasRosters'].astype("bool")
        df1['nHomePlayers'] = df1['nHomePlayers'].astype("int")
        df1['nAwayPlayers'] = df1['nAwayPlayers'].astype("int")
        df1['keyEvents'] = df1['keyEvents'].astype("int")
        df1['commentary'] = df1['commentary'].astype("int")
        df1['standings'] = df1['standings'].astype("int")

        df1['snapshotTime'] = pd.to_datetime(df1['snapshotTime'], utc=True)
        df1['matchDate'] = pd.to_datetime(df1['matchDate'], utc=True)
        # print(df1.info())

        mask = df1.duplicated(subset=['eventId'], keep="last")
        # print(df1[mask].to_string())
        df1.drop(df1[mask].index, inplace=True)

        # mask = df_officials.duplicated(subset = ['eventId', 'order'], keep=False)
        # print(df_officials[mask])
        # df_officials.drop(df_officials[mask].index, inplace = True)

        if osStr == "Windows":
            (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
        # print(conn)

        tableName1 = 'EventSnapshots'
        (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
        df1["updateId"] = updateId
        msg = sqlConn.eventSnapshotsInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
        msg, errFlag = sqlConn.updateLog(osStr,conn,cursor, msg)
        if errFlag < 0:
            err = errFlag
            msg = tableName1 + " update insertion error"
        else:
            msg = tableName1 + " database insertion successful"
        print(msg)

        conn.close()
    return("18 EventSnapshots Complete", err)
def Insert_PlayerDBTransferMarket(rootDir, rootDir2,dataSet, dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/transfer market/'

    filename1 = directory2 + 'players.csv'
    filename2 = directory2 + 'matched_players.json'

    defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    defaultTimeZone = tz.gettz("UTC")
    defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
    print(defaultTime)

    df1 = sqlConn.importCsvToDf(filename1)
    df1['player_id'] = df1['player_id'].astype("int")
    df1['first_name'] = df1['first_name'].fillna("").astype(object)
    df1['last_name'] = df1['last_name'].fillna("").astype(object)
    df1['name'] = df1['name'].fillna("").astype(object)
    df1['last_season'] = df1['last_season'].astype("int")
    df1['current_club_id'] = df1['current_club_id'].astype("int")
    df1['player_code'] = df1['player_code'].fillna("").astype(object)
    df1['country_of_birth'] = df1['country_of_birth'].fillna("").astype(object)
    df1['city_of_birth'] = df1['city_of_birth'].fillna("").astype(object)
    df1['country_of_citizenship'] = df1['country_of_citizenship'].fillna("").astype(object)
    df1['date_of_birth'] = df1['date_of_birth'].fillna("").astype(object)
    df1['sub_position'] = df1['sub_position'].fillna("").astype(object)
    df1['position'] = df1['position'].fillna("").astype(object)
    df1['foot'] = df1['foot'].fillna("").astype(object)
    df1['height_in_cm'] = df1['height_in_cm'].fillna(0).astype(int)
    df1['contract_expiration_date'] = pd.to_datetime(df1['contract_expiration_date'], utc=True)
    df1['contract_expiration_date'] = df1['contract_expiration_date'].fillna(defaultTime)
    df1['agent_name'] = df1['agent_name'].fillna("").astype(object)
    df1['image_url'] = df1['image_url'].fillna("").astype(object)
    df1['url'] = df1['url'].fillna("").astype(object)
    df1['current_club_domestic_competition_id'] = df1['current_club_domestic_competition_id'].fillna("").astype(object)
    df1['current_club_name'] = df1['current_club_name'].fillna("").astype(object)
    df1['market_value_in_eur'] = df1['market_value_in_eur'].fillna(0).astype("int")
    df1['highest_market_value_in_eur'] = df1['highest_market_value_in_eur'].fillna(0).astype("int")
    df1 = df1.sort_values(by=['player_id']).reset_index(drop=True)
    # inx=df1['id']
    # mask1=inx.duplicated(keep="first")
    # print(df1[mask1])
    # df1.drop(df1[mask1].index, inplace = True)
    print(df1.info())

    # mask = df1.duplicated(subset = ['id'], keep=False)
    # print(df1[mask])
    # df1.drop(df1[mask].index, inplace = True)

    df2 = sqlConn.importJsonToDf(filename2)
    df2['playerId'] = df2['playerId'].astype("int")
    df2['player_id_TM'] = df2['player_id_TM'].astype("int")
    # df2=df2.rename(columns={"matchedScore":"fuzzyScore"})
    df2['fuzzyScore'] = df2['fuzzyScore'].astype("int")
    print(df2.info())

    hostName = dbConnect["hostName"]
    userId = dbConnect["userId"]
    pwd = dbConnect["pwd"]
    odbcDriver = dbConnect["odbcDriver"]
    dbName = dbConnect["dbName"]
    osStr = dbConnect["osStr"]

    if osStr == "Windows":
        (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    print(conn)

    tableName1 = 'PlayerDBTM'
    (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName1, dataSet)
    df1["updateId"] = updateId
    msg = sqlConn.playerDBTMInsertRecordSQL(osStr,conn,cursor, tableName1, df1)
    msg = sqlConn.updateLog(osStr,conn,cursor, msg)
    print(msg)

    tableName2 = 'PlayerIdTM'
    (updateId, updateTime) = sqlConn.getUpdateIdSQL(osStr,conn,cursor, tableName2, dataSet)
    df2["updateId"] = updateId
    msg = sqlConn.playerIdTMInsertRecordSQL(osStr,conn,cursor, tableName2, df2)
    msg = sqlConn.updateLog(osStr,conn,cursor, msg)
    print(msg)

    conn.close()
    return ("19 PlayerDBTransferMarket Complete")
def Insert_teamTMValue(rootDir,rootDir2,dataSet,dbConnect):
    directory1 = rootDir
    directory2 = rootDir2 + 'tables/teams/'
    err = 0
    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']
    return("20 teamTMValue Complete", err)

def Install_All(dbConnect,dataSet,rootDir,rootDir2,nStart,nEnd):

    startDate = dataSet['startDate']
    endDate = dataSet['endDate']
    extractionDate = dataSet['extractionDate']

    dataSet = startDate + "-" + endDate + "-" + extractionDate

    description = "Insert Tables"

    print()
    print(dataSet, description)
    #nStart = 1
    #nEnd = 16
    errInsert = 0
    if nStart == 1:
        print()
        msg, err = Insert_Status_Season(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart ==2:
        print()
        msg, err = Insert_Teams(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 3:
        print()
        msg, err = Insert_Fixtures(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 4:
        print()
        msg, err = Insert_TeamStats(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 5:
        print()
        msg, err = Insert_GameInfo(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 6:
        print()
        msg, err = Insert_Standings(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 7:
        print()
        msg, err = Insert_Athletes(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 8:
        print()
        msg, err = Insert_PlayerInTeam(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 9:
        print()
        msg, err = Insert_KeyEvents_01(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 10:
        print()
        msg, err = Insert_KeyEvents_02(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 11:
        print()
        msg, err = Insert_KeyEvents_03(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 12:
        print()
        msg, err = Insert_Details(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 13:
        print()
        msg, err = Insert_PlayerStats(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 14:
        print()
        msg, err = Insert_Roster_01(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 15:
        print()
        msg, err = Insert_Roster_02(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 16:
        print()
        msg, err = Insert_Roster_03(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 17:
        print()
        msg, err = Insert_Participants(rootDir, rootDir2, dataSet, dbConnect)
        if err < 0:
            errInsert = -1
        print(description,nStart,msg)
        if nEnd > nStart:
            nStart += 1
    if nStart == 18:
        print()
        if errInsert == 0:
            msg, err = Insert_EventSnapshots(rootDir, rootDir2, dataSet, dbConnect)
            if err < 0:
                errInsert = -1
            print(description,nStart,msg)
        else:
            print("database insertion error.  EventSnapShots not updated!")
        if nEnd > nStart:
            nStart += 1
    #if nStart == 20:
    #    print()
    #    msg, err = Insert_teamTMValue(rootDir, rootDir2, dataSet, dbConnect)
    #    print(description,nStart,msg)
    #    if nEnd > nStart:
    #        nStart += 1
    else:
        print("Nothing to run")
    msg = "Database Update Complete"
    return(msg)
