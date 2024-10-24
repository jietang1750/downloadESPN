import sqlConn
import pyodbc
import pandas as pd
import json
from datetime import datetime,timezone,date, timedelta
import os
from pathlib import Path
import math
from shutil import copyfile
import csv
import ESPNSoccer
import sql_insert_all
def listIntersect(list1, list2):
    list3 = list(set(list1) & set(list2))
    list4 = list(set(list3) ^ set(list2))
    return(list3,list4)
def clockDisplayValueToMinutes(strClockDisplayValue):
    if '+' in strClockDisplayValue:
        regPlayTime = int(strClockDisplayValue.split('+')[0].strip("'"))
        injPlayTime = int(strClockDisplayValue.split('+')[1].strip("'"))
    else:
        regPlayTime = int(strClockDisplayValue.strip("'"))
        injPlayTime = 0
    # print(tmpPlay['clock']['displayValue'],regPlayTime,injPlayTime)
    return(regPlayTime,injPlayTime)

def convClubname1(clubNameFilename, myEncoding):
    nameDict = {}
    with open(clubNameFilename, "r", encoding=myEncoding) as csvfile:
        names = csv.reader(csvfile)
        i = 0
        for line_names in names:
            if i == 0:
                header = line_names
                i += 1
            else:
                excel = line_names[0]
                espn = line_names[1]
                nameDict[espn] = excel
                # print(excel,espn)
    # print(nameDict)
    return(nameDict)
def getFixture(mysqlDict,seasonType,fromZone,strTimeFormatIn,toZone,strTimeFormatOut):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    sql_fixture1 = ("SET @rownum:=0;")
    sql_fixture2 = ("SELECT"
                    "     @rownum:=@rownum + 1 No, x.*"
                    " FROM"
                    "     (SELECT"
                    "         f1.seasonType,"
                    "             f1.eventId,"
                    "             f1.date,"
                    "             CONCAT(t1.displayName, ' vs ', t2.displayName) AS Title,"
                    "             v.fullName AS Venue,"
                    "             f1.attendance,"
                    "             t1.displayName AS homeTeam,"
                    "             t2.displayName AS awayTeam,"
                    "             f1.homeTeamScore,"
                    "             f1.awayTeamScore,"
                    "             f1.homeTeamShootoutScore,"
                    "             f1.awayTeamShootoutScore,"
                    "             f1.displayClock,"
                    "             st.name,"
                    "             f1.updateTime"
                    "     FROM"
                    "         Fixtures f1"
                    "     Left JOIN Leagues l ON f1.leagueId = l.id"
                    "     INNER JOIN StatusType st ON f1.statusId = st.id"
                    "     INNER JOIN Venues v ON f1.venueId = v.id"
                    "     INNER JOIN Teams t1 ON f1.homeTeamId = t1.teamId"
                    "     INNER JOIN Teams t2 ON f1.awayTeamId = t2.teamId"
                    "     WHERE"
                    "         f1.seasonType = %s) x"
                    " ORDER BY date ASC;")
    fixtures = {}
    err = 0
    try:
        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        # print(conn)

        val = (seasonType,)
        cursor.execute(sql_fixture1)
        cursor.execute(sql_fixture2, val)
        rs = cursor.fetchall()
        conn.close()
        for row in rs:
            tmpEventId = row[2]
            matchDateUTC = row[3].strftime(strTimeFormatIn)
            matchDateTimeESTStr = ESPNSoccer.tzConvert(
                matchDateUTC, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
            )
            fixtures[tmpEventId] = {
                "no": row[0],
                "seasonType": row[1],
                "eventId": row[2],
                "matchDateUTC": row[3],
                "dateEST": matchDateTimeESTStr,
                "matchTitle": row[4],
                "venue": row[5],
                "attendance": row[6],
                "homeTeam": row[7],
                "awayTeam": row[8],
                "homeScore": row[9],
                "awayScore": row[10],
                "homeShootoutScore": row[11],
                "awayShootoutScore": row[12],
                "fullGameDisplayClock": row[13],
                "status": row[14],
                "updateTimeUTC": row[15]
            }
    except Exception as e:
        print("getFixture error")
        print(e)
        err = -1

    return(fixtures,err)

def getAllFixture(mysqlDict,seasonType,startDate):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = ("SELECT DISTINCT "
            "x.eventId,"
            "x.date AS matchDateUTC,"
            "l.name AS leagueName,"
            "v.fullName AS venue,"
            "at.attendance,"
            "t1.name AS homeTeam,"
            "t2.name AS awayTeam,"
            "x.homeTeamScore,"
            "x.homeTeamShootoutScore,"
            "x.awayTeamScore,"
            "x.awayTeamShootoutScore,"
            "st.name AS status,"
            "x.updateTime"
        " FROM ("
        "(SELECT *"
        " FROM Fixtures f1"
        " JOIN (SELECT teamId"
        " FROM "
        " TeamsInLeague "
        " WHERE "
        " seasonType = %s ) a"
        " ON a.teamId = f1.homeTeamid)"
        " UNION ALL "
        " (SELECT *"
        " FROM Fixtures f2"
        " JOIN (SELECT teamId"
        " FROM TeamsInLeague "
        " WHERE "
        " seasonType = %s) b"
        " ON b.teamId = f2.awayTeamId)) x"
        " JOIN Teams t1 on t1.teamId = x.homeTeamId"
        " JOIN Teams t2 on t2.teamId = x.awayTeamId"
        " LEFT JOIN Leagues l on l.id = x.leagueId"
        " LEFT JOIN Venues v on v.id = x.venueId"
        " LEFT JOIN StatusType st on st.id = x.statusId"
        " LEFT JOIN Attendance at on at.eventId = x.eventId"
        " WHERE x.date > %s"
        " ORDER BY x.date;"
            )
    allFixtures = {}
    err = 0
    try:
        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        # print(conn)

        val = (seasonType,seasonType,startDate)
        cursor.execute(sql1, val)
        rs = cursor.fetchall()
        conn.close()
        for row in rs:
            tmpEventId = row[0]
            allFixtures[tmpEventId] = {
                "eventId": tmpEventId,
                "matchDateUTC": row[1],
                "leagueName": row[2],
                "venue": row[3],
                "attendance": row[4],
                "homeTeam": row[5],
                "awayTeam": row[6],
                "homeScore": row[7],
                "awayScore": row[8],
                "homeShootoutScore": row[9],
                "awayShootoutScore": row[10],
                "status": row[11],
                "updateTimeUTC": row[12]
            }
    except Exception as e:
        print("getAllFixture error")
        print(e)
        print(sql1)
        err = -1

    return(allFixtures,err)
def getLineup(mysqlDict,eventId):
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

    # eventIdList = [704288]

    sql1 = f"""
            SELECT 
            a.eventId,
            a.teamId,
            t.name as teamName,
            a.homeAway,
            a.athleteId,
            a.athleteDisplayName,
            a.starter,
            a.jersey,
            a.position,
            a.formation,
            if(a.formationPlace = 0, 99, a.formationPlace) as formationPlace,
            f.clock,
            f.displayClock,
            IFNULL(b.clockValue, 0) AS clockValue,
            IFNULL(b.clockDisplayValue, '') AS clockDisplayValue,
            if(a.subbedIn = 1, "Subbed In For","") AS subbedIn,
            IFNULL(c.athleteDisplayName, '') AS subbedInForAthleteName,
            c.jersey AS subbedInForAthleteJersey,
            if(a.subbedOut = 1, "Subbed Out By","") AS subbedOut,
            IFNULL(d.athleteDisplayName, '') AS subbedOutForAthleteName,
            d.jersey AS subbedOutForAthleteJersey,
            a.updateTime
        FROM
            (SELECT 
                *
            FROM
                TeamRoster
            WHERE
                eventId = {eventId}) a
                LEFT JOIN 
            (SELECT 
                k1.eventId,
                    k1.teamId,
                    k1.teamDisplayName,
                    k1.clockValue,
                    k1.clockDisplayValue,
                    k2.participant,
                    k2.keyEventOrder,
                    t.athleteId
            FROM
                KeyEvents k1
            LEFT JOIN KeyEventParticipants k2 ON k1.keyEventId = k2.keyEventId
            LEFT JOIN TeamRoster t ON t.eventId = k1.eventId
                AND t.athleteDisplayName = k2.participant
            WHERE
                k1.eventId = {eventId} AND k1.typeId = 76) b ON a.athleteId = b.athleteId
                LEFT JOIN
            (SELECT 
                t1.athleteId, t1.athleteDisplayName, t1.jersey
            FROM
                TeamRoster t1
            WHERE
                eventId = {eventId}) c ON c.athleteId = a.subbedInForAthleteId
                LEFT JOIN
            (SELECT 
                t2.athleteId, t2.athleteDisplayName,t2.jersey
            FROM
                TeamRoster t2
            WHERE
                eventId = {eventId}) d ON d.athleteId = a.subbedOutForAthleteId
                JOIN
            Fixtures f ON f.eventId = a.eventId
                JOIN
            Teams t ON t.teamId = a.teamId
        ORDER BY a.eventId, a.homeAway DESC , a.starter DESC , formationPlace , subbedIn DESC , b.clockValue
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    lineUp = []
    for item in rs:
        i += 1
        eventId = item[0]
        teamId = item[1]
        teamName = item[2]
        homeAway = item[3]
        athleteId = item[4]
        player = item[5]
        starter = item[6]
        jersey = item[7]
        position = item[8]
        formation = item[9]
        formationPlace = item[10]
        gameClock = item[11]
        gameClockDisplay = item[12]
        subClock = item[13]
        subClockDisplay = item[14]
        subbedIn = item[15]
        subbedInForAthleteName = item[16]
        subbedInForAthleteJersey = item[17]
        subbedOut = item[18]
        subbedOutByAthleteName = item[19]
        subbedOutByAthleteJersey = item[20]
        updateTime = item[21]
        tmpLineUp = {
            "eventId":eventId,
            "teamId": teamId,
            "teamName": teamName,
            "homeAway": homeAway,
            "athleteId": athleteId,
            "athleteName": player,
            "starter": starter,
            "jersey": jersey,
            "position": position,
            "formation": formation,
            "formationPlace": formationPlace,
            "gameClockValue": gameClock,
            "gameClockDisplay": gameClockDisplay,
            "subClockValue": subClock,
            "subClockDisplay": subClockDisplay,
            "subbedIn": subbedIn,
            "subbedInForAthleteName": subbedInForAthleteName,
            "subbedInForAthleteJersey": subbedInForAthleteJersey,
            "subbedOut": subbedOut,
            "subbedOutByAthleteName": subbedOutByAthleteName,
            "subbedOutByAthleteJersey": subbedOutByAthleteJersey,
            "updateTime": updateTime
        }
        lineUp.append(tmpLineUp)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(lineUp, err)
def getEventsTeams(mysqlDict,seasonType):
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

    # eventId = 704280

    sql1 = f"""
            SELECT eventId,statusId,homeTeamId,awayTeamId
            From Fixtures
            where seasonType = {seasonType} 
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    events = []
    teams =[]
    for item in rs:
        i += 1
        eventId = item[0]
        statusId = item[1]
        homeTeamId = item[2]
        awayTeamId = item[3]
        if statusId ==28:
            events.append(eventId)
        if homeTeamId not in teams:
            teams.append(homeTeamId)
        if awayTeamId not in teams:
            teams.append(awayTeamId)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(events, teams,err)
def getPlayerStats(mysqlDict,seasonType, teamId):
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

    # eventId = 704280

    sql1 = f"""
            SELECT 
            p1.teamId,
            t.name,
            a.fullName,
            p1.id,
            p1.playerIndex,
            p1.appearances_value,
            p1.foulsCommitted_value,
            p1.foulsSuffered_value,
            p1.goalAssists_value,
            p1.goalsConceded_value,
            p1.offsides_value,
            p1.ownGoals_value,
            p1.redCards_value,
            p1.saves_value,
            p1.shotsFaced_value,
            p1.shotsOnTarget_value,
            p1.subIns_value,
            p1.totalGoals_value,
            p1.totalShots_value,
            p1.yellowCards_value
        FROM
            PlayerStatsDB p1
                JOIN
            Teams t ON t.teamId = p1.teamId
                JOIN
            Athletes a ON a.id = p1.id
        WHERE
            p1.seasonType = {seasonType} 
                AND p1.teamId = {teamId} 
        order by p1.appearances_value desc,p1.totalGoals_value desc;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    playerStats = []
    for row in rs:
        i += 1
        teamId = row[0]
        teamName = row[1]
        athleteName = row[2]
        athleteId = row[3]
        playerIndex = row[4]
        appearances = row[5]
        foulsCommitted = row[6]
        foulsSuffered = row[7]
        goalAssists = row[8]
        goalsConceded = row[9]
        offsides = row[10]
        ownGoals = row[11]
        redCards = row[12]
        saves = row[13]
        shotsFaced = row[14]
        shotsOnTarget = row[15]
        subIns = row[16]
        totalGoals = row[17]
        totalShots = row[18]
        yellowCards = row[19]
        tmpPlayerStat = {
            "teamId": teamId,
            "teamName": teamName,
            "athleteName": athleteName,
            "athleteId": athleteId,
            "playerIndex": playerIndex,
            "appearances": appearances,
            "foulsCommitted": foulsCommitted,
            "foulsSuffered": foulsSuffered,
            "goalAssists": goalAssists,
            "goalsConceded": goalsConceded,
            "offsides": offsides,
            "ownGoals": ownGoals,
            "redCards": redCards,
            "saves": saves,
            "shotsFaced": shotsFaced,
            "shotsOnTarget": shotsOnTarget,
            "subIns": subIns,
            "totalGoals": totalGoals,
            "totalShots": totalShots,
            "yellowCards": yellowCards
        }
        playerStats.append(tmpPlayerStat)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(playerStats, err)
def getPlayerStats1(mysqlDict,seasonType, teamIdList):
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

    # eventId = 704280

    sql1 = f"""
            SELECT 
            p1.teamId,
            t.name,
            a.fullName,
            p1.id,
            p1.playerIndex,
            p1.appearances_value,
            p1.foulsCommitted_value,
            p1.foulsSuffered_value,
            p1.goalAssists_value,
            p1.goalsConceded_value,
            p1.offsides_value,
            p1.ownGoals_value,
            p1.redCards_value,
            p1.saves_value,
            p1.shotsFaced_value,
            p1.shotsOnTarget_value,
            p1.subIns_value,
            p1.totalGoals_value,
            p1.totalShots_value,
            p1.yellowCards_value
        FROM
            PlayerStatsDB p1
                JOIN
            Teams t ON t.teamId = p1.teamId
                JOIN
            Athletes a ON a.id = p1.id
        WHERE
            p1.seasonType = {seasonType} 
                AND p1.teamId in {tuple(teamIdList)} 
        order by p1.appearances_value desc,p1.totalGoals_value desc;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    playerStats = []
    for row in rs:
        i += 1
        teamId = row[0]
        teamName = row[1]
        athleteName = row[2]
        athleteId = row[3]
        playerIndex = row[4]
        appearances = row[5]
        foulsCommitted = row[6]
        foulsSuffered = row[7]
        goalAssists = row[8]
        goalsConceded = row[9]
        offsides = row[10]
        ownGoals = row[11]
        redCards = row[12]
        saves = row[13]
        shotsFaced = row[14]
        shotsOnTarget = row[15]
        subIns = row[16]
        totalGoals = row[17]
        totalShots = row[18]
        yellowCards = row[19]
        tmpPlayerStat = {
            "teamId": teamId,
            "teamName": teamName,
            "athleteName": athleteName,
            "athleteId": athleteId,
            "playerIndex": playerIndex,
            "appearances": appearances,
            "foulsCommitted": foulsCommitted,
            "foulsSuffered": foulsSuffered,
            "goalAssists": goalAssists,
            "goalsConceded": goalsConceded,
            "offsides": offsides,
            "ownGoals": ownGoals,
            "redCards": redCards,
            "saves": saves,
            "shotsFaced": shotsFaced,
            "shotsOnTarget": shotsOnTarget,
            "subIns": subIns,
            "totalGoals": totalGoals,
            "totalShots": totalShots,
            "yellowCards": yellowCards
        }
        playerStats.append(tmpPlayerStat)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(playerStats, err)

def getTeamStats(mysqlDict,seasonType):
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

    # eventId = 704280

    sql1 = f"""
    SELECT 
    ts.eventId,
    ts.teamId,
    t.name AS teamName,
    IF(ts.teamOrder = 0, 'home', 'away') as homeAway,
    ts.foulsCommitted,
    ts.yellowCards,
    ts.redCards,
    ts.offsides,
    ts.wonCorners,
    ts.saves,
    ts.possessionPct,
    ts.totalShots,
    ts.shotsOnTarget,
    ts.shotPct,
    ts.penaltyKickGoals,
    ts.penaltyKickShots,
    ts.accuratePasses,
    ts.totalPasses,
    ts.passPct,
    ts.accurateCrosses,
    ts.totalCrosses,
    ts.crossPct,
    ts.totalLongBalls,
    ts.accurateLongBalls,
    ts.longballPct,
    ts.blockedShots,
    ts.effectiveTackles,
    ts.totalTackles,
    ts.tacklePct,
    ts.interceptions,
    ts.effectiveClearance,
    ts.totalClearance
FROM
    TeamStats ts
        JOIN
    Fixtures f ON f.eventId = ts.eventId
        JOIN
    Teams t ON t.teamId = ts.teamId
WHERE
    f.seasonType = {seasonType} AND f.statusId = 28
ORDER BY ts.eventId , ts.teamOrder;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    teamStats = []
    for row in rs:
        i += 1
        eventId = row[0]
        teamId = row[1]
        teamName = row[2]
        homeAway = row[3]
        foulsCommitted = row[4]
        yellowCards = row[5]
        redCards = row[6]
        offsides = row[7]
        wonCorners = row[8]
        saves = row[9]
        possessionPct = row[10]
        totalShots = row[11]
        shotsOnTarget = row[12]
        shotPct = row[13]*100.0
        penaltyKickGoals = row[14]
        penaltyKickShots = row[15]
        accuratePasses = row[16]
        totalPasses = row[17]
        passPct = row[18]*100.0
        accurateCrosses = row[19]
        totalCrosses = row[20]
        crossPct = row[21]*100.0
        totalLongBalls = row[22]
        accurateLongBalls = row[23]
        longballPct = row[24]*100.0
        blockedShots = row[25]
        effectiveTackles = row[26]
        totalTackles = row[27]
        tacklePct = row[28]*100.0
        interceptions = row[29]
        effectiveClearance = row[30]
        totalClearance = row[31]
        tmpTeamStat = {
            "eventId": eventId,
            "teamId": teamId,
            "team": teamName,
            "homeAway": homeAway,
            "foulsCommitted": foulsCommitted,
            "yellowCards": yellowCards,
            "redCards": redCards,
            "offsides": offsides,
            "wonCorners": wonCorners,
            "saves": saves,
            "possessionPct": possessionPct,
            "totalShots": totalShots,
            "shotsOnTarget": shotsOnTarget,
            "shotPct": shotPct,
            "penaltyKickGoals": penaltyKickGoals,
            "penaltyKickShots": penaltyKickShots,
            "accuratePasses": accuratePasses,
            "totalPasses": totalPasses,
            "passPct": passPct,
            "accurateCrosses": accurateCrosses,
            "totalCrosses": totalCrosses,
            "crossPct": crossPct,
            "totalLongBalls": totalLongBalls,
            "accurateLongBalls": accurateLongBalls,
            "longballPct": longballPct,
            "blockedShots": blockedShots,
            "effectiveTackles": effectiveTackles,
            "totalTackles": totalTackles,
            "tacklePct": tacklePct,
            "interceptions": interceptions,
            "effectiveClearance": effectiveClearance,
            "totalClearance": totalClearance
        }
        teamStats.append(tmpTeamStat)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(teamStats, err)
def getPlays(mysqlDict,eventList,strTimeFormat):
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

    # eventId = 704280

    sql1 = f"""
            SELECT 
            p.eventId,
            p.playOrder,
            p.playId,
            p.teamDisplayName,
            pp.participant,
            tr.jersey,
            pp.playOrder as participantOrder,
            p.text,
            p.shortText,
            p.period,
            p.clockValue,
            p.clockDisplayValue,
            p.scoringPlay,
            p.shootout,
            p.wallclock,
            u.updateDateTime
        FROM
            Plays p
                LEFT JOIN
            PlayParticipants pp ON p.eventId = pp.eventId
                AND p.playId = pp.playId
                LEFT JOIN
            TeamRoster tr ON tr.eventId = p.eventId
                AND tr.athleteDisplayName = pp.participant
                JOIN
            UpdateId u ON u.updateId = p.updateId
        WHERE
            p.eventId in {tuple(eventList)}
        ORDER BY p.playOrder;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    i = 0
    plays = []
    for row in rs:
        i += 1
        eventId = row[0]
        playOrder = row[1]
        playId = row[2]
        teamName = row[3]
        playerName = row[4]
        playerJersey = row[5]
        participantOrder = row[6]
        text = row[7]
        clockDisplay = row[11]
        updateTime = row[15].strftime(strTimeFormat)
        tmpPlay = {
            "eventId": eventId,
            "team": teamName,
            "playOrder": playOrder,
            "playerName": playerName,
            "playerJersey": playerJersey,
            "participantOrder": participantOrder,
            "text": text,
            "clockDisplay": clockDisplay,
            "updateTime":updateTime
        }
        plays.append(tmpPlay)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(plays, err)
def getSeasonInfo(mysqlDict,year,league):
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
            a.*, b.nTotMatches, st.name as seasonName
        FROM
            (SELECT 
                seasonType,
                    MAX(year) AS year,
                    MAX(leagueId) AS leagueId,
                    MAX(midsizeLeagueName) AS midsizeName,
                    COUNT(teamId) AS nTeams
            FROM
                TeamsInLeague
            GROUP BY seasonType) a
                JOIN
            (SELECT 
                COUNT(eventId) AS nTotMatches, seasonType
            FROM
                Fixtures
            GROUP BY seasonType) b ON a.seasonType = b.seasonType
            join
            SeasonType st on st.typeId = b.seasonType
            WHERE a.year = %s AND a.midsizeName = %s
            ;
        """
    # print(sql1)
    val = (year,league,)
    cursor.execute(sql1, val,multi=True)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    plays = []
    row = rs[0]
    seasonType = row[0]
    year = row[1]
    leagueId = row[2]
    midsizeName = row[3]
    nTeams = row[4]
    nTotMatches = row[5]
    seasonName = row[6]
    season = {
        "year": year,
        "seasonType": seasonType,
        "seasonName": seasonName,
        "midsizeName": midsizeName,
        "leagueId": leagueId,
        "nTeams": nTeams,
        "nTotMatches": nTotMatches
    }
    conn.close()
    return(season, err)
def getStandings(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    sql1 = ("SELECT "
            "    a.teamRank,"
            "    a.teamId,"
            "    t1.name,"
            "    a.Points,"
            "    a.MP,"
            "    a.Win,"
            "    a.Draw,"
            "    a.Loss,"
            "    a.GF,"
            "    a.GA,"
            "    a.GD,"
            "    (IFNULL(homeCleanSheets, 0) + IFNULL(awayCleanSheets, 0)) AS cleanSheets,"
            "    a.Deductions,"
            "    a.Streak,"
            "    a.homePoints,"
            "    a.homeMP,"
            "    a.homeWin,"
            "    a.homeDraw,"
            "    a.homeLoss,"
            "    a.homeGF,"
            "    a.homeGA,"
            "    a.homeGD,"
            "    IFNULL(c2.homeCleanSheets, 0) AS homeCleanSheets,"
            "    a.awayPoints,"
            "    a.awayMP,"
            "    a.awayWin,"
            "    a.awayDraw,"
            "    a.awayLoss,"
            "    a.awayGF,"
            "    a.awayGA,"
            "    a.awayGD,"
            "    IFNULL(c1.awayCleanSheets, 0) AS awayCleanSheets,"
            "    a.updateTime"
            "    FROM "
            "    (SELECT "
            "        s1.teamId,"
            "            s1.teamRank,"
            "            s1.points AS Points,"
            "            s1.gamesPlayed AS MP,"
            "            s1.wins AS Win,"
            "            s1.ties AS Draw,"
            "            s1.losses AS Loss,"
            "            s1.pointsFor AS GF,"
            "            s1.pointsAgainst AS GA,"
            "            s1.pointDifferential AS GD,"
            "            s1.deductions AS Deductions,"
            "            s1.streak AS Streak,"
            "            (s1.homeWins * 3 + s1.homeTies) AS homePoints,"
            "            s1.homeGamesPlayed AS homeMP,"
            "            s1.homeWins AS homeWin,"
            "            s1.homeTies AS homeDraw,"
            "            s1.homeLosses AS homeLoss,"
            "            s1.homePointsFor AS homeGF,"
            "            s1.homePointsAgainst AS homeGA,"
            "            (s1.homePointsFor - s1.homePointsAgainst) AS homeGD,"
            "            (s1.awayWins * 3 + s1.awayTies) AS awayPoints,"
            "            s1.awayGamesPlayed AS awayMP,"
            "            s1.awayWins AS awayWin,"
            "            s1.awayTies AS awayDraw,"
            "            s1.awayLosses AS awayLoss,"
            "            s1.awayPointsFor AS awayGF,"
            "            s1.awayPointsAgainst AS awayGA,"
            "            (s1.awayPointsFor - s1.awayPointsAgainst) AS awayGD,"
            "            s1.timeStamp AS updateTime"
            "    FROM "
            "        Standings s1"
            "    WHERE "
            "        seasonType = %s) a"
            "        LEFT JOIN "
            "    (SELECT "
            "        awayTeamId, COUNT(awayTeamId) AS awayCleanSheets"
            "    FROM "
            "        Fixtures "
            "    WHERE "
            "        seasonType = %s AND statusId = 28"
            "            AND homeTeamScore = 0"
            "    GROUP BY awayTeamId) c1 ON c1.awayTeamId = a.teamId"
            "        LEFT JOIN "
            "    (SELECT "
            "        homeTeamId, COUNT(homeTeamId) AS homeCleanSheets"
            "    FROM "
            "        Fixtures "
            "    WHERE "
            "        seasonType = %s AND statusId = 28"
            "            AND awayTeamScore = 0"
            "    GROUP BY homeTeamId) c2 ON c2.homeTeamId = a.teamId"
            "        JOIN "
            "    Teams t1 ON t1.teamId = a.teamId"
            " ORDER BY a.teamRank;")
    standings = {}
    err = 0
    try:
        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        # print(conn)

        val = (seasonType,seasonType,seasonType,)
        cursor.execute(sql1, val)
        rs = cursor.fetchall()
        conn.close()
        for row in rs:
            tmpTeamId = row[1]
            standings[tmpTeamId] = {
                "teamRank": row[0],
                "teamId": tmpTeamId,
                "teamName": row[2],
                "Points": row[3],
                "MP": row[4],
                "Win": row[5],
                "Draw": row[6],
                "Loss": row[7],
                "GF": row[8],
                "GA": row[9],
                "GD": row[10],
                "cleanSheets": row[11],
                "Deductions": row[12],
                "Streak": row[13],
                "homePoints": row[14],
                "homeMP": row[15],
                "homeWin": row[16],
                "homeDraw": row[17],
                "homeLoss": row[18],
                "homeGF": row[19],
                "homeGA": row[20],
                "homeGD": row[21],
                "homeCleanSheets":row[22],
                "awayPoints": row[23],
                "awayMP": row[24],
                "awayWin": row[25],
                "awayDraw": row[26],
                "awayLoss": row[27],
                "awayGF": row[28],
                "awayGA": row[29],
                "awayGD": row[30],
                "awayCleanSheets":row[31],
                "updateTimeUTC": row[32]
            }
    except Exception as e:
        print("getStandings error")
        print(e)
        err = -1

    return(standings,err)

def getPlayerInfo(mysqlDict,athleteIdList):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    sql1 = f"""
        SELECT 
            id,
            displayName,
            weight AS weight_lb,
            height AS height_in,
            age,
            dateOfBirth,
            citizenship,
            positionName
        FROM
            PlayerDB
        WHERE
            id in {tuple(athleteIdList)};
        """
    playerInfo = []
    err = 0
    try:
        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        # print(conn)

        cursor.execute(sql1)
        rs = cursor.fetchall()
        conn.close()
        for row in rs:
            tmpAthleteId = row[0]
            tmpAthleteName = row[1]
            tmpWeightLb = row[2]
            tmpHeightIn = row[3]
            tmpAge = row[4]
            tmpDoB = row[5]
            tmpCitizenship = row[6]
            tmpPosition = row[7]
            tmpPlayerInfo = {
                "playerId": tmpAthleteId,
                "playerName": tmpAthleteName,
                "weightLb": tmpWeightLb,
                "heightIn": tmpHeightIn,
                "age": tmpAge,
                "DoB": tmpDoB,
                "citizenship": tmpCitizenship,
                "position": tmpPosition
            }
            playerInfo.append(tmpPlayerInfo)
    except Exception as e:
        print("getFixture error")
        print(e)
        err = -1

    return(playerInfo,err)

def formatExcelOut(fixtures,nameDict,nMatchesPerDay,strTimeFormatIn):
    excelOutputDict = {}
    unrecognizedTeam = []
    stage = "REGULAR_SEASON"
    for tmpId in fixtures:
        tmpFixture = {}
        fixture = fixtures[tmpId]
        tmpFixture['matchDay'] = str(math.floor((fixture['no'] - 1) / nMatchesPerDay) + 1)
        tmpFixture['date'] = fixture['matchDateUTC'].strftime(strTimeFormatIn)
        tmpFixture['name'] = fixture['matchTitle']
        tmpFixture['venue'] = fixture['venue']
        espnHometeam = fixture['homeTeam']
        espnAwayteam = fixture['awayTeam']
        if espnHometeam in nameDict.keys():
            homeTeam = nameDict[espnHometeam]
        else:
            homeTeam = espnHometeam
            if homeTeam not in unrecognizedTeam:
                unrecognizedTeam.append(homeTeam)
        if espnAwayteam in nameDict.keys():
            awayTeam = nameDict[espnAwayteam]
        else:
            awayTeam = espnAwayteam
            if awayTeam not in unrecognizedTeam:
                unrecognizedTeam.append(awayTeam)
        tmpFixture['homeTeam'] = homeTeam
        tmpFixture['awayTeam'] = awayTeam
        tmpHomeScore = str(fixture['homeScore'])
        tmpHomeShootoutScore = ""
        tmpAwayScore = str(fixture['homeScore'])
        tmpAwayShootoutScore = ""
        status = fixture['status']
        homeScore = ESPNSoccer.score(tmpHomeScore,status)
        awayScore = ESPNSoccer.score(tmpAwayScore,status)
        tmpFixture['homeScore'] = homeScore
        tmpFixture['awayScore'] = awayScore
        if "STATUS_" in status:
            statusExcel = status.replace("STATUS_", "")
        else:
            statusExcel = status
        tmpFixture['status'] = statusExcel
        tmpFixture['updateTime'] = fixture['updateTimeUTC'].strftime(strTimeFormatIn)
        statusIndex = 2
        if "POSTPONE" in fixture['status']:
            statusIndex = 1
        tmpFixture['statusIndex'] = statusIndex
        tmpFixture['stage'] = stage
        excelOutputDict[tmpId] = tmpFixture
    return(excelOutputDict)

def outputExcel(excelOutputDict, outputFilename, outputFilenameBak, nTot,delim,
                strTimeFormatIn,strDateFormatOut,strTimeFormatOut):
    description = "Output Excel fixture data"
    sortedExcelOutputTuple = sorted(
        excelOutputDict.items(),
        key=lambda k: (k[1]["statusIndex"], k[1]["date"], k[1]["name"]))

    #for sortedEvent in sortedExcelOutputTuple:
    #    print(sortedEvent)

    if os.path.isfile(outputFilename):
        copyfile(outputFilename, outputFilenameBak)

    b = open(outputFilename, "w", encoding="utf-8")
    log = {}
    line = (
            "Match Day"
            + delim
            + "MatchDate"
            + delim
            + "Match Time(New York)"
            + delim
            + "Match Time(London)"
            + delim
            + "Match Time(Moscow)"
            + delim
            + "Match Time(Dubai)"
            + delim
            + "Match Time(Shanghai)"
            + delim
    )
    line = (
            line
            + "Fixture"
            + delim
            + "Stadium"
            + delim
            + "Home Team"
            + delim
            + "Home Goal"
            + delim
            + "Away Goal"
            + delim
            + "Away Team"
            + delim
            + "Last Update Time"
            + delim
            + "Stage"
            + delim
            + "Status"
            + delim
            + "State"
            + "\n"
    )
    b.write(line)
    i = 0
    for sortedEvent in sortedExcelOutputTuple:
        eventId = sortedEvent[0]
        event = sortedEvent[1]

        matchDay = event["matchDay"]
        date = event["date"]
        name = event["name"]
        venue = event["venue"]
        homeTeam = event["homeTeam"]
        awayTeam = event["awayTeam"]
        homeScore = event["homeScore"]
        awayScore = event["awayScore"]
        statusExcel = event["status"]
        updateTime = event["updateTime"]
        stage = event['stage']

        #strTimeFormatOut = "%Y/%m/%d"
        matchDate = ESPNSoccer.extractDateTime(date, strTimeFormatIn, strDateFormatOut)

        #strTimeFormatOut = "%m/%d/%Y %H:%M:%S"
        fromZone = "UTC"
        toZone = "America/New York"
        matchDateTimeEST = ESPNSoccer.tzConvert(
            date, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )
        toZone = "Europe/London"
        matchDateTimeUK = ESPNSoccer.tzConvert(
            date, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )
        toZone = "Europe/Moscow"
        matchDateTimeCET = ESPNSoccer.tzConvert(
            date, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )
        toZone = "Asia/Dubai"
        matchDateTimeEET = ESPNSoccer.tzConvert(
            date, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )
        toZone = "Asia/Shanghai"
        matchDateTimeMSK = ESPNSoccer.tzConvert(
            date, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )

        line = (
                matchDay
                + delim
                + matchDate
                + delim
                + matchDateTimeEST
                + delim
                + matchDateTimeUK
                + delim
                + matchDateTimeCET
                + delim
                + matchDateTimeEET
                + delim
                + matchDateTimeMSK
                + delim
                + name
                + delim
                + venue
                + delim
                + homeTeam
                + delim
                + homeScore
                + delim
                + awayScore
                + delim
                + awayTeam
                + delim
                + updateTime
                + delim
                + stage
                + delim
                + statusExcel
                + delim
        )
        # print(i + 1, line)
        line = line + "\n"
        b.write(line)
        i += 1
    b.close()
    log["nFixtures"] = i
    print("Total Fixtures=", i)
    if i != nTot:
        log["code"] = -1
        print(description, "i=",i, "nTot=", nTot)
        print(description, "error! nTot not the same as number of games. Restore previous output file")
        copyfile(outputFilenameBak, outputFilename)
    else:
        print(description,outputFilename)

    return log

def processAllFixtures(allFixtures,fromZone,strTimeFormatIn,toZone,strTimeFormatOut):
    fixtures = []
    i = 0
    for tmpId in allFixtures:
        i += 1
        matchDateUTC = allFixtures[tmpId]['matchDateUTC'].strftime(strTimeFormatIn)
        leagueName = allFixtures[tmpId]['leagueName']
        venue = allFixtures[tmpId]['venue']
        attendance = allFixtures[tmpId]['attendance']
        homeTeam = allFixtures[tmpId]['homeTeam']
        awayTeam = allFixtures[tmpId]['awayTeam']
        homeTeamScore = allFixtures[tmpId]['homeScore']
        awayTeamScore = allFixtures[tmpId]['awayScore']
        homeTeamShootoutScore = allFixtures[tmpId]['homeShootoutScore']
        awayTeamShootoutScore = allFixtures[tmpId]['awayShootoutScore']
        status = allFixtures[tmpId]['status']
        updateTime = allFixtures[tmpId]['updateTimeUTC']
        matchDateTimeEST = ESPNSoccer.tzConvert(
            matchDateUTC, fromZone, strTimeFormatIn, toZone, strTimeFormatOut
        )
        name = awayTeam + " at " + homeTeam
        tmpFixture = {
                      'dateEST':matchDateTimeEST,
                      'name':leagueName,
                      'name_x':name,
                      'venue':venue,
                      "hometeam":homeTeam,
                      "homegoal":homeTeamScore,
                      "awaygoal": awayTeamScore,
                      "homeShootoutScore": homeTeamShootoutScore,
                      "awayShootoutScore": awayTeamShootoutScore,
                      "awayteam": awayTeam,
                      'status': status,
                      'updateTime_x':updateTime
                      }
        # print(i,tmpFixture)
        fixtures.append(tmpFixture)
    df = pd.DataFrame(fixtures)
    df = df.rename(
        columns={
            "dateEST": "Date Time (US Eastern)",
            "name": "League",
            "name_x": "Fixture Name",
            "venue": "Venue",
            "hometeam": "Home Team",
            "homegoal": "Home Score",
            "awaygoal": "Away Score",
            "homeShootoutScore": "Home Shootout Score",
            "awayShootoutScore": "Away Shootout Score",
            "awayteam": "Away Team",
            "status": "Status",
            "updateTime_x": "Update Time",
        }
    )
    #print(df.info())
    return df

def processLeagueTable(standings):
    leagueTable = []
    for teamId in standings:
       leagueTable.append(standings[teamId])
    df = pd.DataFrame(leagueTable)
    # print(df.info())
    df = df.drop('teamId',axis = 1)
    df = df.drop('Streak',axis = 1)
    df = df.drop('homeGD',axis = 1)
    df = df.drop('homePoints',axis = 1)
    df = df.drop('awayGD',axis = 1)
    df = df.drop('awayPoints',axis = 1)
    df = df.rename(
        columns={
            "teamRank": "Rank",
            "teamName": "Team",
            # 'team.record.rankChange','team.record.streak',
            "Points": "Points",
            "MP": "MP",
            "Win": "Win",
            "Draw": "Draw",
            "Loss": "Loss",
            "GF": "GF",
            "GA": "GA",
            "GD": "GD",
            "cleanSheets": "Clean Sheets",
            # 'team.record.ppg',
            "Deductions": "Deductions",
            "homeMP": "Home MP",
            "homeWin": "Home Win",
            "homeDraw": "Home Draw",
            "homeLoss": "Home Loss",
            "homeGF": "Home GF",
            "homeGA": "Home GA",
            "homeCleanSheets": "Home Clean Sheets",
            "awayMP": "Away MP",
            "awayWin": "Away Win",
            "awayDraw": "Away Draw",
            "awayLoss": "Away Loss",
            "awayGF": "Away GF",
            "awayGA": "Away GA",
            "awayCleanSheets": "Away Clean Sheets",
            "updateTimeUTC": "Update Time",
        }
    )
    #print(df.info())
    return df
def processTeamStats(df_teamStats, df_fixtures):
    df = df_teamStats
    df_fixtures = df_fixtures.rename(
        columns = {
            "homeScore": "homegoal",
            "awayScore": "awaygoal",
            "homeTeam": "hometeam",
            "awayTeam": "awayteam",
            "matchTitle": "name",
            "updateTimeUTC": "updateTime"
        }
    )
    # print(df.to_string())
    # print(df_fixtures.to_string())
    df = pd.merge(df, df_fixtures, left_on="eventId",right_on='eventId')
    #df = df.loc[df["status"] == "STATUS_FULL_TIME"]
    # print(df.info())
    colRequired = [
        "team",
        "dateEST",
        "name",
        "hometeam",
        "homegoal",
        "awaygoal",
        "awayteam",
        "homeAway",
        "foulsCommitted",
        "yellowCards",
        "redCards",
        "offsides",
        "wonCorners",
        "saves",
        "possessionPct",
        "totalShots",
        "shotsOnTarget",
        "shotPct",
        "penaltyKickGoals",
        "penaltyKickShots",
        "accuratePasses",
        "totalPasses",
        "passPct",
        "accurateCrosses",
        "totalCrosses",
        "crossPct",
        "accurateLongBalls",
        "totalLongBalls",
        "longballPct",
        "blockedShots",
        "effectiveTackles",
        "totalTackles",
        "tacklePct",
        "interceptions",
        "effectiveClearance",
        "totalClearance",
        "updateTime",
    ]
    colInDf = df.columns.tolist()
    (colFound,colMissing) = listIntersect(colInDf,colRequired)
    print("teamStats. all columns:     ",colInDf)
    print("teamStats. required columns:",colRequired)
    print("teamStats. found columns:   ",colFound)
    print("teamStats. dropped columns: ",colMissing)
    df_new = df[colFound]
    if len(colMissing) >0:
        for colName in colMissing:
            df_new[colName] = ''
    df_new = df_new[colRequired]
    '''
    df_new = df[
        [
            "team",
            "dateEST",
            "name",
            "hometeam",
            "homegoal",
            "awaygoal",
            "awayteam",
            "homeAway",
            "foulsCommitted",
            "yellowCards",
            "redCards",
            "offsides",
            "wonCorners",
            "saves",
            "possessionPct",
            "totalShots",
            "shotsOnTarget",
            "shotPct",
            "penaltyKickGoals",
            "penaltyKickShots",
            "accuratePasses",
            "totalPasses",
            "passPct",
            "accurateCrosses",
            "totalCrosses",
            "crossPct",
            "accurateLongBalls",
            "totalLongBalls",
            "longballPct",
            "blockedShots",
            "effectiveTackles",
            "totalTackles",
            "tacklePct",
            "interceptions",
            "effectiveClearance",
            "totalClearance",
            "updateTime",
        ]
    ]
    '''
    df_new = df_new.rename(
        columns={
            "team": "Team",
            "dateEST": "Date Time (US Eastern)",
            "name": "Fixture",
            "hometeam": "Home Team",
            "homegoal": "Home Goal",
            "awaygoal": "Away Goal",
            "awayteam": "Away Team",
        }
    )
    # print(df_new.info())
    return df_new
def processPlays(df_plays, df_fixtures):
    df = df_plays
    df_fixtures = df_fixtures.rename(
        columns = {
            "homeScore": "homegoal",
            "awayScore": "awaygoal",
            "homeTeam": "hometeam",
            "awayTeam": "awayteam",
            "matchTitle": "name",
        }
    )
    # print(df.to_string())
    # print(df_fixtures.to_string())
    df = pd.merge(df, df_fixtures, left_on="eventId",right_on='eventId')
    # print(df.info())
    colRequired = [
        "no",
        "dateEST",
        "name",
        "hometeam",
        "homegoal",
        "awaygoal",
        "awayteam",
        "team",
        "playOrder",
        "participantOrder",
        "playerName",
        "playerJersey",
        "clockDisplay",
        "text",
        "updateTime",
    ]
    colInDf = df.columns.tolist()
    (colFound,colMissing) = listIntersect(colInDf,colRequired)
    print("plays. all columns:     ",colInDf)
    print("plays. required columns:",colRequired)
    print("plays. found columns:   ",colFound)
    print("plays. dropped columns: ",colMissing)
    df_new = df[colFound]
    if len(colMissing) >0:
        for colName in colMissing:
            df_new[colName] = ''
    df_new = df_new[colRequired]
    # print(df_new.to_string())
    df_new = df_new.rename(
        columns={
            "dateEST": "Date Time (US Eastern)",
            "name": "Fixture",
            "hometeam": "Home Team",
            "homegoal": "Home Goal",
            "awaygoal": "Away Goal",
            "awayteam": "Away Team",
            "team": "Team",
            "playerJersey": "Jersey",
            "text": "Play Description",
            "clockDisplay": "Clock",
        }
    )
    # print(df_new.info())
    df_new = df_new.sort_values(by=['no','playOrder','participantOrder'])
    return df_new


def processLineup(mysqlConn, eventList, matches):
    err = 0
    i = 0
    k = 0
    playerPlayTime = {}
    playerInfo = {}
    lineup = []
    for tmpEventId in eventList:
        k += 1
        lineUp, err = getLineup(mysqlConn, tmpEventId)
        # print(seasonName, "lineup", k, "of", len(fullTimeEventList), len(lineUp), tmpEventId)
        match = matches[tmpEventId]
        matchDateTimeESt = match["dateEST"]
        title = match["matchTitle"]
        homeTeam = match["homeTeam"]
        homeScore = match["homeScore"]
        awayScore = match["awayScore"]
        awayTeam = match["awayTeam"]
        for item in lineUp:
            i += 1
            athleteId = item["athleteId"]
            if athleteId in playerIndexDict:
                playerIndex = playerIndexDict[athleteId]
            else:
                playerIndex = 0
                playerIndexDict[athleteId] = 0
            eventId = item['eventId']
            teamId = item['teamId']
            teamName = item['teamName']
            playerName = item["athleteName"]
            jersey = item["jersey"]
            starter = item["starter"]
            position = item["position"]
            formation = item["formation"]
            formationPlace = item["formationPlace"]
            gameClockDisplay = item["gameClockDisplay"]
            regularGameTime, injuryGameTime = clockDisplayValueToMinutes(gameClockDisplay)
            totGameTime = regularGameTime + injuryGameTime
            subbedIn = item["subbedIn"]
            subbedOut = item["subbedOut"]
            homeAway = item["homeAway"]
            updateTime = item["updateTime"]
            playTimeStart = 0
            if starter:
                playTimeEnd = totGameTime
                subMsg = "starter"
            else:
                playTimeEnd = 0
                subMsg = "substitute"
            subbedInForAthleteName = ""
            subbedInForAthleteJersey = ""
            subbedInClockDisplay = ""
            subbedOutByAthleteName = ""
            subbedOutByAthleteJersey = ""
            subbedOutClockDisplay = ""
            subInMsg = ""
            subOutMsg = ""
            if subbedIn:
                subbedInClockValue = item["subClockValue"]
                subbedInClockDisplay = item["subClockDisplay"]
                subbedInForAthleteName = item["subbedInForAthleteName"]
                subbedInForAthleteJersey = item["subbedInForAthleteJersey"]
                playTimeStart = subbedInClockValue / 60
                playTimeEnd = totGameTime
                subInMsg = "SubbedIn"
            if subbedOut:
                subbedOutClockValue = item["subClockValue"]
                subbedOutClockDisplay = item["subClockDisplay"]
                subbedOutByAthleteName = item["subbedOutByAthleteName"]
                subbedOutByAthleteJersey = item["subbedOutByAthleteJersey"]
                playTimeEnd = subbedOutClockValue / 60
                subOutMsg = "SubbedOut"
            playerInfo[athleteId, teamId] = {"jersey": jersey, "position": position,"updateTime":updateTime}
            playTime = playTimeEnd - playTimeStart
            if (athleteId, teamId) not in playerPlayTime:
                playerPlayTime[athleteId, teamId] = playTime
            else:
                playerPlayTime[athleteId, teamId] = playerPlayTime[athleteId, teamId] + playTime
            #if eventId in lineupEventList:
            #    print(seasonName + ":", k, i, eventId, teamId, starter,
            #          formation, formationPlace, round(playTime, 1),
            #          athleteId, playerIndex, playerName, subMsg,
            #          subInMsg, subbedInClockDisplay, subbedInForAthleteName,
            #          subOutMsg, subbedOutClockDisplay, subbedOutByAthleteName)
            #print(athleteId, teamId,playerInfo[athleteId,teamId])
            #print(athleteId, teamId, playerPlayTime[athleteId,teamId])
            tmpLineup = {
                "Date Time (US Eastern)": matchDateTimeESt,
                "Fixture": title,
                "Home Team": homeTeam,
                "Home Goal": homeScore,
                "Away Goal": awayScore,
                "Away Team": awayTeam,
                "Team": teamName,
                "Player Id":playerIndex,
                "Player Name":playerName,
                "Jersey": jersey,
                "Position": position,
                "Formation": formation,
                "Position Sort Order": formationPlace,
                "playTime": playTime,
                "subbedOutClock": subbedOutClockDisplay,
                "subbedOut": subbedOut,
                "subbedOutByName": subbedOutByAthleteName,
                "subbedOutByJersey": subbedOutByAthleteJersey,
                "subbedInClock":subbedInClockDisplay,
                "subbedIn": subbedIn,
                "subbedInForName":subbedInForAthleteName,
                "subbedInForJersey":subbedInForAthleteJersey,
                "homeAway":homeAway,
                "updateTime":updateTime
            }
            lineup.append(tmpLineup)
    return(lineup,playerInfo,playerPlayTime,err)

def processPlayerStats(playerStats,playerInfo,playerPlayTime):
    sortedStats = sorted(playerStats, key=lambda item: item['athleteName'])
    i = 0
    newPlayerStats =[]
    for playerStat in sortedStats:
        #print(playerStat)
        i += 1
        athleteId = playerStat['athleteId']
        playerIndex = playerIndexDict[athleteId]
        playerName = playerStat['athleteName']
        # print(athleteId, playerIndex, playerName, playerInfoDict[athleteId]["DoB"])
        playerDoB = playerInfoDict[athleteId]["DoB"]
        if playerDoB:
            playerDoB = datetime.strptime(playerDoB,"%Y-%m-%dT%H:%MZ").strftime("%Y-%m-%d")
        playerWeightKg = playerInfoDict[athleteId]["weightLb"] * 0.454
        playerHeightCm = playerInfoDict[athleteId]["heightIn"] * 2.54
        playerAge = playerInfoDict[athleteId]["age"]
        playerCitizenship = playerInfoDict[athleteId]["citizenship"]
        teamId = playerStat['teamId']
        teamName = playerStat['teamName']
        playerPosition = playerInfoDict[athleteId]["position"]
        appearances = playerStat["appearances"]
        subIns = playerStat["subIns"]
        foulsCommitted = playerStat["foulsCommitted"]
        foulsSuffered = playerStat["foulsSuffered"]
        ownGoals = playerStat["ownGoals"]
        offsides = playerStat["offsides"]
        yellowCards = playerStat["yellowCards"]
        redCards = playerStat["redCards"]
        goalAssists = playerStat["goalAssists"]
        shotsOnTarget = playerStat["shotsOnTarget"]
        totalShots = playerStat["totalShots"]
        totalGoals = playerStat["totalGoals"]
        goalsConceded = playerStat["goalsConceded"]
        shotsFaced = playerStat["shotsFaced"]
        saves = playerStat["saves"]
        if (athleteId, teamId) in playerPlayTime:
            playerTotPlayTime = playerPlayTime[athleteId, teamId]
        else:
            playerTotPlayTime = 0
        if appearances > 0:
            playerAvePlayTime = playerTotPlayTime / appearances
        else:
            playerAvePlayTime = 0
        if (athleteId, teamId) in playerInfo:
            jersey = playerInfo[athleteId, teamId]["jersey"]
            rosterPosition = playerInfo[athleteId, teamId]["position"]
            updateTime = playerInfo[athleteId, teamId]["updateTime"]
        else:
            jersey = 0
            rosterPosition = ""
            updatetime = ""
        #print(seasonName, "player Stats", i, athleteId, playerIndex, playerName, teamName, jersey, rosterPosition,
        #      appearances, round(playerTotPlayTime, 1), round(playerAvePlayTime, 1),playerWeightKg,playerHeightCm,
        #      playerAge, playerDoB)
        tmpPlayerStats = {
                "Id":playerIndex,
                "Name":playerName,
                "Weight(kg)":playerWeightKg,
                "Height(cm)":playerHeightCm,
                "Age":playerAge,
                "Citizenship":playerCitizenship,
                "Team":teamName,
                "Jersey":jersey,
                "Position":playerPosition,
                "Total Play Time(min)":playerTotPlayTime,
                "Average Play Time(min)":playerAvePlayTime,
                "Appearances": appearances,
                "subIns": subIns,
                "foulsCommitted": foulsCommitted,
                "foulsSuffered": foulsSuffered,
                "ownGoals": ownGoals,
                "offsides": offsides,
                "yellowCards": yellowCards,
                "redCards": redCards,
                "goalAssists": goalAssists,
                "shotsOnTarget": shotsOnTarget,
                "totalShots": totalShots,
                "totalGoals": totalGoals,
                "goalsConceded": goalsConceded,
                "shotsFaced": shotsFaced,
                "saves": saves,
                "Update Time":updateTime
                }
        newPlayerStats.append(tmpPlayerStats)
    return(newPlayerStats)


with open('config_db2.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)
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

with open('configESPN.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)
rootDir=Response['rootDir']
rootDir2=Response['rootDir2']

currentTime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

#lineupEventList = [704279, 704280, 704288]

statFromZone = "UTC"
statToZone = "America/New York"

strTimeFormatIn = "%Y-%m-%dT%H:%MZ"
strTimeFormatOut = "%m/%d/%Y %H:%M:%S"
strDateFormatOut = "%Y%m%d"

delim = "|"
yearStr = "2024"
year = int(yearStr)
defaultEncoding = "UTF-8"
#leagueList = ["ENG.1","ENG.2","ENG.3","GER.1","FRA.1","ESP.1","ITA.1","TUR.1","KSA.1"]
#leagueList = ["UEFA.NATIONS"]
leagueList = ["SWE.1"]
#leagueList = ["UEFA.CHAMPIONS","ARG.1","ENG.4","JPN.1","CHN.1","USA.1"]
excelLeagueList = ["ENG.1","ENG.2","ENG.3","GER.1","FRA.1","ESP.1","ITA.1","TUR.1","KSA.1"]
#leagueList = ["ENG.2","ENG.3","GER.1","FRA.1","ITA.1","TUR.1","KSA.1"]

for league in leagueList:
    print()
    myEncoding = defaultEncoding
    if league == "GER.1":
        myEncoding = "ISO-8859-1"
    if league == "FRA.1":
        myEncoding = "ISO-8859-1"
    if league == "ESP.1":
        myEncoding = "ISO-8859-1"
    midsizeName = league
    directory1 = rootDir + yearStr + "/"
    directory2 = directory1
    directory3 = rootDir + yearStr + "/output/"
    fixtureFileName = directory1 + league + "_" + yearStr + "_fixture.csv"
    fixtureFileNameJson = directory1 + league + "_" + yearStr + "_fixture.json"
    fixtureListFilename = directory1 + league + "_" + yearStr + "_fixture_list.csv"
    errLogFilename = directory1 + "errLog.json"

    outputFilename = directory3 + "output_" + league + "_" + yearStr + ".txt"
    outputFilenameBak = directory3 + "output_" + league + "_" + yearStr + "_bak.txt"
    clubNameFilename = directory1 + league + "_clubnames_" + yearStr + ".csv"


    stat_outputDir = (
            "C:/Users/Jie/Documents/Soccer/Soccer_Interactive_Table_2020/data/export_data/"
            + midsizeName
            + "/"
    )

    Path(stat_outputDir).mkdir(parents=True, exist_ok=True)

    club_name_file = Path(clubNameFilename)
    if club_name_file.is_file():
        nameDict = convClubname1(clubNameFilename, myEncoding)
    else:
        print(clubNameFilename, "does not exit!")
        nameDict = {}

    season, err = getSeasonInfo(mysqlDict, year, midsizeName)
    seasonType = season["seasonType"]
    midsizeName = season['midsizeName']
    seasonName = season['seasonName']
    nTeams = season['nTeams']
    nTotMatches = season['nTotMatches']
    nMatchesPerMatchDay = 10

    print("season:", seasonType,"year:", year,"league:",midsizeName)
    print("Total matches:",nTotMatches)
    print("Total teams:  ",nTeams)
    seasonStartDate = "2024-7-1"

    #
    # Get event list and team list of the season
    #
    fullTimeEventList, teams, err = getEventsTeams(mysqlDict, seasonType)
    # print(fullTimeEventList)
    print(seasonName, 'seasonType:', seasonType)
    print(seasonName, 'full time events:', len(fullTimeEventList))
    print(seasonName, 'teams:', len(teams))
    #
    # Process fixture list
    #
    fixtures,err = getFixture(mysqlDict,seasonType,statFromZone,strTimeFormatIn,statToZone,strTimeFormatOut)
    fixtureList = []
    for tmpId in fixtures:
        tmpFixture=fixtures[tmpId]
        fixtureList.append(tmpFixture)
    df_fixtures = pd.DataFrame(fixtureList)
    if midsizeName in excelLeagueList:
        #for tmpEventId in fixtures:
        #    print(fixtures[tmpEventId])
        excelOutputDict = formatExcelOut(fixtures,nameDict,nMatchesPerMatchDay,strTimeFormatIn)
        #for tmpEventId in excelOutputDict:
        #    print(excelOutputDict[tmpEventId])
        tmpLog = outputExcel(
            excelOutputDict, outputFilename, outputFilenameBak, nTotMatches,
            delim,strTimeFormatIn,strDateFormatOut,strTimeFormatOut)
    #
    # Process stat for all fixtures
    #
    allFixtures,err = getAllFixture(mysqlDict,seasonType, seasonStartDate)
    df_all_fixtures = processAllFixtures(allFixtures,statFromZone,strTimeFormatIn,
                                        statToZone,strTimeFormatOut)
    filename = "AllFixturesExport.csv"
    filename = stat_outputDir + filename
    df_all_fixtures.to_csv(filename, index=False)
    print("All Fixture", filename)
    #
    # Process standings
    #
    standings,err = getStandings(mysqlDict,seasonType)
    df_league_table = processLeagueTable(standings)

    filename = "LeagueTableExport.csv"
    filename = stat_outputDir + filename
    df_league_table.to_csv(filename, index=False)
    print("League Table", filename)

    #
    # Process team stats
    #
    teamStats, err = getTeamStats(mysqlDict, seasonType)
    #for teamStat in teamStats:
    #    print(teamStat)
    print(seasonName, "teamStats:", len(teamStats))
    df_teamStats = pd.DataFrame(teamStats)
    # print(df_teamStats.info())
    # print(df_fixtures.info())
    df_teamStats_processed = processTeamStats(df_teamStats,df_fixtures)

    filename = "TeamStatsExport.csv"
    filename = stat_outputDir + filename
    df_teamStats_processed.to_csv(filename, index=False)
    print("teamStats", filename)

    #
    # Process plays
    #
    plays, err = getPlays(mysqlDict, fullTimeEventList,strTimeFormatIn)
    # for play in plays:
    #     print(play)
    print(seasonName, "plays:", len(plays))
    df_plays = pd.DataFrame(plays)
    df_plays_processed = processPlays(df_plays,df_fixtures)
    #print(df_plays_processed.info())

    filename = "PlaysExport.csv"
    filename = stat_outputDir + filename
    df_plays_processed.to_csv(filename, index=False)
    print("plays", filename)

    #
    # Process line up
    #
    playerStats = []
    playerIndexDict = {}
    #
    # Process playerStats
    #
    tmpPlayerStats, err = getPlayerStats1(mysqlDict, seasonType, teams)
    for tmpPlayerStat in tmpPlayerStats:
        tmpAthleteId = tmpPlayerStat['athleteId']
        tmpPlayerIndex = tmpPlayerStat['playerIndex']
        playerStats.append(tmpPlayerStat)
        if tmpAthleteId not in playerIndexDict:
            playerIndexDict[tmpAthleteId] = tmpPlayerIndex
    playerInfo, err = getPlayerInfo(mysqlDict, list(playerIndexDict.keys()))

    playerInfoDict = {}
    for player in playerInfo:
       playerId = player["playerId"]
       playerInfoDict[playerId] = player

    lineup, playerInfo, playerPlayTime, err = processLineup(mysqlDict, fullTimeEventList, fixtures)

    df_lineup_processed = pd.DataFrame(lineup)
    # print(df_lineup_processed.info())

    filename = "LineupExport.csv"
    filename = stat_outputDir + filename
    df_lineup_processed.to_csv(filename, index=False)
    print("lineup", filename)

    newPlayerStats = processPlayerStats(playerStats,playerInfo,playerPlayTime)

    df_playerStats_processed = pd.DataFrame(newPlayerStats)
    # print(df_playerStats_processed.info())

    filename = "PlayerStatsExport.csv"
    filename = stat_outputDir + filename
    df_playerStats_processed.to_csv(filename, index=False)
    print("playerStats", filename)