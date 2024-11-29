import pandas as pd
from sqlalchemy import create_engine
import time
import urllib.parse
import json
from datetime import datetime,timezone,date, timedelta
import os
import sys
from pathlib import Path
import csv
import ESPNSoccer

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
        cur = conn.cursor(dictionary=True)
    except Exception as e:
        print(e)
        print('task is terminated')
        sys.exit()
    return(conn,cur)

def exportSummary(mysqlDict):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT
                row_number() over (partition by a.year order by b.midsizeName) as Rn,
                a.year,
                a.seasonType,
                a.seasonName,
                if (isnull(b.midsizeName), "none", b.midsizeName) as midsizeName,
                a.nToTEvents,
                a.nCompletedEvents,
                a.nTeams,
                a.nHomeLineup,
                a.nAwayLineup,
                a.nDetails,
                a.nCommentary,
                a.nKeyEvents,
                a.nPlays,
                a.nStandings,
                a.nTeamsInSeason,
                a.nHomePlayerStats,
                a.nAwayPlayerStats
            FROM
                (SELECT 
                    y.year,
                        y.seasonType,
                        MAX(y.name) AS seasonName,
                        max(y.leagueId) as leagueId,
                        COUNT(y.eventId) AS nTotEvents,
                        sum(if(y.statusId >= 28,1,0)) as nCompletedEvents,
                        COUNT(DISTINCT y.homeTeamId) AS nTeams,
                        sum(if(y.nHomeLineup > 0, 1,0)) as nHomeLineup,
                        sum(if(y.nAwayLineup > 0, 1,0)) as nAwayLineup,
                        sum(if(y.nDetails > 0 ,1,0)) as nDetails,
                        sum(if(y.nCommentary>0,1,0)) AS nCommentary,
                        sum(if(y.nKeyEvents>0,1,0)) AS nKeyEvents,
                        sum(if(y.nPlays >0 ,1,0)) AS nPlays,
                        sum(if(y.nHomeRoster >0 ,1,0)) AS nHomeRoster,
                        sum(if(y.nAwayRoster >0, 1, 0)) AS nAwayRoster,
                        sum(if(y.nHomePlayerStats>0,1,0)) as nHomePlayerStats,
                        sum(if(y.nAwayPlayerStats>0,1,0)) as nAwayPlayerStats,
                        x1.nStandings,
                        x2.nTeamsInSeason,
                        x2.nPlayersInSeason
                FROM
                    EventSummary y
                LEFT JOIN (SELECT 
                    seasonType, COUNT(teamId) AS nStandings
                FROM
                    Standings
                GROUP BY seasonType) x1 ON x1.seasonType = y.seasonType
                LEFT JOIN (SELECT 
                            a.seasonType,
                            COUNT(a.teamId) AS nTeamsInSeason,
                            SUM(nTeamRoster) AS nPlayersInSeason
                        FROM
                            (SELECT 
                                seasonType, teamId, COUNT(athleteId) AS nTeamRoster
                            FROM
                                PlayerInTeam
                            GROUP BY seasonType , teamId) a
                        GROUP BY a.seasonType
                ) x2 ON x2.seasonType = y.seasonType
                GROUP BY year, y.seasonType) a
            left join Leagues b on b.id = a.leagueId
            order by year and Rn;
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        conn, cur = connectDB(hostName, userId, pwd, dbName)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("exportLeagues error")
        print(e)
        err = -1
    return(rs,err)

def exportTeamListFromSeasons(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    encoded_pwd = urllib.parse.quote_plus(pwd)

    sql1 = f"""
               SELECT DISTINCT
                a.teamId
            FROM
                (SELECT 
                    seasonType, homeTeamId AS teamId
                FROM
                    Fixtures UNION SELECT 
                    seasonType, awayTeamId AS teamId
                FROM
                    Fixtures) a
            WHERE
                a.seasonType IN {tuple(seasonType)} 
            ; 
            """

    err = 0
    connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    print(connString)
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql1, engine)
    except Exception as e:
        print("exportLeagues error")
        print(e)
        err = -1
    return(df,err)

def exportTeams(mysqlDict,teamList):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    encoded_pwd = urllib.parse.quote_plus(pwd)

    sql1 = f"""
                SELECT 
                teamId,
                location,
                name,
                abbreviation,
                displayName,
                shortDisplayName,
                color,
                alternateColor,
                logoURL,
                venueId,
                slug
            FROM
                Teams
            WHERE
                teamId IN {tuple(teamList)};
            """

    fixtures = {}
    err = 0
    connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql1, engine)
    except Exception as e:
        print("exportLeagues error")
        print(e)
        err = -1
    return(df,err)
def exportFixture(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    encoded_pwd = urllib.parse.quote_plus(pwd)

    sql_fixture = f"""
               SELECT
                row_number() OVER (PARTITION BY seasonType order by date, eventId) Rn,
                seasonType,
                leagueId,
                eventId,
                date,
                venueId,
                attendance,
                homeTeamId,
                awayTeamId,
                homeTeamWinner,
                awayTeamWinner,
                homeTeamScore,
                awayTeamScore,
                homeTeamShootoutScore,
                awayTeamShootoutScore,
                statusId,
                updateTime
            FROM
                Fixtures
            WHERE
                seasonType in {tuple(seasonType)} 
            ORDER BY seasonType, date ASC; 
            """

    fixtures = {}
    err = 0
    connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql_fixture, engine)
    except Exception as e:
        print("getFixture error")
        print(e)
        err = -1
    # convert bit to bool
    df['homeTeamWinner'] = df['homeTeamWinner'].apply(lambda x: bool(ord(x)))
    df['awayTeamWinner'] = df['awayTeamWinner'].apply(lambda x: bool(ord(x)))
    return(df,err)
def exportLineup(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                f.seasonType,
                t.eventId,
                t.teamId,
                t.homeAway,
                t.winner,
                t.formation,
                t.active,
                t.starter,
                t.jersey,
                t.athleteId,
                t.athleteDisplayName,
                t.position,
                t.formationPlace,
                t.subbedIn,
                t.subbedInForAthleteId,
                t.subbedInForAthleteJersey,
                k1.subInAthleteId,
                k1.subInClockValue,
                k1.subInDisplayClock,
                /*k1.keyEventText AS subInText,*/
                t.subbedOut,
                t.subbedOutForAthleteId,
                t.subbedOutForAthleteJersey,
                k2.subOutAthleteId,
                k2.subOutClockValue,
                k2.subOutDisplayClock,
                /*k2.keyEventText AS subOutText,*/
                t.updateTime
            FROM
                (SELECT 
                    *
                FROM
                    TeamRoster) t
                    JOIN
                Fixtures f ON f.eventId = t.eventId
                    LEFT JOIN
                (SELECT 
                    ke.eventId,
                        ke.keyEventId,
                        ke.clockValue AS subInClockValue,
                        ke.clockDisplayValue AS subInDisplayClock,
                        ke.keyEventText,
                        a.id AS subInAthleteId
                FROM
                    KeyEvents ke
                LEFT JOIN KeyEventParticipants kp ON kp.keyEventId = ke.keyEventId
                LEFT JOIN Athletes a ON a.fullName = kp.participant
                WHERE
                    ke.typeId = 76) k1 ON t.subbedInForAthleteId = k1.subInAthleteId
                    AND t.eventId = k1.eventId
                    LEFT JOIN
                (SELECT 
                    ke.eventId,
                        ke.keyEventId,
                        ke.clockValue AS subOutClockValue,
                        ke.clockDisplayValue AS subOutDisplayClock,
                        ke.keyEventText,
                        a.id AS subOutAthleteId
                FROM
                    KeyEvents ke
                LEFT JOIN KeyEventParticipants kp ON kp.keyEventId = ke.keyEventId
                LEFT JOIN Athletes a ON a.fullName = kp.participant
                WHERE
                    ke.typeId = 76) k2 ON t.subbedOutForAthleteId = k2.subOutAthleteId
                    AND t.eventId = k2.eventId
            where f.seasonType = {seasonType}
            ORDER BY t.eventId , t.homeAway DESC , t.starter DESC , t.formationPlace
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("export lineup error")
        # print(tuple(eventList))
        print(e)
        err = -1
    else:
        return(rs,err)

def exportPlays(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
            SELECT 
                f.seasonType,
                p.eventId,
                p.playOrder,
                p.playId,
                p.typeId,
                p.text,
                p.shortText,
                p.period,
                p.clockValue,
                p.clockDisplayValue,
                p.teamId,
                p.scoringPlay,
                p.shootout,
                p.wallclock,
                p.goalPositionX,
                p.goalPositionY,
                p.fieldpositionX,
                p.fieldPositionY,
                p.fieldPosition2X,
                p.fieldPosition2Y,
                a.id AS athleteId,
                pp.participant,
                u.updateDateTime
            FROM
                Plays p
                    JOIN
                (SELECT 
                    eventId, seasonType
                FROM
                    Fixtures
                WHERE
                    seasonType = {seasonType}) f ON f.eventId = p.eventId
                    LEFT JOIN
                PlayParticipants pp ON pp.playId = p.playId
                    LEFT JOIN
                Athletes a ON a.fullName = pp.participant
                    JOIN
                UpdateId u ON u.updateId = p.updateId
            ORDER BY f.eventId , p.clockValue , pp.playOrder;
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("export plays error")
        # print(tuple(eventList))
        print(e)
        err = -1
    else:
        return(rs,err)

def exportKeyEvents(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
               SELECT 
                f.seasonType,
                k.eventId,
                k.keyEventOrder,
                k.keyEventId,
                k.typeId,
                k.period,
                k.clockValue,
                k.clockDisplayValue,
                k.scoringPlay,
                k.shootout,
                k.keyEventText,
                k.keyEventShortText,
                k.teamId,
                k.goalPositionX,
                k.goalPositionY,
                k.fieldPositionX,
                k.fieldPositionY,
                k.fieldPosition2X,
                k.fieldPosition2Y,
                kp.keyEventOrder AS participantOrder,
                a.id AS athleteId,
                u.updateDateTime
            FROM
                KeyEvents k
                    JOIN
                (SELECT 
                    eventId, seasonType
                FROM
                    Fixtures
                WHERE
                    seasonType = {seasonType}) f ON f.eventId = k.eventId
                    LEFT JOIN
                KeyEventParticipants kp ON kp.keyEventId = k.keyEventId
                    LEFT JOIN
                Athletes a ON a.fullName = kp.participant
                    JOIN
                UpdateId u ON u.updateId = k.updateId
            ORDER BY k.eventId , k.clockValue , kp.keyEventOrder; 
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("export keyEvents error")
        # print(tuple(eventList))
        print(e)
        err = -1
    else:
        return(rs,err)

def exportCommentary(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                    f.seasonType,
                    c.eventId,
                    c.commentaryOrder,
                    c.id AS commentaryId,
                    c.clockDisplayValue,
                    c.commentaryText,
                    u.updateDateTime
                FROM
                    Commentary c
                        JOIN
                    (SELECT 
                        eventId, seasonType
                    FROM
                        Fixtures
                    WHERE
                        seasonType = {seasonType}) f ON f.eventId = c.eventId
                            JOIN
                    UpdateId u ON u.updateId = c.updateId;
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("export commentary error")
        # print(tuple(eventList))
        print(e)
        err = -1
    else:
        return(rs,err)

def exportPlayerStats(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
            seasonType,
            seasonYear as year,
            league,
            teamId,
            id as playerId,
            appearances_value,
            subIns_value,
            foulsCommitted_value, 
            foulsSuffered_value,
            yellowCards_value,
            redCards_value,
            ownGoals_value,
            goalAssists_value,
            offsides_value,
            shotsOnTarget_value,
            totalShots_value,
            totalGoals_value,
            shotsFaced_value,
            saves_value,
            goalsConceded_value,
            timestamp
            FROM PlayerStatsDB
            WHERE
                seasonType = {seasonType} 
            order by seasonType;
            """

    fixtures = {}
    err = 0
    #encoded_pwd = urllib.parse.quote_plus(pwd)
    #connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("export commentary error")
        # print(tuple(eventList))
        print(e)
        err = -1
    else:
        return(rs,err)

def exportLeagues(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                    SELECT 
                    typeId AS seasonType,
                    year,
                    seasonName,
                    seasonSlug,
                    leagueId,
                    midsizeName,
                    leagueName,
                    leagueShortName
                FROM
                    SeasonTypeLeagueId
                WHERE
                    typeId IN {tuple(seasonType)};
            """

    fixtures = {}
    err = 0
    encoded_pwd = urllib.parse.quote_plus(pwd)
    connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql1, engine)
    except Exception as e:
        print("exportLeagues error")
        print(e)
        err = -1
    return(df,err)

#
# main
#
exportDir = "E:/soccer/espnExport/tmp/"

mysqlDict =  {"userId": "jtang",
                "pwd": "@CstAgt9903!",
                "hostName": "tang-03-lx",
                "dbName": "excel4soccer",
                "osStr": "Linux"}

seasonYear = 2024

eventSummary = []
startTime = time.time()
rs_summary, err = exportSummary(mysqlDict)
endTime = time.time()
elapsedTime = endTime - startTime
print("exportSummary elapsed time:",elapsedTime,"sec")
for row in rs_summary:
    tmpSeasonType = row['seasonType']
    eventSummary.append(row)
    # print(row)

seasonType = []
seasonTypeDict = {}
for season in eventSummary:
    tmpSeasonType = int(season["seasonType"])
    tmpSeasonYear = int(season['year'])
    tmpMidSizeName = season['midsizeName']
    tmpCompletedEvents = int(season['nCompletedEvents'])
    tmpHomePlayerStats = int(season['nHomePlayerStats'])
    tmpAwayPlayerStats = int(season['nAwayPlayerStats'])
    if (tmpSeasonYear == seasonYear and
            tmpCompletedEvents > 0 and
            tmpHomePlayerStats == tmpCompletedEvents and
            tmpAwayPlayerStats == tmpCompletedEvents):
        seasonType.append(tmpSeasonType)
        seasonTypeDict[tmpSeasonType] = {"year":tmpSeasonYear,'midsizeName':tmpMidSizeName}
#seasonType = [12654,12370]
nSeasons = len(seasonType)
print(nSeasons)
#print(seasonType)
#print(seasonTypeDict)

df_team_list, err = exportTeamListFromSeasons(mysqlDict,seasonType)
print(df_team_list.info())
teamList = df_team_list['teamId'].tolist()
print(len(teamList))

df_teams, err = exportTeams(mysqlDict,teamList)
print(df_teams.info())
filename = exportDir + "teams.csv"
# Save DataFrame to CSV
df_teams.to_csv(filename, index=False)
print(filename)

df_fixtures, err = exportFixture(mysqlDict,seasonType)
print(df_fixtures.info())
eventList = df_fixtures['eventId'].tolist()
print(len(eventList))

filename = exportDir + "fixtures.csv"
# Save DataFrame to CSV
df_fixtures.to_csv(filename, index=False)
print(filename)

#eventList = [704388,704279]
nEvents = len(eventList)
lineup = []
plays = []
keyEvents = []
commentary = []
playerStats = []
i = 0
j1 = 0
j2 = 0
j3 = 0
j4 = 0
j5 = 0
for tmpSeasonType in seasonType:
    midsizeName = seasonTypeDict[tmpSeasonType]['midsizeName']
    bSave = False
    if i == 0:
        oldMidsizeName = midsizeName
    if oldMidsizeName != midsizeName:
        bSave = True
    i += 1
    rs1, err = exportLineup(mysqlDict, tmpSeasonType)
    rs2, err = exportPlays(mysqlDict, tmpSeasonType)
    rs3, err = exportKeyEvents(mysqlDict, tmpSeasonType)
    rs4, err = exportCommentary(mysqlDict, tmpSeasonType)
    rs5, err = exportPlayerStats(mysqlDict, tmpSeasonType)
    if (len(rs1)) > 0:
        if bSave:
            if j1 > 0:
                df_lineup = pd.json_normalize(lineup)
                # print(df_lineup.info())
                filename = exportDir + "lineup/lineup_" + str(seasonYear) + "_" + oldMidsizeName + ".csv"
                # Save DataFrame to CSV
                df_lineup.to_csv(filename, index=False)
                # print(filename)
                lineup = []
                j1 = 0
        j1 += 1
        for row in rs1:
            lineup.append(row)
            #print(row)
    print("seasonType", tmpSeasonType, "processed", i, "out of", nSeasons,
          "complete lineups   ", j1, bSave, oldMidsizeName)
    if (len(rs2)) > 0:
        if bSave:
            if j2 > 0:
                df_plays = pd.json_normalize(plays)
                # print(df_plays.info())
                filename = exportDir + "plays/plays_" + str(seasonYear) + "_" + oldMidsizeName + ".csv"
                # Save DataFrame to CSV
                df_plays.to_csv(filename, index=False)
                # print(filename)
                plays = []
                j2 = 0
        j2 += 1
        for row in rs2:
            plays.append(row)
            #print(row)
    print("seasonType", tmpSeasonType,"processed", i, "out of", nSeasons,
          "complete plays     ", j2, bSave, oldMidsizeName)
    if (len(rs3)) > 0:
        if bSave:
            if j3 > 0:
                df_keyEvents = pd.json_normalize(keyEvents)
                # print(df_keyEvents.info())
                filename = exportDir + "keyEvents/keyEvents_" + str(seasonYear) + "_" + oldMidsizeName + ".csv"
                # Save DataFrame to CSV
                df_keyEvents.to_csv(filename, index=False)
                # print(filename)
                kenEvents = []
                j3 = 0
        j3 += 1
        for row in rs3:
            keyEvents.append(row)
            #print(row)
    print("seasonType", tmpSeasonType, "processed", i, "out of", nSeasons,
          "complete keyEvents ", j3, bSave, oldMidsizeName)
    if (len(rs4)) > 0:
        if bSave:
            if j4 > 0:
                df_commentary = pd.json_normalize(commentary)
                # print(df_commentary.info())
                filename = exportDir + "commentary/commentary_" + str(seasonYear) + "_" + oldMidsizeName + ".csv"
                # Save DataFrame to CSV
                df_commentary.to_csv(filename, index=False)
                # print(filename)
                commentary = []
                j4 = 0
        j4 += 1
        for row in rs4:
            commentary.append(row)
            #print(row)
    print("seasonType", tmpSeasonType, "processed", i, "out of", nSeasons,
          "complete commentary", j4, bSave, oldMidsizeName)
    if (len(rs5)) > 0:
        if bSave:
            if j5 > 0:
                df_playerStats = pd.json_normalize(playerStats)
                # print(df_commentary.info())
                filename = exportDir + "playerStats/playerStats_" + str(seasonYear) + "_" + oldMidsizeName + ".csv"
                # Save DataFrame to CSV
                df_playerStats.to_csv(filename, index=False)
                # print(filename)
                playerStats = []
                j4 = 0
        j4 += 1
        for row in rs4:
            playerStats.append(row)
            #print(row)
    print("seasonType", tmpSeasonType, "processed", i, "out of", nSeasons,
          "complete playerStats", j5, bSave, oldMidsizeName)
    #
    # Reset oldMidsizeName to midsizeName at end of loop
    if bSave:
        oldMidsizeName = midsizeName


df_leagues, err = exportLeagues(mysqlDict,seasonType)
print(df_leagues.info())
filename = exportDir + "leagues.csv"
# Save DataFrame to CSV
df_leagues.to_csv(filename, index=False)
print(filename)