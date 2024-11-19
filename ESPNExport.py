import pandas as pd
from sqlalchemy import create_engine
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
                SeasonType,
                seasonYear,
                seasonName,
                midsizeName,
                leagueId
            FROM
                EventSnapshotsSummary
            WHERE
                isComplete = 'Yes' and seasonYear = 2023;
            """

    fixtures = {}
    err = 0
    connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql1, engine)
    except Exception as e:
        print("exportLeagues error")
        print(e)
        err = -1
    return(df,err)

def exportTeamListFromSeasons(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

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

    fixtures = {}
    err = 0
    connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
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
    connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
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
    connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
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
def exportLineup(mysqlDict,eventId):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                a.eventId,
                a.teamId,
                t.name,
                a.homeAway,
                a.athleteId,
                a.athleteDisplayName,
                a.starter,
                a.jersey,
                a.position,
                a.formation,
                IF(a.formationPlace = 0,
                    99,
                    a.formationPlace) AS formationPlace,
                f.clock,
                f.displayClock,
                IFNULL(b.clockValue, 0) AS clockValue,
                IFNULL(b.clockDisplayValue, '') AS clockDisplayValue,
                if (a.subbedIn = 1,"Subbed In For","") as subbedIn,
                IFNULL(a.subbedInForAthleteId, '') AS subbedInForAthleteId,
                IFNULL(c.athleteDisplayName, '') AS subbedInForAthleteName,
                c.jersey as subbedInForAthleteJersey,
                if (a.subbedOut = 1,"Subbed Out By","") as subbedOut,
                IFNULL(a.subbedOutForAthleteId, '') AS subbedOutForAthleteId,
                IFNULL(d.athleteDisplayName, '') AS subbedOutForAthleteName,
                d.jersey as subbedOutForAthleteJersey
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
                    t2.athleteId, t2.athleteDisplayName, t2.jersey
                FROM
                    TeamRoster t2
                WHERE
                    eventId = {eventId}) d ON d.athleteId = a.subbedOutForAthleteId
                    JOIN
                Fixtures f ON f.eventId = a.eventId
                    JOIN
                Teams t ON t.teamId = a.teamId
            ORDER BY a.eventId, a.homeAway DESC , a.starter DESC , formationPlace , subbedIn DESC , b.clockValue;
            """

    fixtures = {}
    err = 0
    #connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
    conn, cur = connectDB(hostName,userId,pwd,dbName)
    try:
        #engine = create_engine(connString)
        #df = pd.read_sql(sql1, engine)
        cur.execute(sql1)
        rs = cur.fetchall()
    except Exception as e:
        print("exportLeagues error")
        print(tuple(eventList))
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
    connString = "mysql+pymysql://" + userId + ":" + pwd + "@" + hostName + ":3306/" + dbName
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
                "pwd": "cstagt9903",
                "hostName": "tang-svr",
                "dbName": "excel4soccer",
                "osStr": "Linux"}


df_summary, err = exportSummary(mysqlDict)
print(df_summary.info())
seasonType = df_summary['seasonType'].tolist()
print(len(seasonType))
#seasonType = 12654
#seasonType = [12654,12370]

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
i = 0
j = 0
for eventId in eventList:
    i += 1
    rs, err = exportLineup(mysqlDict, eventId)
    if (len(rs)) > 0:
        for row in rs:
            lineup.append(row)
            #print(row)
    else:
        j += 1
    if int(i/100)*100 == i or i == nEvents:
        print("processed",i, "out of", nEvents, "empty lineups", j)

df_lineup = pd.json_normalize(lineup)
print(df_lineup.info())

filename = exportDir + "lineup.csv"
# Save DataFrame to CSV
df_lineup.to_csv(filename, index=False)
print(filename)


"""
df_leagues, err = exportLeagues(mysqlDict,seasonType)
print(df_leagues.info())
filename = exportDir + "leagues.csv"
# Save DataFrame to CSV
df_leagues.to_csv(filename, index=False)
print(filename)
"""
