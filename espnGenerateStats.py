import sqlConn
import pyodbc
import pandas as pd
import json
from datetime import datetime,timezone,date, timedelta
import os
import math
from shutil import copyfile
import csv
import ESPNSoccer
import sql_insert_all
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
def getFixture(mysqlDict,seasonType):
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
                    "     INNER JOIN Leagues l ON f1.leagueId = l.id"
                    "     INNER JOIN StatusType st ON f1.statusId = st.id"
                    "     INNER JOIN Venues v ON f1.venueId = v.id"
                    "     INNER JOIN Teams t1 ON f1.homeTeamId = t1.teamId"
                    "     INNER JOIN Teams t2 ON f1.awayTeamId = t2.teamId"
                    "     WHERE"
                    "         f1.seasonType = ?) x"
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
            fixtures[tmpEventId] = {
                "no": row[0],
                "eventId": row[1],
                "seasonType": row[2],
                "matchDateUTC": row[3],
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
        " seasonType = ? ) a"
        " ON a.teamId = f1.homeTeamid)"
        " UNION ALL "
        " (SELECT *"
        " FROM Fixtures f2"
        " JOIN (SELECT teamId"
        " FROM TeamsInLeague "
        " WHERE "
        " seasonType = ?) b"
        " ON b.teamId = f2.awayTeamId)) x"
        " JOIN Teams t1 on t1.teamId = x.homeTeamId"
        " JOIN Teams t2 on t2.teamId = x.awayTeamId"
        " LEFT JOIN Leagues l on l.id = x.leagueId"
        " LEFT JOIN Venues v on v.id = x.venueId"
        " LEFT JOIN StatusType st on st.id = x.statusId"
        " LEFT JOIN Attendance at on at.eventId = x.eventId"
        " WHERE x.date > ?"
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

        val = (seasonType,seasonType,startDate,)
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

def getSeaonInfo(mysqlDict,year,midsizeName):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']
    sql1 = (
        "SELECT "
        "   a.*, b.nTotMatches"
        " FROM "
        "   (SELECT "
        "       seasonType,"
        "       MAX(year) AS year,"
        "       MAX(leagueId) AS leagueId,"
        "       MAX(midsizeLeagueName) AS midsizeName,"
        "       COUNT(teamId) AS nTeams"
        "   FROM "
        "       TeamsInLeague "
        "   GROUP BY seasonType) a"
        "   JOIN "
        "       (SELECT "
        "           COUNT(eventId) AS nTotMatches, seasonType"
        "       FROM "
        "           Fixtures"
        "       GROUP BY seasonType) b ON a.seasonType = b.seasontype"
        "  where year = ? and midsizeName = ?;"
                    )
    try:
        if osStr == "Windows":
            (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
        elif osStr == "Linux":
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        else:
            (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
        # print(conn)

        val = (year,midsizeName,)
        cursor.execute(sql1, val)
        rs = cursor.fetchall()
        conn.close()
    except Exception as e:
        print("getSeasonInfo error")
        print(e)
        print(sql1)
        rs = []

    return(rs)
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
            "        seasonType = ?) a"
            "        LEFT JOIN "
            "    (SELECT "
            "        awayTeamId, COUNT(awayTeamId) AS awayCleanSheets"
            "    FROM "
            "        Fixtures "
            "    WHERE "
            "        seasonType = ? AND statusId = 28"
            "            AND homeTeamScore = 0"
            "    GROUP BY awayTeamId) c1 ON c1.awayTeamId = a.teamId"
            "        LEFT JOIN "
            "    (SELECT "
            "        homeTeamId, COUNT(homeTeamId) AS homeCleanSheets"
            "    FROM "
            "        Fixtures "
            "    WHERE "
            "        seasonType = ? AND statusId = 28"
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
        print("getFixture error")
        print(e)
        err = -1

    return(standings,err)

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
    # print("Total Fixtures=", i)
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
    #print(df.info())
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

with open('config_db.json','r') as file:
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

seasonType = 12654
year = "2024"
league = "ENG.1"
defaultEncoding = "UTF-8"

directory1 = rootDir + year + "/"
directory2 = directory1
directory3 = rootDir + year + "/output/"
fixtureFileName = directory1 + league + "_" + year + "_fixture.csv"
fixtureFileNameJson = directory1 + league + "_" + year + "_fixture.json"
fixtureListFilename = directory1 + league + "_" + year + "_fixture_list.csv"
errLogFilename = directory1 + "errLog.json"

outputFilename = directory3 + "output_" + league + "_" + year + ".txt"
outputFilenameBak = directory3 + "output_" + league + "_" + year + "_bak.txt"
clubNameFilename = directory1 + league + "_clubnames_" + year + ".csv"

statFromZone = "UTC"
statToZone = "America/New York"

strTimeFormatIn = "%Y-%m-%dT%H:%MZ"
strTimeFormatOut = "%m/%d/%Y %H:%M:%S"
strDateFormatOut = "%Y%m%d"

delim = "|"
year = 2024
midsizeName = "ENG.1"
myEncoding = defaultEncoding

stat_outputDir = (
        "C:/Users/Jie/Documents/Soccer/Soccer_Interactive_Table_2020/data/export_data/"
        + midsizeName
        + "/"
)

if os.path.isfile(clubNameFilename):
    nameDict = convClubname1(clubNameFilename, myEncoding)
else:
    print(clubNameFilename, "does not exit!")
    nameDict = {}

rs = getSeaonInfo(mysqlDict,year,midsizeName)
# print(rs)
seasonType = rs[0][0]
nTeams=rs[0][4]
nTotMatches = rs[0][5]
nMatchesPerMatchDay = 10

print()
print("season:", seasonType,"year:", year,"league:",midsizeName)
print("Total matches:",nTotMatches)
print("Total teams:  ",nTeams)
seasonStartDate = "2024-7-1"

#
# Process fixture list
#
fixtures,err = getFixture(mysqlDict,seasonType)
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

