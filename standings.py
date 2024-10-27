import json
import mysql
from datetime import datetime,timezone,date, timedelta
from backports.zoneinfo import ZoneInfo
import os
import csv
import sqlConn

def clockDisplayValueToMinutes(strClockDisplayValue):
    if '+' in strClockDisplayValue:
        regPlayTime = int(strClockDisplayValue.split('+')[0].strip("'"))
        injPlayTime = int(strClockDisplayValue.split('+')[1].strip("'"))
    else:
        regPlayTime = int(strClockDisplayValue.strip("'"))
        injPlayTime = 0
    # print(tmpPlay['clock']['displayValue'],regPlayTime,injPlayTime)
    return(regPlayTime,injPlayTime)

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
            a.subbedIn,
            IFNULL(c.athleteDisplayName, '') AS subbedInForAthleteName,
            a.subbedOut,
            IFNULL(d.athleteDisplayName, '') AS subbedOutForAthleteName,
            a.updateTime
        FROM
            (SELECT 
                *
            FROM
                excel4soccer.TeamRoster
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
                t1.athleteId, t1.athleteDisplayName
            FROM
                TeamRoster t1
            WHERE
                eventId = {eventId}) c ON c.athleteId = a.subbedInForAthleteId
                LEFT JOIN
            (SELECT 
                t2.athleteId, t2.athleteDisplayName
            FROM
                TeamRoster t2
            WHERE
                eventId = {eventId}) d ON d.athleteId = a.subbedOutForAthleteId
            join Fixtures f on f.eventId = a.eventId
        ORDER BY a.eventId, a.homeAway desc , a.starter DESC , formationPlace , subbedIn DESC , b.clockValue
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
        homeAway = item[2]
        athleteId = item[3]
        player = item[4]
        starter = item[5]
        jersey = item[6]
        position = item[7]
        formation = item[8]
        formationPlace = item[9]
        gameClock = item[10]
        gameClockDisplay = item[11]
        subClock = item[12]
        subClockDisplay = item[13]
        subbedIn = item[14]
        subbedInForAthleteName = item[15]
        subbedOut = item[16]
        subbedOutByAthleteName = item[17]
        updateTime = item[18]
        tmpLineUp = {
            "eventId":eventId,
            "teamId": teamId,
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
            "subbedOut": subbedOut,
            "subbedOutByAthleteName": subbedOutByAthleteName,
            "updateTime": updateTime
        }
        lineUp.append(tmpLineUp)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(lineUp, err)
def getFixtures(mysqlDict,seasonType):
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
    fixtures = []
    teamsInLeague =[]
    for item in rs:
        i += 1
        eventId = item[0]
        statusId = item[1]
        homeTeamId = item[2]
        awayTeamId = item[3]
        if statusId ==28:
            fixtures.append(eventId)
        if homeTeamId not in teamsInLeague:
            teamsInLeague.append(homeTeamId)
        if awayTeamId not in teamsInLeague:
            teamsInLeague.append(awayTeamId)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(fixtures, teamsInLeague,err)
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
        shotPct = row[13]
        penaltyKickGoals = row[14]
        penaltyKickShots = row[15]
        accuratePasses = row[16]
        totalPasses = row[17]
        passPct = row[18]
        accurateCrosses = row[19]
        totalCrosses = row[20]
        crossPct = row[21]
        totalLongBalls = row[22]
        accurateLongBalls = row[23]
        longballPct = row[24]
        blockedShots = row[25]
        effectiveTackles = row[26]
        totalTackles = row[27]
        tacklePct = row[28]
        interceptions = row[29]
        effectiveClearance = row[30]
        totalClearance = row[31]
        tmpTeamStat = {
            "eventId": eventId,
            "teamId": teamId,
            "teamName": teamName,
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
def getPlays(mysqlDict,eventList):
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
        eventId,
        playOrder,
        typeId,
        text,
        clockDisplayValue,
        teamId 
        FROM Plays
        where eventId in {tuple(eventList)}
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
        typeId = row[2]
        text = row[3]
        clockDisplay = row[4]
        teamId = row[5]
        tmpPlay = {
            "eventId": eventId,
            "teamId": teamId,
            "playOrder": playOrder,
            "text": text,
            "clockDisplay": clockDisplay
        }
        plays.append(tmpPlay)
        #print(i, athleteId, starter, formationPlace, subMsg, playTime)
    conn.close()
    return(plays, err)
def getSeasonInfo(mysqlDict,seasonType):
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
            a.*, b.nTotMatches, st.name as seasonName
        FROM
            (SELECT 
                seasonType,
                    MAX(year) AS year,
                    MAX(leagueId) AS leagueId,
                    MAX(midsizeLeagueName) midsizeName,
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
            where b.seasonType = {seasonType}
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
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
            ORDER BY teamRank;
        """
    # print(sql1)
    cursor.execute(sql1, multi=True)
    rs = cursor.fetchall()
    standings = []
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
                "teamRank":teamRank,
                "rankChange":rankChange,
                "timeStamp":timeStamp,
                "updateId":updateId
            }
        )
    conn.close()
    return(standings, err)
#
# Main program
#
timeObj1 = datetime.now(ZoneInfo("America/New_York"))

with open('config_db2.json','r') as file:
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
seasons =[12654,12655,12826]
#seasons =[12655]
#seasons =[12654]
lineupEventList = [704279, 704280, 704288]
teamRank = {}
teamId = {}
gamesPlayed = {}
for seasonType in seasons:
    teamRank[seasonType] = []
    teamId[seasonType] = []
    gamesPlayed[seasonType] = []
    standings, err = getStandings(mysqlDict,seasonType)
    for teamRecord in standings:
        teamRank[seasonType].append(teamRecord['teamRank'])
        teamId[seasonType].append(teamRecord['teamId'])
        gamesPlayed[seasonType].append(teamRecord['gamesPlayed'])

for seasonType in seasons:
    print("seasonType", seasonType, "team rank    ", teamRank[seasonType])
    print("seasonType", seasonType, "team Id      ", teamId[seasonType])
    print("seasonType", seasonType, "games played ", gamesPlayed[seasonType])
