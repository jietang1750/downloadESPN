import pandas as pd
from sqlalchemy import create_engine
import time
import urllib.parse
import json
from datetime import datetime,timezone,date, timedelta
import sys
from kaggle.api.kaggle_api_extended import KaggleApi
import threading
import ctypes
import numpy as np

def show_progress(start_time, status_message="Running"):
    """Show a progress indicator with spinner and elapsed time"""
    spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    i = 0
    while True:
        elapsed = time.time() - start_time
        sys.stdout.write(f"\r{spinner[i]} {status_message}... Elapsed time: {timedelta(seconds=int(elapsed))}    ")
        sys.stdout.flush()
        time.sleep(0.1)
        i = (i + 1) % len(spinner)

def run_with_progress(func, *args, status_message="Running"):
    """Run a function with a progress spinner"""
    start_time = time.time()
    progress_thread = threading.Thread(target=lambda: show_progress(start_time, status_message))
    progress_thread.daemon = True
    progress_thread.start()

    result = func(*args)

    # Stop the progress thread
    progress_thread.join(timeout=0)
    if progress_thread.is_alive():
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(progress_thread.ident),
            ctypes.py_object(SystemExit)
        )

    elapsed_time = time.time() - start_time
    print(f"\n{status_message} completed in {elapsed_time:.2f} seconds")
    return result

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
    eventSummary = []
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
        print("export Summary error")
        print(e)
        err = -1
    for row in rs:
        eventSummary.append(row)
    return(eventSummary,err)

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
                tr.athleteId,
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
                TeamRoster tr on tr.eventId = p.eventId 
                    and tr.teamId = p.teamId 
                    and tr.athleteDisplayName = pp.participant 
                    JOIN
                UpdateId u ON u.updateId = p.updateId
            ORDER BY f.eventId , p.clockValue , pp.playOrder;
            """

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
                k.typeId as keyEventTypeId,
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
            id as athleteId,
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

def exportStandings(mysqlDict,seasonTypeList):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                s.seasonType,
                s.year,
                s.leagueId,
                a.last_match_date as last_matchDateTime,
                s.teamRank,
                s.teamId,
                s.gamesPlayed,
                s.wins,
                s.ties,
                s.losses,
                s.points,
                s.pointsFor AS gf,
                s.pointsAgainst AS ga,
                s.pointDifferential AS gd,
                s.deductions,
                a.clean_sheet,
                a.form,
                a.next_opponent,
                a.next_homeAway,
                a.next_matchDate as next_matchDateTime,
                s.timeStamp
            FROM
                Standings s
            left join
            (select row_number() OVER 
                (PARTITION BY lt1.seasonType order by lt1.pts_cum desc, lt1.gd_cum desc, lt1.gf_cum desc) teamRank,
            lt1.seasonType, lt1.date as last_match_date, lt1.teamId, lt1.mp, 
            lt1.win_cum as win, lt1.draw_cum as draw,lt1.loss_cum as loss,
            lt1.pts_cum as pts, lt1.gf_cum as gf, lt1.ga_cum as ga, lt1.gd_cum as gd, 
            lt1.cleansheet_cum as clean_sheet, lt1.form as form, lt2.next_opponent, 
            lt2.next_homeAway, lt2.next_matchDate
            from
            (with l as 
            (
            select  b.seasonType, b.date,b.teamId, b.win_cum +b.draw_cum + b.loss_cum as mp, b.win_cum, 
            b.draw_cum, b.loss_cum, b.pts_cum,b.gf_cum, b.ga_cum, 
            b.gf_cum-b.ga_cum as gd_cum, b.cleansheet_cum, b.result, b.Rn
            from
            (
            select a.seasonType, 
            row_number() OVER (PARTITION BY seasonType, a.teamId order by date desc) Rn,
            a.date,a.eventId,a.teamId,
            if(a.win = 1, "W",if(a.draw =1,"D","L")) as result,
            sum(a.win) over(partition by a.seasonType, a.teamId) as win_cum, 
            sum(a.draw) over (partition by a.seasonType, a.teamId) as draw_cum, 
            sum(a.loss) over (partition by a.seasonType, a.teamId) as loss_cum,
            sum(a.pts) over(partition by a.seasonType, a.teamId) as pts_cum, 
            sum(a.gf) over (partition by a.seasonType, a.teamId) as gf_cum, 
            sum(a.ga) over (partition by a.seasonType, a.teamId) as ga_cum,
            sum(a.cleansheet) over (partition by a.seasonType, a.teamId) as cleansheet_cum
            from
            (select eventId,date, seasonType, hometeamId as teamId, homeTeamOrder as homeAway, 
            if(homeTeamScore > AwayTeamScore,1,0) as win,
            if (homeTeamScore = awayTeamScore,1,0) as draw,
            if(homeTeamScore < AwayTeamScore,1,0) as loss,
            if (homeTeamScore = awayTeamScore,1,if(homeTeamScore > AwayTeamScore,3,0)) as pts,
            homeTeamScore as gf,
            awayTeamScore as ga,
            if (awayTeamScore =0, 1, 0) as cleansheet
            from Fixtures
            where seasonType in {tuple(seasonTypeList)} and statusID >= 28
            union
            select eventId,date, seasonType, awayteamId as teamId, awayTeamOrder as homeAway, 
            if(homeTeamScore < AwayTeamScore,1,0) as win,
            if (homeTeamScore = awayTeamScore,1,0) as draw,
            if(homeTeamScore > AwayTeamScore,1,0) as loss,
            if (homeTeamScore = awayTeamScore,1,if(homeTeamScore < AwayTeamScore,3,0)) as pts,
            awayTeamScore as gf,
            homeTeamScore as ga,
            if (homeTeamScore =0, 1, 0) as cleansheet
            from Fixtures
            where seasonType in {tuple(seasonTypeList)} and statusID >= 28
            order by eventId
            ) a ) b
            order by pts_cum desc, gd_cum desc)
            select l.*,
                   (select group_concat(l2.result order by l2.date separator '')
                    from l l2
                    where l2.seasonType = l.seasonType and l2.teamId = l.teamId and
                          l2.Rn between l.Rn and l.Rn + 4
                   ) as form
            from l
            where Rn = 1) lt1
            left join
            (select b.seasonType, b.teamId, b.next as next_opponent, b.homeAway as next_homeAway, 
            b.date as next_matchDate, b.statusId as next_match_status
            from
            (select row_number() OVER (PARTITION BY a.teamId, seasonType order by date) Rn, 
            a.date, a.teamId, a.next, a.homeAway, a.eventId, a.seasonType, a.statusId
            from
            ((select eventId,date, seasonType, hometeamId as teamId, awayTeamId as next, 
            hometeamForm as form, if(homeTeamOrder=0,"home","away") as homeAway, 
            statusId
            from Fixtures
            where SeasonType in {tuple(seasonTypeList)} and date > CURDATE()
            )
            union
            (select eventId, date,seasonType, awayteamId as teamId, homeTeamId as next, 
            awayteamForm as form, if(awayTeamOrder=1,"away","home")  as homeAway,
            statusId
            from Fixtures
            where SeasonType in {tuple(seasonTypeList)} and date > CURDATE()
            )
            ) a)b
            where b.Rn = 1
            order by b.date) lt2 on lt2.teamId = lt1.teamId and lt2.seasonType = lt1.seasonType
            ) a on a.seasonType = s.seasonType
            where s.seasonType in {tuple(seasonTypeList)} and s.teamId = a.teamId
            ORDER BY s.seasonType , s.teamRank;
            """

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

def exportPlayerInfo(mysqlDict,eventList):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                select p.id as athleteId,
            p.firstName,
            p.middleName,
            p.lastName,
            p.fullName,
            p.displayName,
            p.shortName,
            p.nickName,
            p.slug,
            p.displayWeight,
            p.weight,
            p.displayHeight,
            p.height, 
            p.age,
            p.dateOfBirth,
            p.gender,
            p.jersey,
            p.citizenship,
            p.birthPlaceCountry,
            p.positionName,
            p.positionId,
            p.positionAbbreviation,
            p.headshot_href as headshotUrl,
            p.headshot_alt,
            p.timestamp
            from PlayerDB p
            join 
            (
            SELECT distinct athleteId FROM TeamRoster
            where eventId in {tuple(eventList)})
            a on a.athleteId = p.id;
            """

    err = 0
    encoded_pwd = urllib.parse.quote_plus(pwd)
    connString = "mysql+pymysql://" + userId + ":" + encoded_pwd + "@" + hostName + ":3306/" + dbName
    try:
        engine = create_engine(connString)
        df = pd.read_sql(sql1, engine)
    except Exception as e:
        print("export PlayerInfo error")
        print(e)
        err = -1
    return(df,err)

def exportVenues(mysqlDict,venueList):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT id as venueId, fullName, shortName, capacity, city, country FROM Venues
            where id in {tuple(venueList)};
            """

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

def exportStatus(mysqlDict):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
            SELECT id as statusId, name, state, description FROM StatusType; 
            """

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

def exportTeamRoster(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                b.seasonYear,
                b.seasonType,
                b.league as midsizeName,
                b.teamId,
                b.teamName,
                b.athleteId,
                b.playerDisplayName,
                b.jersey,
                p.name as position,
                b.timeStamp
            FROM
                PlayerInTeam b
                    LEFT JOIN
                (SELECT 
                    x.seasonType,
                        x.teamId,
                        x.updateId,
                        COUNT(x.teamId) AS nPlayers,
                        MAX(y.currentUpdateId) AS currentUpdateId
                FROM
                    PlayerInTeam x
                JOIN (SELECT 
                    seasonType, teamId, MAX(updateId) AS currentUpdateId
                FROM
                    PlayerInTeam
                GROUP BY seasonType, teamId) y ON x.seasonType = y.seasonType
                    AND x.teamId = y.teamId
                GROUP BY x.seasonType , x.teamId , x.updateId) a ON b.updateId = a.updateId
                JOIN
                    PositionType p ON p.id = b.positionId
            WHERE
                b.seasonType in {tuple(seasonType)} and b.updateId = a.currentUpdateId
            ORDER BY b.updateId DESC, b.teamId, b.positionId, b.athleteId;
            """

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
    df = df.replace(-1, np.nan)
    return(df,err)

def exportKeyEventName(mysqlDict):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT keyEventId as keyEventTypeId, keyEventName FROM KeyEventType;
            """

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

def exportTeamStats(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    dbName = mysqlDict['dbName']
    #odbcDriver = mysqlDict['odbcDriver']
    osStr = mysqlDict['osStr']

    sql1 = f"""
                SELECT 
                a.seasonType,
                t.eventId,
                t.teamId,
                t.teamOrder,
                t.possessionPct,
                t.foulsCommitted,
                t.yellowCards,
                t.redCards,
                t.offsides,
                t.wonCorners,
                t.saves,
                t.totalShots,
                t.shotsOnTarget,
                t.shotPct,
                t.penaltyKickGoals,
                t.penaltyKickShots,
                t.accuratePasses,
                t.totalPasses,
                t.passPct,
                t.accurateCrosses,
                t.totalCrosses,
                t.crossPct,
                t.totalLongBalls,
                t.accurateLongBalls,
                t.longballPct,
                t.blockedShots,
                t.effectiveTackles,
                t.totalTackles,
                t.tacklePct,
                t.interceptions,
                t.effectiveClearance,
                t.totalClearance,
                t.updateTime
            FROM
                TeamStats t
                    INNER JOIN
                (SELECT 
                    eventId, seasonType
                FROM
                    Fixtures
                WHERE
                    seasonType IN {tuple(seasonType)} and statusId >= 28) a ON a.eventId = t.eventId
            /*WHERE  t.possessionPct > 0*/
            ORDER BY t.eventId , teamOrder;
            """

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
    df = df.replace(-1,np.nan)
    return(df,err)

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

def upload_to_kaggle(export_dir, dataset_name, dataset_title, dataset_description):
    """
    Upload all files from export directory to Kaggle as a dataset, zipped by subdirectory
    """
    try:
        # Initialize the Kaggle API
        api = KaggleApi()
        api.authenticate()
        
        import tempfile
        import shutil
        import os
        import zipfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Get all subdirectories in the export directory
            subdirs = [d for d in os.listdir(export_dir) 
                      if os.path.isdir(os.path.join(export_dir, d))]
            
            # Get all files in root directory
            root_files = [f for f in os.listdir(export_dir) 
                         if os.path.isfile(os.path.join(export_dir, f))]
            
            print(f"Found {len(subdirs)} subdirectories and {len(root_files)} root files")
            
            # Start progress thread for zipping
            start_time = time.time()
            progress_thread = threading.Thread(
                target=lambda: show_progress(start_time, "Zipping files")
            )
            progress_thread.daemon = True
            progress_thread.start()
            
            # Zip files from each subdirectory
            for subdir in subdirs:
                source_dir = os.path.join(export_dir, subdir)
                zip_path = os.path.join(temp_dir, f"{subdir}_data.zip")
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    files = os.listdir(source_dir)
                    for file in files:
                        file_path = os.path.join(source_dir, file)
                        if os.path.isfile(file_path):  # Only zip files, not subdirectories
                            zipf.write(file_path, os.path.basename(file_path))
            
            # Zip files from root directory
            if root_files:
                root_zip_path = os.path.join(temp_dir, "base_data.zip")
                with zipfile.ZipFile(root_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in root_files:
                        file_path = os.path.join(export_dir, file)
                        if file != "dataset-metadata.json":  # Skip metadata file if it exists
                            zipf.write(file_path, file)
            
            # Stop zipping progress thread
            progress_thread.join(timeout=0)
            if progress_thread.is_alive():
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_long(progress_thread.ident),
                    ctypes.py_object(SystemExit)
                )
            
            zip_time = time.time() - start_time
            print(f"\nZipping completed in {zip_time:.2f} seconds")
            
            # Create metadata file
            metadata = {
                "title": dataset_title,
                "id": dataset_name,
                "licenses": [{"name": "CC0-1.0"}],
                "description": (dataset_description + 
                              f"\n\nFiles are organized in {len(subdirs)} zip archives by category, " +
                              "plus one archive for base files.")
            }
            
            with open(os.path.join(temp_dir, "dataset-metadata.json"), 'w') as f:
                json.dump(metadata, f)
            
            # Start progress thread for upload
            start_time = time.time()

            # Create new dataset or update existing one
            api.dataset_create_version(
                folder=temp_dir,
                version_notes=f"Update {datetime.now().strftime('%Y-%m-%d')}",
                quiet=False
            )
            
            upload_time = time.time() - start_time
            print(f"\nUpload completed in {upload_time:.2f} seconds")
            print(f"Successfully uploaded dataset to Kaggle: {dataset_name}")
            
    except Exception as e:
        print(f"\nError uploading to Kaggle: {str(e)}")

#
# main
#
exportDir = "E:/soccer/espnExport/tmp/"

mysqlDict =  {"userId": "jtang",
                "pwd": "@CstAgt9903!",
                "hostName": "tang-03-lx",
                "dbName": "excel4soccer",
                "osStr": "Linux"}

seasonYearList = [2024]

# Assuming this is the section where eventSummary is generated
rs_summary = run_with_progress(exportSummary, mysqlDict, status_message="Exporting Summary")

eventSummary = rs_summary[0]
#print(len(eventSummary))

# Generate seasonType from eventSummary
seasonType = []
seasonTypeDict = {}
for season in eventSummary:
    # print(season)
    tmpSeasonType = int(season["seasonType"])
    tmpSeasonYear = int(season['year'])
    tmpMidSizeName = season['midsizeName']
    tmpCompletedEvents = int(season['nCompletedEvents'])
    tmpHomePlayerStats = int(season['nHomePlayerStats'])
    tmpAwayPlayerStats = int(season['nAwayPlayerStats'])
    if (tmpSeasonYear in seasonYearList and
            tmpCompletedEvents > 0
            and
            tmpHomePlayerStats == tmpCompletedEvents and
            tmpAwayPlayerStats == tmpCompletedEvents
            ):
        seasonYear = tmpSeasonYear
        seasonType.append(tmpSeasonType)
        seasonTypeDict[tmpSeasonType] = {"year": tmpSeasonYear, 'midsizeName': tmpMidSizeName}

nSeasons = len(seasonType)
print("total number of seasons",nSeasons)
# Execute exportStandings with progress spinner
df_standings, err_standings = run_with_progress(
    exportStandings, mysqlDict, seasonType, status_message="Exporting Standings"
)

# print(df_standings.info())
filename = exportDir + "standings.csv"
# Save DataFrame to CSV
df_standings.to_csv(filename, index=False)
print(filename)

df_team_list, err = exportTeamListFromSeasons(mysqlDict,seasonType)
# print(df_team_list.info())
teamList = df_team_list['teamId'].tolist()
print("total number of teams", len(teamList))

df_teams, err = exportTeams(mysqlDict,teamList)
#print(df_teams.info())
filename = exportDir + "teams.csv"
# Save DataFrame to CSV
df_teams.to_csv(filename, index=False)
print(filename)

df_fixtures, err = exportFixture(mysqlDict,seasonType)
# print(df_fixtures.info())
eventList = df_fixtures['eventId'].tolist()
print("total number of events", len(eventList))
tmpVenueList = df_fixtures['venueId'].tolist()
venueList = list(set(tmpVenueList))
print("total numbe of venues", len(venueList))

filename = exportDir + "fixtures.csv"
# Save DataFrame to CSV
df_fixtures.to_csv(filename, index=False)
print(filename)

df_roster, err = exportTeamRoster(mysqlDict,seasonType)
# print(df_roster.info())
filename = exportDir + "teamRoster.csv"
# Save DataFrame to CSV
df_roster.to_csv(filename, index=False)
print(filename)

df_teamStats, err = exportTeamStats(mysqlDict,seasonType)
# print(df_roster.info())
filename = exportDir + "teamStats.csv"
# Save DataFrame to CSV
df_teamStats.to_csv(filename, index=False)
print(filename)

df_players, err = exportPlayerInfo(mysqlDict,eventList)
# print(df_fixtures.info())
filename = exportDir + "players.csv"
# Save DataFrame to CSV
df_players.to_csv(filename, index=False)
print(filename)

df_venues, err = exportVenues(mysqlDict,venueList)
# print(df_fixtures.info())
filename = exportDir + "venues.csv"
# Save DataFrame to CSV
df_venues.to_csv(filename, index=False)
print(filename)

df_status, err = exportStatus(mysqlDict)
# print(df_fixtures.info())
filename = exportDir + "status.csv"
# Save DataFrame to CSV
df_status.to_csv(filename, index=False)
print(filename)

df_keyEventDescription, err = exportKeyEventName(mysqlDict)
# print(df_fixtures.info())
filename = exportDir + "keyEventDescription.csv"
# Save DataFrame to CSV
df_keyEventDescription.to_csv(filename, index=False)
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
                keyEvents = []
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
                j5 = 0
        j5 += 1
        for row in rs5:
            playerStats.append(row)
            #print(row)
    print("seasonType", tmpSeasonType, "processed", i, "out of", nSeasons,
          "complete playerStats", j5, bSave, oldMidsizeName)
    #
    # Reset oldMidsizeName to midsizeName at end of loop
    if bSave:
        oldMidsizeName = midsizeName


df_leagues, err = exportLeagues(mysqlDict,seasonType)
# print(df_leagues.info())
filename = exportDir + "leagues.csv"
# Save DataFrame to CSV
df_leagues.to_csv(filename, index=False)
print(filename)


dataset_name = "excel4soccer/espn-soccer-data"  # Replace with your desired dataset name
dataset_title = "ESPN Soccer Data"
dataset_description = """
This dataset contains detailed soccer match data compiled from ESPN public soccer data API.
This dataset contains multiple csv files.  The csv files include the following data:
- Match fixtures information
- Teams/clubs information
- Player information 
- Team Roster
- Match lineups
- Play-by-play information
- Key events
- Commentary
- Player statistics
- Team statics
Data is updated daily and covers major soccer leagues world wide
"""

upload_to_kaggle(
    export_dir=exportDir,
    dataset_name=dataset_name,
    dataset_title=dataset_title,
    dataset_description=dataset_description
)

