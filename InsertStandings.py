import pandas as pd
import numpy as np
import json
from datetime import datetime,timezone,date, timedelta
import os
from pathlib import Path
import csv

import extractESPNData01
import sqlConn
import ESPNSoccer

def getEventListWithStandings(mysqlDict):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver = mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    eventList = []

    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    sql1 = f"""
            select a.eventId, a.seasonType, a.seasonYear, a.midsizeName, a.leagueId
            from
            (
            select row_number() over (partition by seasonType order by matchDate desc) Rn, seasonYear, eventId, 
            seasonType, seasonName,midsizeName, matchDate, standings, leagueId
            from EventSnapshots) a 
            join (SELECT seasonType, nHomeTeams, nAwayTeams FROM EventSnapshotsSummary where lastMatchDate >= seasonEndDate
            ) b
            on b.seasonType = a.seasonType  
            left join (SELECT distinct seasonType FROM Standings) c
            on c.seasonType = a.seasonType
            where Rn = 1 and isNull(c.seasonType) and a.standings > 0 and (a.standings = b.nHomeTeams or a.standings = b.nAwayTeams)
            order by seasonYear, matchDate;
           """
    cursor.execute(sql1)
    # print(data1.rowcount)
    rs = cursor.fetchall()
    for rs_row in rs:
        eventId = rs_row[0]
        seasonType = rs_row[1]
        seasonYear = rs_row[2]
        midsizeName = rs_row[3]
        leagueId = rs_row[4]
        eventList.append({"seasonType":seasonType,
                          "eventId":eventId,
                          "seasonYear": seasonYear,
                          "midsizeName": midsizeName,
                          "leagueId": leagueId})
    return(eventList)
def getStandingsFromFixtures(mysqlDict,seasonType):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver = mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    standingsFromFixtures = {}

    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    sql1 = f"""
           select row_number() OVER (PARTITION BY lt1.seasonType order by lt1.pts_cum desc, lt1.gd_cum desc, lt1.gf_cum desc) teamRank,
                lt1.seasonType, lt1.date as last_match_date, t1.name as team, lt1.teamId, lt1.mp, lt1.win_cum as win, lt1.draw_cum as draw,lt1.loss_cum as loss,
                lt1.pts_cum as pts, lt1.gf_cum as gf, lt1.ga_cum as ga, lt1.gd_cum as gd, lt1.new_form as form, lt2.next as next_teamId, t2.name as next_team, lt2.homeAway as next_homeAway, lt2.date as next_match_date
                from
                (with l as 
                (
                select b.seasonType, b.date,b.teamId, b.win_cum +b.draw_cum + b.loss_cum as mp, b.win_cum, b.draw_cum, b.loss_cum, b.pts_cum,b.gf_cum, b.ga_cum, b.gf_cum-b.ga_cum as gd_cum, b.cleansheet_cum, b.result, b.Rn
                from
                (
                select a.seasonType, 
                row_number() OVER (PARTITION BY a.teamId order by date desc) Rn,
                a.date,a.eventId,a.teamId, a.form,
                a.win,a.draw, a.loss,
                if(a.win = 1, "W",if(a.draw =1,"D","L")) as result,
                sum(a.win) over(partition by a.teamId) as win_cum, 
                sum(a.draw) over (partition by a.teamId) as draw_cum, 
                sum(a.loss) over (partition by a.teamId) as loss_cum,
                a.pts,a.ga,a.gf,
                sum(a.pts) over(partition by a.teamId) as pts_cum, 
                sum(a.gf) over (partition by a.teamId) as gf_cum, 
                sum(a.ga) over (partition by a.teamId) as ga_cum,
                a.cleansheet,
                sum(a.cleansheet) over (partition by a.teamId) as cleansheet_cum
                from
                (
                (select eventId,date, seasonType, hometeamId as teamId, hometeamForm as form, homeTeamOrder as homeAway, 
                if(homeTeamScore > AwayTeamScore,1,0) as win,
                if (homeTeamScore = awayTeamScore,1,0) as draw,
                if(homeTeamScore < AwayTeamScore,1,0) as loss,
                if (homeTeamScore = awayTeamScore,1,if(homeTeamScore > AwayTeamScore,3,0)) as pts,
                homeTeamScore as gf,
                awayTeamScore as ga,
                if (awayTeamScore =0, 1, 0) as cleansheet
                from Fixtures
                where seasonType = %s and statusID = 28)
                union
                (select eventId, date,seasonType, awayteamId as teamId, awayteamForm as form, awayTeamOrder as homeAway,
                if(homeTeamScore < AwayTeamScore,1,0) as win,
                if (homeTeamScore = awayTeamScore,1,0) as draw,
                if(homeTeamScore > AwayTeamScore,1,0) as loss,
                if (homeTeamScore = awayTeamScore,1,if(homeTeamScore < AwayTeamScore,3,0)) as pts,
                awayTeamScore as gf,
                homeTeamScore as ga,
                if (homeTeamScore =0, 1, 0) as cleansheet
                from Fixtures
                where seasonType = %s and statusID = 28)
                ) a)b
                order by pts_cum desc, gd_cum desc)
                select l.*,
                       (select group_concat(l2.result order by l2.date separator '')
                        from l l2
                        where l2.teamId = l.teamId and
                              l2.Rn between l.Rn and l.Rn + 4
                       ) as new_form
                from l
                where Rn = 1) lt1
                join (select b.*
                from
                (select row_number() OVER (PARTITION BY a.teamId order by date) Rn, a.date, a.teamId, a.next, a.homeAway, a.eventId, a.seasonType, a.statusId
                from
                ((select eventId,date, seasonType, hometeamId as teamId, awayTeamId as next, hometeamForm as form, if(homeTeamOrder=0,"home","away") as homeAway, 
                statusId
                from Fixtures
                where seasonType = %s)
                union
                (select eventId, date,seasonType, awayteamId as teamId, homeTeamId as next, awayteamForm as form, if(awayTeamOrder=1,"away","home")  as homeAway,
                statusId
                from Fixtures
                where seasonType = %s)
                ) a)b
                where b.Rn = 1
                order by b.date
                ) lt2 on lt2.teamId = lt1.teamId
                join Teams t1 on t1.teamId = lt1.teamId
                join Teams t2 on t2.teamId = lt2.next
                ;
           """
    val = (seasonType,seasonType,seasonType,seasonType,)
    cursor.execute(sql1,val)
    rs = cursor.fetchall()
    nRow = cursor.rowcount
    if nRow ==0:
        return (standingsFromFixtures, -1)
    else:
        i = 0
        for rs_row in rs:
            i += 1
            teamId = rs_row[4]
            mp = rs_row[5]
            win = rs_row[6]
            draw = rs_row[7]
            loss = rs_row[8]
            pts = rs_row[9]
            gf = rs_row[10]
            ga = rs_row[11]
            gd = rs_row[12]
            #print(i, seasonType, teamId,mp,win,draw,loss,pts,gf,ga,gd)
            standingsFromFixtures[teamId] = {
                "seasonType":seasonType,
                "teamId":teamId,
                "mp":mp,
                "win":win,
                "draw":draw,
                "loss":loss,
                "pts":pts,
                "gf":gf,
                "ga":ga,
                "gd":gd,
                }
        return(standingsFromFixtures,0)
def Insert_StandingsFromEvents(rootDir, rootDir2,dataSet, dbConnect):
    # directory1 = rootDir
    directory2 = rootDir2 + 'tables/standings/'

    filename2 = directory2 + 'teamStandingsFromEvents.json'

    userId=dbConnect['userId']
    pwd=dbConnect['pwd']
    hostName=dbConnect['hostName']
    dbName=dbConnect['dbName']
    odbcDriver=dbConnect['odbcDriver']
    osStr=dbConnect['osStr']

    err = 0
    #defaultTime = datetime.strptime("1980-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    #defaultTimeZone = tz.gettz("UTC")
    #defaultTime = defaultTime.replace(tzinfo=defaultTimeZone)
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
            #sortedStandingsOld,err = get_Standings(dbConnect,seasonType)
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
            #df2['pointsAgainst'] = df2['pointsAgainst'].fillna(0).astype("int")
            df2['pointsAgainst'] = np.nan
            #df2['pointsFor'] = df2['pointsFor'].fillna(0).astype("int")
            df2['pointsFor'] = np.nan
            #df2['streak'] = df2['streak'].fillna(0).astype("int")
            df2['streak'] = np.nan
            df2['ties'] = df2['ties'].fillna(0).astype("int")
            df2['wins'] = df2['wins'].fillna(0).astype("int")
            #df2['awayGamesPlayed'] = df2['awayGamesPlayed'].fillna(0).astype("int")
            df2['awayGamesPlayed'] = np.nan
            #df2['awayLosses'] = df2['awayLosses'].fillna(0).astype("int")
            df2['awayLosses'] = np.nan
            #df2['awayPointsAgainst'] = df2['awayPointsAgainst'].fillna(0).astype("int")
            df2['awayPointsAgainst'] = np.nan
            #df2['awayPointsFor'] = df2['awayPointsFor'].fillna(0).astype("int")
            df2['awayPointsFor'] = np.nan
            #df2['awayTies'] = df2['awayTies'].fillna(0).astype("int")
            df2['awayTies'] = np.nan
            #df2['awayWins'] = df2['awayWins'].fillna(0).astype("int")
            df2['awayWins'] = np.nan
            df2['deductions'] = df2['deductions'].fillna(0).astype("int")
            #df2['homeGamesPlayed'] = df2['homeGamesPlayed'].fillna(0).astype("int")
            df2['homeGamesPlayed'] = np.nan
            #df2['homeLosses'] = df2['homeLosses'].fillna(0).astype("int")
            df2['homeLosses'] = np.nan
            #df2['homePointsAgainst'] = df2['homePointsAgainst'].fillna(0).astype("int")
            df2['homePointsAgainst'] = np.nan
            #df2['homePointsFor'] = df2['homePointsFor'].fillna(0).astype("int")
            df2['homePointsFor'] = np.nan
            #df2['homeTies'] = df2['homeTies'].fillna(0).astype("int")
            df2['homeTies'] = np.nan
            #df2['homeWins'] = df2['homeWins'].fillna(0).astype("int")
            df2['homeWins'] = np.nan
            #df2['ppg'] = df2['ppg'].fillna(0).astype("int")
            df2['ppg'] = np.nan
            df2['rank'] = df2['rank'].fillna(0).astype("int")
            #df2['rankChange'] = df2['rankChange'].fillna(0).astype("int")
            df2['rankChange'] = np.nan
            df2['timeStamp'] = pd.to_datetime(df2['timeStamp'], utc=True)
            #df2['timeStamp'] = df2['timeStamp'].fillna(defaultTime)

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
            df2 = df2.replace({np.nan: None})
            #standingsList = df2.to_dict('records')
            #sortedStandings = sorted(standingsList, key=lambda x: (x['rank'], x['teamId']))
            bInsert = True
            #if err < 0:
            #    bInsert = True
            #else:
            #    excludedKeys = ["timeStamp", "updateId"]
            #    bDiff = compare_table(sortedStandings,sortedStandingsOld,excludedKeys)
            #    if bDiff:
            #        bUpdate = True
            #if (bUpdate or bInsert) and seasonType in seasonList:
            print("Sstandings seasonType:", seasonType,"Processed", i, "out of", nTotSeasons, "update", bUpdate, "insert", bInsert)
            if bUpdate or bInsert:
                #print("new",len(sortedStandings))
                #print("old",len(sortedStandingsOld))
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

#
# main program
#
with open('config_db_lx.json','r') as file:
#with open('config_db.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)

# Read working directories from config_db.json
rootDir=Response['rootDir']
rootDir2=Response['rootDir2']
importLeagueFilter=Response['leagues']
Progress=Response['Progress']

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

eventList = getEventListWithStandings(mysqlDict)
print(len(eventList))
# print(eventList)

directory = rootDir
directory2 = rootDir2
dirEvents = directory + "events/"
dirStandings = directory + "standings/"
dirSaveStandings = directory2 + "tables/standings/"

standings = {}
i = 0
nMatched = 0
matchedSeasons = []
unMatchedSeasons = []
for tmpEvent in eventList:
    i += 1
    eventId = tmpEvent["eventId"]
    seasonType = tmpEvent["seasonType"]
    seasonYear = tmpEvent["seasonYear"]
    leagueId = tmpEvent["leagueId"]
    midsizeName = tmpEvent["midsizeName"]
    tmpSeason = {"seasonYear": seasonYear,
                 "seasonType": seasonType,
                 "leagueId": leagueId,
                 "midsizeName": midsizeName}
    standingsFromFixtures,err = getStandingsFromFixtures(mysqlDict, seasonType)
    (bEvent, event, errEvent) = extractESPNData01.openEvent(eventId, dirEvents,directory)
    if bEvent:
        tmpStandingsFromEvent = event['standings']['groups'][0]['standings']['entries']
        k = 0
        # standings[seasonType] = []
        tmpStandings = []
        errCode = 0
        for entry in tmpStandingsFromEvent:
            team = entry['team']
            teamId = int(entry['id'])
            stats = entry['stats']
            updateTime = event['updateTime']
            tmpStat = {'year': seasonYear,
                       'leagueId': leagueId,
                       'midsizeLeagueName':midsizeName,
                       'seasonType': seasonType,
                       'teamId': teamId}
            k += 1
            for stat in stats:
                if 'name' in stat and 'value' in stat:
                    tmpStat[stat['name']] = stat['value']
                elif stat['name'] == "overall":
                    tmpStat[stat['name']] = stat['summary']
            tmpStat['deductions'] = 0
            if 'points' not in tmpStat or 'wins' not in tmpStat:
                errCode = -3
                # print("incomplete", seasonType, tmpStat)
                break
            else:
                pts1 = tmpStat['points']
                pts2 = tmpStat['wins'] * 3 + tmpStat['ties']
                mp1 = tmpStat['gamesPlayed']
                wins1 = tmpStat['wins']
                ties1 = tmpStat['ties']
                losses1 = tmpStat['losses']
            if err == 0 and teamId in standingsFromFixtures:
                pts3 = standingsFromFixtures[teamId]['pts']
                mp3 = standingsFromFixtures[teamId]['mp']
                wins3 = standingsFromFixtures[teamId]['win']
                ties3 = standingsFromFixtures[teamId]['draw']
                losses3 = standingsFromFixtures[teamId]['loss']
                # print("from fixtures",i,seasonType,k,team,standingsFromFixtures[teamId])
            else:
                #print(teamId, "not in Fixtures")
                errCode = -1
                break
            if mp3 != mp1 or wins3 != wins1 or ties3 != ties1 or losses3 != losses1:
                print(teamId, "points not match")
                print("from fixtures",i,seasonType,midsizeName,k,team,standingsFromFixtures[teamId])
                print("from events  ",i, seasonType,midsizeName, k, team, tmpStat)
                errCode = -2
                break
            deductions = pts2 - pts1
            tmpStat['deductions'] = deductions
            tmpStat['timeStamp'] = updateTime
            tmpStandings.append(tmpStat)
            # print("from events  ", i,seasonType,k,team,tmpStat)
        if errCode == 0:
            print(i, "nMatched=", nMatched,seasonYear,seasonType,midsizeName, "matched")
            nMatched += 1
            tmpSeason['match'] = "matched"
            matchedSeasons.append(tmpSeason)
            standings[seasonType] = tmpStandings
        elif errCode == -1:
            print(i, "nMatched=", nMatched,seasonYear,seasonType,midsizeName, "teamId not in fixtures", teamId)
            tmpSeason['match'] = "teamid not in fixtures"
            unMatchedSeasons.append(tmpSeason)
        elif errCode == -2:
            print(i, "nMatched=", nMatched,seasonYear,seasonType,midsizeName, "pts not match", teamId)
            tmpSeason['match'] = "pts not match"
            unMatchedSeasons.append(tmpSeason)
        elif errCode == -3:
            print(i, "nMatched=", nMatched,seasonYear,seasonType,midsizeName, "incomplete standings in event")
            tmpSeason['match'] = "incomplete standings in event"
            unMatchedSeasons.append(tmpSeason)
    #if i >= 3:
    #    break
i = 0
for tmpSeason in unMatchedSeasons:
    i += 1
    print("not matched", i,tmpSeason)
i = 0
for tmpSeason in matchedSeasons:
    i += 1
    print("matched    ", i,tmpSeason)
print()
print("matched seasons", nMatched)
print("total seasons", i)

for seasonType in standings:
    tmpStandings = standings[seasonType]
    for item in tmpStandings:
        print(item)
    print()

filename = dirSaveStandings + 'teamStandingsFromEvents.json'
myStandings = []
for tmpKey in standings.keys():
    tmpStanding = standings[tmpKey]
    for item in tmpStanding:
        myStandings.append(item)
    #print('standings', tmpKey)
    #print('standings', tmpStanding)
with open(filename, 'w') as file:
    json.dump(myStandings, file)
file.close()
print('team standings in League',filename)

# Insert into Database.  Must first check that the entries are not already in database!
#startDate = "20240701"
#endDate = "20250630"
#currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
#
#dataSet = startDate + "-" + endDate + "-" + currentTime
#
#msg, err = Insert_StandingsFromEvents(rootDir, rootDir2, dataSet, mysqlDict)