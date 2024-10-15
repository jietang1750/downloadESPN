import pandas as pd
import json


def importJsonToDf(filename):
    err = 0
    try:
        with open(filename, "r") as file:
            tableJson = json.load(file)
        file.close
    except FileNotFoundError as e:
        err = -1
    if not(tableJson):
        err = -2
    if err != 0:
      tableJson=[{'error': err}]
    df = pd.json_normalize(tableJson)
    return df

def importCsvToDf(filename):
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError as e:
        errJson = [{"error": -1}]
        df = pd.json_normalize(errJson)
    return df


def importFixture1(league, directory):
    filename = league + "_fixtures_json.txt"
    filename = directory + filename
    # print(filename)
    df = importJsonToDf(filename)
    df["id"] = df["id"].astype("int")
    df["hometeamId"] = df["hometeamId"].astype("int")
    df["awayteamId"] = df["awayteamId"].astype("int")
    df["season"] = df["season"].astype("str")
    # print(df['updateTime'])
    df["updateTime"] = pd.to_datetime(df["updateTime"])
    inx = df["id"]
    # mask1 = inx.duplicated(keep=False) & (df_fixtures["status"] == 6)
    mask1 = inx.duplicated(keep="last")
    # print(df[mask1].to_string())
    df.drop(df[mask1].index, inplace=True)
    #print(df.info())
    return df


def importFixture2(league, year, directory, teamListDict):
    filename = league + "_" + year + "_" + "fixture.json"
    filename = directory + filename
    with open(filename, "r") as file:
        fixtureJson = json.load(file)
    file.close()
    fixtures = []
    for eventId in fixtureJson.keys():
        tmpFixture1 = {}
        tmpFixture2 = {}
        for key in fixtureJson[eventId].keys():
            tmpFixture1[key] = fixtureJson[eventId][key]
        hometeamId = int(tmpFixture1["homeTeamId"])
        awayteamId = int(tmpFixture1["awayTeamId"])
        hometeam = teamListDict[hometeamId]
        awayteam = teamListDict[awayteamId]
        tmpFixture2["id"] = eventId
        tmpFixture2["uid"] = tmpFixture1["uid"]
        tmpFixture2["league"] = tmpFixture1["league"]
        tmpFixture2["season"] = tmpFixture1["season"]
        tmpFixture2["status"] = tmpFixture1["status"]
        tmpFixture2["date"] = tmpFixture1["dateUTC"]
        tmpFixture2["name"] = awayteam + " at " + hometeam
        tmpFixture2["venue"] = tmpFixture1["venue"]
        tmpFixture2["hometeam"] = hometeam
        tmpFixture2["hometeamId"] = hometeamId
        tmpFixture2["awayteam"] = awayteam
        tmpFixture2["awayteamId"] = awayteamId
        tmpFixture2["homegoal"] = tmpFixture1["homeScore"]
        tmpFixture2["awaygoal"] = tmpFixture1["awayScore"]
        tmpFixture2["homeform"] = ""
        tmpFixture2["awayform"] = ""
        tmpFixture2["hasOdds"] = ""
        tmpFixture2["fileName"] = ""
        tmpFixture2["updateTime"] = tmpFixture1["updateTime"]
        # if eventId == "679247":
        #     print(tmpFixture2)
        fixtures.append(tmpFixture2)
    df = pd.json_normalize(fixtures)
    df["id"] = df["id"].astype("int")
    df["hometeamId"] = df["hometeamId"].astype("int")
    df["awayteamId"] = df["awayteamId"].astype("int")
    df["season"] = df["season"].astype("str")
    df["updateTime"] = pd.to_datetime(df["updateTime"])
    # print(df.info())
    return df


def importLeagueList(directory):
    filename = "leagueList.txt"
    filename = directory + filename
    with open(filename, "r") as file:
        leagueListJson = json.load(file)
    file.close()

    leagueList = []
    for id in leagueListJson:
        tmpLeague = leagueListJson[id]
        # print(tmpLeague)
        if "logos" in tmpLeague:
            try:
                logo1 = tmpLeague["logos"][0]["href"]
                logo1LastUpdated = tmpLeague["logos"][0]["lastUpdated"]
                tmpLeague["logoUrl1LastUpdated"] = logo1LastUpdated
                tmpLeague["logoUrl1"] = logo1
                # print(logo1, logo1LastUpdated)
            except:
                logo1 = ""
                logo1LastUpdated = ""
            if len(tmpLeague["logos"]) == 2:
                try:
                    logo2 = tmpLeague["logos"][1]["href"]
                    logo2LastUpdated = tmpLeague["logos"][1]["lastUpdated"]
                    tmpLeague["logoUrl2"] = logo2
                    tmpLeague["logoUrl2LastUpdated"] = logo2LastUpdated
                    # print(logo2, logo2LastUpdated)
                except:
                    logo1 = ""
                    logo1LastUpdated = ""
            leagueList.append(tmpLeague)
        else:
            print("no logo")

    df = pd.json_normalize(leagueList)
    df = df.drop(columns=["links", "logos"])
    df["id"] = df["id"].astype("int")
    df["alternateId"] = df["alternateId"].astype("int")
    # print(df.info())
    return df


def importScanFixture(directory):
    filename = "scanFixture.json"
    filename = directory + filename
    df = importJsonToDf(filename)
    df["id"] = df["id"].astype("int")
    df["leagueId"] = df["leagueId"].astype("int")
    df["hometeamId"] = df["hometeamId"].astype("int")
    df["awayteamId"] = df["awayteamId"].astype("int")
    df["updateTime"] = pd.to_datetime(df["updateTime"])
    # print(df.columns)
    return df


def importLineup2(directory):
    filename = "lineup2.txt"
    filename = directory + filename
    df = importJsonToDf(filename)
    if 'error' in df.columns:
        return df
    else:
        df["id"] = df["id"].astype("int")
        df["athleteId"] = df["athleteId"].astype("int")
    return df


def importSummary(league, directory):
    filename = "summary_" + league + ".json"
    filename = directory + "team_summary/" + filename
    df = importJsonToDf(filename)
    df = df.drop(columns=["links", "logo"])

    df["id"] = df["id"].astype("int")
    # print(df.to_string())
    # print(df.columns)
    return df


def importLeagueTables(filename, directory):
    # filename = 'league_table.txt'
    filename = directory + filename
    with open(filename, "r") as file:
        tableJson = json.load(file)
    file.close()

    tableList = []
    for teamId in tableJson.keys():
        tmpTable = tableJson[teamId]
        tmpTable["teamId"] = teamId
        tableList.append(tmpTable)
    df = pd.json_normalize(tableList)
    # print(df.columns)
    return df


def importStandings(league, year, directory):
    filename = league + "_" + year + "_table.txt"
    filename = directory + "/standings/" + filename
    try:
        with open(filename, "r") as file:
            response = json.load(file)
        file.close()
    except FileNotFoundError as e:
        errJson = [{"error": -1}]
        df = pd.json_normalize(errJson)
        return df
    standings = response["sports"][0]["leagues"][0]["teams"]
    timeStamp = response["sports"][0]["leagues"][0]["timeStamp"]
    df = pd.json_normalize(standings)
    df["updateTime"] = timeStamp
    if "team.record.rank" not in df:
        err = -1
    elif "team.record.points" not in df:
        err = -1
    elif "team.record.gamesPlayed" not in df:
        err = -1
    elif "team.record.wins" not in df:
        err = -1
    elif "team.record.ties" not in df:
        err = -1
    elif "team.record.losses" not in df:
        err = -1
    elif "team.record.pointsFor" not in df:
        err = -1
    elif "team.record.pointsAgainst" not in df:
        err = -1
    elif "team.record.pointDifferential" not in df:
        err = -1
    elif "team.record.deductions" not in df:
        err = -1
    elif "team.record.homeGamesPlayed" not in df:
        err = -1
    elif "team.record.homeGamesPlayed" not in df:
        err = -1
    elif "team.record.homeWins" not in df:
        err = -1
    elif "team.record.homeTies" not in df:
        err = -1
    elif "team.record.homeLosses" not in df:
        err = -1
    elif "team.record.homePointsFor" not in df:
        err = -1
    elif "team.record.homePointsAgainst" not in df:
        err = -1
    elif "team.record.awayGamesPlayed" not in df:
        err = -1
    elif "team.record.awayWins" not in df:
        err = -1
    elif "team.record.awayTies" not in df:
        err = -1
    elif "team.record.awayLosses" not in df:
        err = -1
    elif "team.record.awayPointsFor" not in df:
        err = -1
    elif "team.record.awayPointsAgainst" not in df:
        err = -1
    else:
        err = 0
    # print('err',err)
    if err == -1:
        errJson = [{"error": -1}]
        df = pd.json_normalize(errJson)
        return df
    df = df.drop(columns=["team.links", "team.logos"])
    err = 0
    df["team.id"] = df["team.id"].astype("int")
    # df['athleteId'] = df['athleteId'].astype('int')
    # print(df.to_string())
    # print(df.columns)
    return df


def importTeam(league, directory):
    filename = league + ".txt"
    filename = directory + "teams/" + league + "/" + filename
    df = importJsonToDf(filename)
    df = df.drop(columns=["links", "injuries", "flag.href", "flag.alt", "flag.rel"])
    df = df.drop(
        columns=[
            "seasons.$ref",
            "leagues.$ref",
            "transactions.$ref",
            "events.$ref",
            "defaultLeague.$ref",
        ]
    )
    df = df.drop(columns=["defaultTeam.$ref", "headshot.alt", "headshot.href"])
    df["id"] = df["id"].astype("int")
    # df['athleteId'] = df['athleteId'].astype('int')
    df["teamId"] = df["teamId"].astype("int")
    # print(df.columns)
    # print(df.to_string())
    return df


def importPlayerStats(directory):
    filename = "playerStats.txt"
    filename = directory + filename
    df = importJsonToDf(filename)
    # print(df.info())
    if 'error' in df.columns:
        err_playerStats = df.loc[0,'error']
        return(df)
    else:
        df["id"] = df["id"].astype("int")
        df["teamId"] = df["teamId"].astype("int")
        df["athlete.id"] = df["athlete.id"].astype("int")
        # df['athleteId'] = df['athleteId'].astype('int')
        # print(df.info())
        aggCols = [
        "appearances",
        "foulsCommitted",
        "foulsSuffered",
        "ownGoals",
        "yellowCards",
        "redCards",
        "subIns",
        "goalsConceded",
        "saves",
        "shotsFaced",
        "goalAssists",
        "shotsOnTarget",
        "totalGoals",
        "totalShots",
        "offsides"
        ]
        allCols=df.columns.tolist()
        (colFound,colMissing) = listIntersect(allCols,aggCols)
        if colFound:
            d1 = dict.fromkeys(colFound,'sum')
            df_new = (df.groupby(["athlete.id","teamId"]).agg(d1).reset_index())
            allAggCols = df_new.columns.tolist()
            (aggColFound, aggColMissing) = listIntersect(allAggCols, aggCols)
            print("playerStats. all columns:    ", allAggCols)
            print("playerStats. found columns:  ", aggColFound)
            print("playerStats. dropped columns:", aggColMissing)
            if len(aggColMissing) > 0:
                for colName in aggColMissing:
                    df_new[colName] = ''
            df_new = df_new[allAggCols]
        else:
            df_new=df
            df_new['error'] = -2
        #df_new = (
        #    df.groupby(["athlete.id", "teamId"])
        #    .agg(
        #        {
        #            "appearances": "sum",
        #            "foulsCommitted": "sum",
        #            "foulsSuffered": "sum",
        #            "ownGoals": "sum",
        #            "yellowCards": "sum",
        #            "redCards": "sum",
        #            "subIns": "sum",
        #            "goalsConceded": "sum",
        #            "saves": "sum",
        #            "shotsFaced": "sum",
        #            "goalAssists": "sum",
        #            "shotsOnTarget": "sum",
        #            "totalGoals": "sum",
        #            "totalShots": "sum",
        #            "offsides": "sum",
        #        }
        #    )
        #    .reset_index()
        #)
        # print(df_new.columns)
        return df_new


def importPlays(directory):
    filename = "plays.txt"
    filename = directory + filename
    df = importJsonToDf(filename)
    df = df.rename(columns={"athleteName": "playerName"})
    df["id"] = df["id"].astype("int")
    df["athleteId"] = df["athleteId"].astype("int")
    # print(df.to_string())
    # print(df.columns)
    return df


def importTeamStats(directory):
    filename = "teamStats.txt"
    filename = directory + filename
    df = importJsonToDf(filename)
    if 'error' in df.columns:
        return df
    else:
        df["id"] = df["id"].astype("int")
        df["teamId"] = df["teamId"].astype("int")
        #print('importTeamStats')
        #print(df.info())
        #print(df.to_string())
        #print(df.columns)
        return df


def importCleanSheets(directory):
    filename = "cleanSheets.txt"
    filename = directory + filename
    df = importJsonToDf(filename)
    df["teamId"] = df["teamId"].astype("int")
    # print(df.to_string())
    # print(df.columns)
    return df


def importStatusLog(league, directory):
    filename = league + "_event_status.log"
    filename = directory + filename
    df = importJsonToDf(filename)
    # print(df.columns)
    df["id"] = df["id"].astype("int")
    return df


def importPlayId(directory):
    filename = "playerId.txt"
    filename = directory + filename
    with open(filename, "r") as file:
        playerIdJson = json.load(file)
    file.close()
    err = 0
    playerIdList = []
    for playerId in playerIdJson:
        excel4soccerId = playerIdJson[playerId]
        tmpPlayer = {"athleteId": playerId, "excel4soccerId": excel4soccerId}
        playerIdList.append(tmpPlayer)
    df = pd.json_normalize(playerIdList)
    if playerIdList:
        df["athleteId"] = df["athleteId"].astype("int")
        df["excel4soccerId"] = df["excel4soccerId"].astype("int")
    else:
        err = -2
        df = pd.json_normalize([{'error':err}])
    return df


def importTimeConversion(directory):
    filename = "time_conversion_json.txt"
    filename = directory + filename
    print("Time Conversion filename:",filename)
    with open(filename, "r") as file:
        timeConversionJson = json.load(file)
    file.close()
    timeConversionList = []
    for tmpTime in timeConversionJson.keys():
        tmpTimeConversion = timeConversionJson[tmpTime]
        tmpTimeConversion["dateKey"] = tmpTime
        timeConversionList.append(tmpTimeConversion)
    df = pd.json_normalize(timeConversionList)
    # print(df.columns)
    return df


def importPositionSortOrder(directory):
    filename = "positionSortOrder.csv"
    filename = directory + filename
    df = importCsvToDf(filename)
    return df


def importTotPlayTime(directory):
    filename = "totPlayTime.txt"
    filename = directory + filename
    with open(filename, "r") as file:
        totPlayTimeJson = json.load(file)
    file.close()
    totPlayTimeList = []
    err = 0
    for athleteId in totPlayTimeJson.keys():
        tmpTotPlayTime = totPlayTimeJson[athleteId]
        tmpTotPlayTime["athleteId"] = athleteId
        totPlayTimeList.append(tmpTotPlayTime)
    if totPlayTimeList:
        df = pd.json_normalize(totPlayTimeList)
        df["athleteId"] = df["athleteId"].astype("int")
    else:
        err = -2
        print("no data in totPlayTimeJson.txt")
        df = pd.json_normalize([{'error':err}])
    return df


def processAllFixtures(
    df_scanFixtures, df_teamSummary, df_timeConversion, df_leagueList
):
    df = df_scanFixtures
    df = pd.merge(df, df_timeConversion, left_on="date", right_on="dateKey")
    df1 = pd.merge(df, df_teamSummary, left_on="hometeamId", right_on="id")
    df2 = pd.merge(df, df_teamSummary, left_on="awayteamId", right_on="id")
    df3 = pd.concat([df1, df2])
    df3 = pd.merge(df3, df_leagueList, left_on="leagueId", right_on="id")
    # print(df3.info())
    df_new = df3[
        [
            "dateEST",
            "name",
            "name_x",
            "venue",
            "hometeam",
            "homegoal",
            "awaygoal",
            "homeShootoutScore",
            "awayShootoutScore",
            "awayteam",
            "status",
            "updateTime_x",
        ]
    ]
    df_new = df_new.drop_duplicates()
    df_new = df_new.rename(
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
    # print(df_new.info())
    return df_new


def processAllFixtures2(
    df_scanFixtures, df_teamSummary, df_timeConversion, df_leagueList
):
    df = df_scanFixtures
    df = pd.merge(df, df_timeConversion, left_on="date", right_on="dateKey")
    df1 = pd.merge(df, df_teamSummary, left_on="hometeamId", right_on="id")
    df2 = pd.merge(df, df_teamSummary, left_on="awayteamId", right_on="id")
    df3 = pd.concat([df1, df2])
    df3 = pd.merge(df3, df_leagueList, left_on="leagueId", right_on="id")
    # print(df3.info())
    df_new = df3[
        [
            "date",
            "dateEST",
            "name",
            "name_x",
            "venue",
            "hometeam",
            "hometeamId",
            "homegoal",
            "awaygoal",
            "homeShootoutScore",
            "awayShootoutScore",
            "awayteamId",
            "awayteam",
            "status",
            "updateTime_x",
            # "updateTime",
        ]
    ]
    df_new = df_new.drop_duplicates()
    df_new = df_new.rename(
        columns={
            "date": "Match Date",
            "dateEST": "Match Time (US EST)",
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
    # print(df_new.info())
    return df_new


def processLineup2(
    df_lineup2,
    df_playerId,
    df_fixtures,
    df_timeConversion,
    #df_eventStatus,
    df_positionSortOrder,
):
    df = df_lineup2

    #print("playerId")
    #print(df_playerId.to_string())
    #print("fixtures")
    #print(df_fixtures.to_string())
    #print("time")
    #print(df_timeConversion.to_string())

    df = pd.merge(df, df_playerId, on="athleteId")
    #print("lineup2 before merge1")
    #print(df.info())
    #print(df.to_string())
    df = pd.merge(df, df_fixtures, on="id")
    #print("lineup2 after merge1")
    #print(df.info())
    #print(df.to_string())
    df = pd.merge(df, df_timeConversion, left_on="date", right_on="dateKey")
    #print("lineup2 after merge2")
    #print(df.info())
    #print(df.to_string())
    # df = pd.merge(df, df_eventStatus, on="id")
    df = pd.merge(df, df_positionSortOrder, on="position")
    #print("lineup2 after merge3")
    #print(df.info())
    #print(df.to_string())


    df_new = df[
        [
            "dateEST",
            "name",
            "hometeam",
            "homegoal",
            "awaygoal",
            "awayteam",
            "teamName",
            "excel4soccerId",
            "playerName",
            "jersey",
            "position",
            "formation",
            "Sort Order",
            "playTime",
            "subbedOutClock",
            "subbedOut",
            "subbedOutForName",
            "subbedOutForJersey",
            "subbedInClock",
            "subbedIn",
            "subbedInForName",
            "subbedInForJersey",
            "homeAway",
            "updateTime",
        ]
    ]

    #print("lineup2_new before rename")
    #print(df_new.info())
    #print(df_new.to_string())

    df_new = df_new.rename(
        columns={
            "dateEST": "Date Time (US Eastern)",
            "name": "Fixture",
            "teamName": "Team",
            "excel4soccerId": "Player Id",
            "playerName": "Player Name",
            "jersey": "Jersey",
            "position": "Position",
            "Sort Order": "Position Sort Order",
            "hometeam": "Home Team",
            "homegoal": "Home Goal",
            "awaygoal": "Away Goal",
            "awayteam": "Away Team",
            "formation": "Formation",
            "playtime": "Play Time",
            "subbedOutForName": "subbedOutByName",
            "subbedOutForJersey": "subbedOutByJersey",
        }
    )
    #print("lineup2_new")
    #print(df_new.info())
    #print(df_new.to_string())
    return df_new


def processPlays(df_plays, df_playerId, df_fixtures, df_timeConversion):
    df = df_plays
    df = pd.merge(df, df_playerId, on="athleteId")
    df = pd.merge(df, df_fixtures, on="id")
    df = pd.merge(df, df_timeConversion, left_on="date", right_on="dateKey")
    #    df = pd.merge(df, df_eventStatus, on="id")
    #    df = pd.merge(df, df_positionSortOrder, on="position")
    #    print(df.info())
    #    df_new = df
    colRequired = [
            "dateEST",
            "name",
            "hometeam",
            "homegoal",
            "awaygoal",
            "awayteam",
            "teamName",
            "excel4soccerId",
            "playerName",
            "jersey",
            "clockValue",
            "substitution",
            "yellowCard",
            "redCard",
            "scoringPlay",
            "didAssist",
            "didScore",
            "penaltyKick",
            "ownGoal",
            "homeAway",
            "updateTime",
        ]
    colInDf = df.columns.tolist()
    (colFound,colMissing) = listIntersect(colInDf,colRequired)
    print("Plays. all columns:     ",colInDf)
    print("Plays. required columns:",colRequired)
    print("Plays. found columns:   ",colFound)
    print("Plays. dropped columns: ",colMissing)
    df_new = df[colFound]
    if len(colMissing) >0:
        for colName in colMissing:
            df_new[colName] = ''
    df_new = df_new[colRequired]
    df_new = df_new.rename(
        columns={
            "dateEST": "Date Time (US Eastern)",
            "name": "Fixture",
            "hometeam": "Home Team",
            "homegoal": "Home Goal",
            "awaygoal": "Away Goal",
            "awayteam": "Away Team",
            "teamName": "Team",
            "excel4soccerId": "Player Id",
            "player Name": "Player Name",
            "jersey": "Jersey",
            "clockValue": "Clock",
            "substitution": "Substitution",
            "yellowCard": "Yellow Card",
            "redCard": "Red Card",
        }
    )
    print(df_new.info())
    return df_new
def listIntersect(list1, list2):
    list3 = list(set(list1) & set(list2))
    list4 = list(set(list3) ^ set(list2))
    return(list3,list4)
def processTeamStats(df_teamStats, df_fixtures, df_timeConversion):
    df = df_teamStats
    df = pd.merge(df, df_fixtures, on="id")
    df = pd.merge(df, df_timeConversion, left_on="date", right_on="dateKey")
    df = df.loc[df["status"] == "STATUS_FULL_TIME"]
    #print(df.info())
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


def processPlayerStats(df_playerStats, df_playerId, df_team, df_totPlayTime):
    df = df_playerStats
    # print('df_playerStats')
    # print(df_playerStats.info())
    # print('df_team')
    # print(df_team.info())
    df = pd.merge(
        df,
        df_team,
        how="left",
        left_on=["athlete.id", "teamId"],
        right_on=["id", "teamId"],
    )
    # df = pd.merge(df, df_team, left_on="athlete.id", right_on="id")
    df = pd.merge(df, df_playerId, left_on="athlete.id", right_on="athleteId")
    df = pd.merge(df, df_totPlayTime, left_on="athlete.id", right_on="athleteId")
    df["Weight(kg)"] = df["weight"] * 0.454
    df["Height(cm)"] = df["height"] * 2.54

    df_new = df[
        [
            "excel4soccerId",
            "displayName",
            "Weight(kg)",
            "Height(cm)",
            "age",
            "citizenship",
            "teamDisplayName",
            "jersey",
            "position.name",
            "totPlayTime",
            "avePlayTime",
            "totAppearance",
            "subIns",
            "foulsCommitted",
            "foulsSuffered",
            "ownGoals",
            "offsides",
            "yellowCards",
            "redCards",
            "goalAssists",
            "shotsOnTarget",
            "totalShots",
            "totalGoals",
            "goalsConceded",
            "shotsFaced",
            "saves",
            "timestamp",
        ]
    ]

    df_new = df_new.rename(
        columns={
            "excel4soccerId": "Id",
            "displayName": "Name",
            "age": "Age",
            "citizenship": "Citizenship",
            "teamDisplayName": "Team",
            "jersey": "Jersey",
            "position.name": "Position",
            "totAppearance": "Appearances",
            "totPlayTime": "Total Play Time(min)",
            "avePlayTime": "Average Play Time(min)",
            "subins": "Sub Ins",
            "timestamp": "Update Time",
        }
    )

    # print(df_new.info())
    return df_new


def processLeagueTable(df_standings, df_cleanSheets):
    df = df_standings
    df = pd.merge(df, df_cleanSheets, left_on="team.id", right_on="teamId")
    # print(df.info())
    df_new = df[
        [
            "team.record.rank",
            "team.name",
            #'team.record.rankChange','team.record.streak',
            "team.record.points",
            "team.record.gamesPlayed",
            "team.record.wins",
            "team.record.ties",
            "team.record.losses",
            "team.record.pointsFor",
            "team.record.pointsAgainst",
            "team.record.pointDifferential",
            "cleanSheets",
            #'team.record.ppg',
            "team.record.deductions",
            "team.record.homeGamesPlayed",
            "team.record.homeWins",
            "team.record.homeTies",
            "team.record.homeLosses",
            "team.record.homePointsFor",
            "team.record.homePointsAgainst",
            "cleanSheetsHome",
            "team.record.awayGamesPlayed",
            "team.record.awayWins",
            "team.record.awayTies",
            "team.record.awayLosses",
            "team.record.awayPointsFor",
            "team.record.awayPointsAgainst",
            "cleanSheetsAway",
            "updateTime",
        ]
    ]
    df_new = df_new.rename(
        columns={
            "team.record.rank": "Rank",
            "team.name": "Team",
            # 'team.record.rankChange','team.record.streak',
            "team.record.points": "Points",
            "team.record.gamesPlayed": "MP",
            "team.record.wins": "Win",
            "team.record.ties": "Draw",
            "team.record.losses": "Loss",
            "team.record.pointsFor": "GF",
            "team.record.pointsAgainst": "GA",
            "team.record.pointDifferential": "GD",
            "cleanSheets": "Clean Sheets",
            # 'team.record.ppg',
            "team.record.deductions": "Deductions",
            "team.record.homeGamesPlayed": "Home MP",
            "team.record.homeWins": "Home Win",
            "team.record.homeTies": "Home Draw",
            "team.record.homeLosses": "Home Loss",
            "team.record.homePointsFor": "Home GF",
            "team.record.homePointsAgainst": "Home GA",
            "cleanSheetsHome": "Home Clean Sheets",
            "team.record.awayGamesPlayed": "Away MP",
            "team.record.awayWins": "Away Win",
            "team.record.awayTies": "Away Draw",
            "team.record.awayLosses": "Away Loss",
            "team.record.awayPointsFor": "Away GF",
            "team.record.awayPointsAgainst": "Away GA",
            "cleanSheetsAway": "Away Clean Sheets",
            "updateTime": "Update Time",
        }
    )

    # print(df_new.info())
    return df_new


def processLeagueTable2(df_leagueTable, df_leagueTableHome, df_leagueTableAway):
    df = df_leagueTable
    df2 = df_leagueTableHome
    df3 = df_leagueTableAway
    df = pd.merge(df, df2, left_on="teamId", right_on="home.teamId")
    df = pd.merge(df, df3, left_on="teamId", right_on="away.teamId")
    # print(df.info())
    df["deductions"] = 0
    df_new = df[
        [
            "rank",
            "team",
            #'team.record.rankChange','team.record.streak',
            "pts",
            "mp",
            "win",
            "draw",
            "loss",
            "gf",
            "ga",
            "gd",
            "cleansheet",
            "deductions",
            #'team.record.ppg',`
            #'team.record.deductions',
            "home.mp",
            "home.win",
            "home.draw",
            "home.loss",
            "home.gf",
            "home.ga",
            "home.cleansheet",
            "away.mp",
            "away.win",
            "away.draw",
            "away.loss",
            "away.gf",
            "away.ga",
            "away.cleansheet",
            "updateTime",
        ]
    ]
    df_new = df_new.rename(
        columns={
            "rank": "Rank",
            "team": "Team",
            # 'team.record.rankChange','team.record.streak',
            "pts": "Points",
            "mp": "MP",
            "win": "Win",
            "draw": "Draw",
            "loss": "Loss",
            "gf": "GF",
            "ga": "GA",
            "gd": "GD",
            "cleansheet": "Clean Sheets",
            # 'team.record.ppg',
            "deductions": "Deductions",
            "home.mp": "Home MP",
            "home.win": "Home Win",
            "home.draw": "Home Draw",
            "home.loss": "Home Loss",
            "home.gf": "Home GF",
            "home.ga": "Home GA",
            "home.cleansheet": "Home Clean Sheets",
            "away.mp": "Away MP",
            "away.win": "Away Win",
            "away.draw": "Away Draw",
            "away.loss": "Away Loss",
            "away.gf": "Away GF",
            "away.ga": "Away GA",
            "away.cleansheet": "Away Clean Sheets",
            "updateTime": "Update Time",
        }
    )

    # print(df_new.info())
    return df_new


def genDataTables(
    rootDir, year, league, df_scanFixtures, df_timeConversion, df_positionSortOrder
):
    msg = "begin"
    errList=[]
    directory = rootDir + year + "/output/export_data/" + league + "/"
    directory1 = rootDir + year + "/"
    outputDir = (
        "C:/Users/Jie/Documents/Soccer/Soccer_Interactive_Table_2020/data/export_data/"
        + league
        + "/"
    )

    # cleanSheets = 'cleanSheets.txt'
    # filename = directory + cleanSheets
    df_cleanSheets = importCleanSheets(directory)

    # standings = league + '_'+ year + '_table.txt'
    # filename = directory1 + 'standings/' + '_table.txt'
    df_standings = importStandings(league, year, directory1)
    # print(df_standings.info())
    # print(df_standings.to_string())
    if "error" in df_standings:
        tableFileName = "leagueTable.txt"
        df_leagueTable = importLeagueTables(tableFileName, directory)
        tableFileName = "leagueTableHome.txt"
        df_leagueTableHome = importLeagueTables(tableFileName, directory)
        df_leagueTableHome.columns = [
            "home." + col for col in df_leagueTableHome.columns
        ]
        tableFileName = "leagueTableAway.txt"
        df_leagueTableAway = importLeagueTables(tableFileName, directory)
        df_leagueTableAway.columns = [
            "away." + col for col in df_leagueTableAway.columns
        ]
        df_league_table = processLeagueTable2(
            df_leagueTable, df_leagueTableHome, df_leagueTableAway
        )
    else:
        df_league_table = processLeagueTable(df_standings, df_cleanSheets)

    # summary = 'summary_' + league + '.json'2
    # filename = directory1 + 'team_summary/' + summary
    df_teamSummary = importSummary(league, directory1)
    df_teamList = pd.DataFrame()
    df_teamList["id"] = df_teamSummary["id"]
    df_teamList["name"] = df_teamSummary["name"]
    teamListDict = pd.Series(df_teamList.name.values, index=df_teamList.id).to_dict()
    # print(teamListDict)

    # fixtures = league + '_fixtures_json.txt'
    # filename = directory1 + fixtures
    df_fixtures = importFixture1(league, directory1)
    # df_fixtures = importFixture2(league, year, directory1, teamListDict)

    df_all_fixtures = processAllFixtures(
        df_scanFixtures, df_teamSummary, df_timeConversion, df_leagueList
    )

    # eventStatus = league + '_event_status.log'
    # filename = directory1 + eventStatus
    #df_eventStatus = importStatusLog(league, directory1)

    # totPlayTime = 'totPlayTime.txt'
    # filename = directory + totPlayTime
    df_totPlayTime = importTotPlayTime(directory)
    if 'error' in df_totPlayTime.columns:
        err_totPlayTime= df_totPlayTime.loc[0,'error']
        errList.append({'totPlayTime':err_totPlayTime})
        print("error in importTotPlayTime:", err_totPlayTime)
    else:
        err_totPlayTime = 0
    # lineup2 = 'playerId.txt'
    # filename = directory + lineup2
    df_playerId = importPlayId(directory)
    if 'error' in df_playerId:
        err_playerId = df_playerId.loc[0,'error']
        errList.append({'playerId':err_playerId})
        print("error in importPlayId", err_playerId)
    else:
        err_playerId = 0
    # lineup2 = 'lineup2.txt'
    # filename = directory + lineup2
    df_lineup2 = importLineup2(directory)
    if 'error' in df_lineup2:
        err_lineup2 = df_lineup2.loc[0,'error']
        print("error in lineup2", err_lineup2)
        errList.append({'lineup2':err_lineup2})
    else:
        err_lineup2 = 0
        df_lineup2_new = processLineup2(
            df_lineup2,
            df_playerId,
            df_fixtures,
            df_timeConversion,
            #df_eventStatus,
            df_positionSortOrder,
        )

    # teamStats = 'teamStats.txt'
    # filename = directory + teamStats
    df_teamStats = importTeamStats(directory)
    if 'error' in df_teamStats:
        err_teamStats = df_teamStats.loc[0,'error']
        print("error in teamStats:", err_teamStats)
        errList.append({'teamStats':err_teamStats})
    else:
        err_teamStats = 0
        df_teamStats_new = processTeamStats(df_teamStats, df_fixtures, df_timeConversion)

    # playerStats = 'playerStats.txt'
    # filename = directory + playerStats
    df_playerStats = importPlayerStats(directory)
    if 'error' in df_playerStats:
        err_playerStats = df_playerStats.loc[0,'error']
        print("error in playerStats:", err_playerStats)
        errList.append({'playerStats':err_playerStats})
    else:
        err_playerStats = 0
    df_team = importTeam(league, directory1)
    if err_playerId == 0 and err_totPlayTime == 0 and err_playerStats ==0 and err_playerStats == 0:
        df_playerStats_new = processPlayerStats(
            df_playerStats, df_playerId, df_team, df_totPlayTime
        )
        df_plays = importPlays(directory)
        df_plays_new = processPlays(df_plays, df_playerId, df_fixtures, df_timeConversion)
        err_plays = 0
    else:
        err_plays = -1

    filename = "LeagueTableExport.csv"
    filename = outputDir + filename
    df_league_table.to_csv(filename, index=False)
    print(filename)

    filename = "AllFixturesExport.csv"
    filename = outputDir + filename
    df_all_fixtures.to_csv(filename, index=False)
    print(filename)

    if err_lineup2 == 0:
        filename = "LineUpExport.csv"
        filename = outputDir + filename
        df_lineup2_new.to_csv(filename, index=False)
        print(filename)
    else:
        print("error in LineupExport:", err_lineup2)

    if err_plays ==0:
        filename = "PlaysExport.csv"
        filename = outputDir + filename
        df_plays_new.to_csv(filename, index=False)
        print(filename)
    else:
        print("error in PlaysExport:", err_plays)

    if err_teamStats == 0:
        filename = "TeamStatsExport.csv"
        filename = outputDir + filename
        df_teamStats_new.to_csv(filename, index=False)
        print(filename)
    else:
        print("error in teamStatsExport:", err_teamStats)

    # print(df_playerStats_new.to_string())
    if err_playerStats == 0:
        filename = "playerStatsExport.csv"
        filename = outputDir + filename
        df_playerStats_new.to_csv(filename, index=False)
        print(filename)
    else:
        print("error in playerStatsExport:", err_playerStats)
    return (errList, df_teamList, df_fixtures)


with open("configESPN.json", "r") as file:
    Response = json.load(file)
file.close()
print(Response)

rootDir = Response["rootDir"]
rootDir2 = Response["rootDir2"]
year = Response["year"]
#year = '2023'
leagues = ["ENG.1","ENG.3", "ENG.2","ENG.3", "GER.1", "FRA.1", "ESP.1", "ITA.1","KSA.1","TUR.1"]
#leagues = ["ENG.1","ENG.3", "ENG.2","ENG.3", "GER.1", "FRA.1", "ESP.1", "ITA.1"]
# leagues = ["ENG.2"]

directory2 = rootDir2

# scanFixtures =  'scanFixtures.json'
# filename = directory2 + scanFixtures
df_scanFixtures = importScanFixture(directory2)
updateTime_scanFixtures = df_scanFixtures.loc[0, "updateTime"]
# print(updateTime_scanFixtures)

# timeConversion = 'time_conversion_json.txt'
# filename = directory2 + timeConversion
df_timeConversion = importTimeConversion(directory2)

# positionSortOrder = 'positionSortOrder.csv'
# filename = directory2 + positionSortOrder
df_positionSortOrder = importPositionSortOrder(directory2)

# leagueList = 'leagueList.txt'
# filename = directory2 + leagueList
df_leagueList = importLeagueList(directory2)

# print(df_leagueList)

errDict = {}
df_teams = pd.DataFrame()
for league in leagues:
    print('League:',league)
    (errListTmp, df_team, df_fixtures) = genDataTables(
        rootDir,
        year,
        league,
        df_scanFixtures,
        df_timeConversion,
        df_positionSortOrder,
    )
    if errListTmp:
        msg = "complete with errors"
    else:
        msg = "complete"
    errDict[league] = errListTmp
    print(league, msg)
    updateTime_Fixtures = df_fixtures.loc[0, "updateTime"]
    if updateTime_Fixtures > updateTime_scanFixtures:
        df_scanFixtures.set_index("id", inplace=True)
        df_fixtures.set_index("id", inplace=True)
        df_scanFixtures.update(df_fixtures)
        df_scanFixtures.reset_index(inplace=True)
        df_fixtures.reset_index(inplace=True)
    df_teams = pd.concat([df_teams, df_team], axis=0)
df_teams = df_teams.reset_index(drop=True)
# print(df_teams.to_string())

df_allFixtures_allLeagues = processAllFixtures2(
    df_scanFixtures, df_teams, df_timeConversion, df_leagueList
)

outputDir = "C:/Users/Jie/Documents/Soccer/Soccer_Interactive_Table_2020/data/export_data/"

print("All fixtures, All leagues")
filename = "FullFixturesExport.csv"
filename = outputDir + filename
df_allFixtures_allLeagues.to_csv(filename, index=False)
print(filename)

print()
print("Summary")
for tmpLeague in errDict.keys():
    if errDict[tmpLeague]:
        print(tmpLeague, 'exited with errors',errDict[tmpLeague])
    else:
        print(tmpLeague, 'completed without error')