import json
from datetime import datetime,timezone,date, timedelta
from zoneinfo import ZoneInfo
import os
import csv
import extractESPNData01
import sqlConn
import ESPNSoccer
import sql_insert_all


def importLeagues(directory):
    #
    # Make a backup of league list and read updated leaguelist
    #
    filename = directory + 'leagueList.txt'
    filenameBak = directory + 'leagueList_bak.txt'

    ESPNSoccer.copyfile(filename, filenameBak)

    league_list = ESPNSoccer.get_league_list()
    with open(filename, 'w') as file:
        json.dump(league_list, file)
    file.close()
    msg = "import leagues successful"
    return (league_list,msg)
def todaysMatchInDB(mysqlDict,eventList):

    # query database today's matches
    #   mysqlImportList -- List of events of today's matches
    #       tmpEvent['id'] = tmpEventId
    #       tmpEvent['matchYear'] = tmpYear
    #       tmpEvent['matchDateTime'] = tmpDateTime
    #       tmpEvent['matchDate'] = tmpDate
    #       tmpEvent['seasonId'] = tmpSeasonId
    #       tmpEvent['leagueId'] = tmpLeagueId
    #       tmpEvent['midSizeName'] = tmpMidSizeLeagueName
    #       tmpEvent['homeTeamId'] = tmpHomeTeamId
    #       tmpEvent['awayTeamId'] = tmpAwayTeamId
    #       tmpEvent['matchStatus'] = tmpStatus
    #   mysqlEventList      -- List of eventIds
    #   mysqlDateList       -- List of Dates in US EST
    #   mysqlSeasonList     -- List of SeasonTypes
    #   mysqlLeagueList     -- List of LeagueIds
    #   mysqlMidSizeNameList-- List of league midsizenames
    #   mysqlTeamList       -- List of teams
    #

    userId=mysqlDict['userId']
    pwd=mysqlDict['pwd']
    hostName=mysqlDict['hostName']
    odbcDriver=mysqlDict['odbcDriver']
    dbName=mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    err = 0
    mysqlImportDict = {}
    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    sql1 = f"""
            SELECT 
            eventId as id,
            seasonYear as year,
            matchDate,
            DATE(CONVERT_TZ(matchDate, 'UTC', 'US/Eastern')) AS matchDate_US_Eastern,
            TIME(CONVERT_TZ(matchDate, 'UTC', 'US/Eastern')) AS matchTime_US_Eastern,
            seasonName,
            seasonType,
            leagueId,
            leagueName,
            midsizeName,
            homeTeamId,
            homeTeamName AS homeTeam,
            awayTeamId,
            awayTeamName AS awayTeam,
            statusId,
            statusName,
            hasCompetitions,
            hasBoxscore,
            hasGameInfo,
            hasOdds,
            hasRosters,
            details,
            nHomeStats,
            nAwayStats,
            nHomePlayers,
            nAwayPlayers,
            keyEvents,
            commentary,
            standings 
        FROM
            EventSnapshots 
        WHERE
            eventId in {tuple(eventList)} 
        ORDER BY matchDate;
        """
    cursor.execute(sql1)
    #print(data1.rowcount)
    rs = cursor.fetchall()

    # print(rs)
    for rs_row in rs:
        tmpEvent = {}
        # nOrder = rs_row[0]
        tmpEventId = rs_row[0]
        tmpYear = rs_row[1]
        tmpDateTime = rs_row[2]
        tmpDate = rs_row[3]
        tmpSeasonId = rs_row[6]
        tmpLeagueId = rs_row[7]
        tmpMidSizeLeagueName = rs_row[9]
        tmpHomeTeamId = rs_row[10]
        tmpAwayTeamId = rs_row[12]
        tmpStatus = rs_row[15]
        tmpHasBoxscore = rs_row[17]
        tmpHasGameInfo = rs_row[18]
        tmpHasOdds = rs_row[19]
        tmpHasRosters = rs_row[20]
        tmpDetails = rs_row[21]
        tmpHomeStats = rs_row[22]
        tmpAwayStats = rs_row[23]
        tmpHomePlayers = rs_row[24]
        tmpAwayPlayers = rs_row[25]
        tmpKeyEvents = rs_row[26]
        tmpCommentary = rs_row[27]
        tmpStandings = rs_row[28]
        tmpEvent['id'] = tmpEventId
        tmpEvent['matchYear'] = tmpYear
        tmpEvent['matchDateTime'] = tmpDateTime
        tmpEvent['matchDate'] = tmpDate
        tmpEvent['seasonId'] = tmpSeasonId
        tmpEvent['leagueId'] = tmpLeagueId
        tmpEvent['midsizeName'] = tmpMidSizeLeagueName
        tmpEvent['homeTeamId'] = tmpHomeTeamId
        tmpEvent['awayTeamId'] = tmpAwayTeamId
        tmpEvent['matchStatus'] = tmpStatus
        tmpEvent['hasBoxscore'] = tmpHasBoxscore
        tmpEvent['hasGameInfo'] = tmpHasGameInfo
        tmpEvent['hasOdds'] = tmpHasOdds
        tmpEvent['hasRosters'] = tmpHasRosters
        tmpEvent['details'] = tmpDetails
        tmpEvent['nHomeStats'] = tmpHomeStats
        tmpEvent['nAwayStats'] = tmpAwayStats
        tmpEvent['nHomePlayers'] = tmpHomePlayers
        tmpEvent['nAwayPlayers'] = tmpAwayPlayers
        tmpEvent['keyEvents'] = tmpKeyEvents
        tmpEvent['commentary'] = tmpCommentary
        tmpEvent['standings'] = tmpStandings
        #mysqlImportList.append(tmpEvent)
        if tmpEventId not in mysqlImportDict.keys():
            mysqlImportDict[tmpEventId] = tmpEvent
        else:
            print("duplicate enventId:",tmpEventId)
            err = 2
    # print(mysqlImportDict)
    # print('events', len(mysqlImportDict))
    conn.close()
    return(mysqlImportDict,err)
def todaysMatch(mysqlDict):
    # query database today's matches
    #   mysqlImportList -- List of events of today's matches
    #       tmpEvent['id'] = tmpEventId
    #       tmpEvent['matchYear'] = tmpYear
    #       tmpEvent['matchDateTime'] = tmpDateTime
    #       tmpEvent['matchDate'] = tmpDate
    #       tmpEvent['seasonId'] = tmpSeasonId
    #       tmpEvent['leagueId'] = tmpLeagueId
    #       tmpEvent['midSizeName'] = tmpMidSizeLeagueName
    #       tmpEvent['homeTeamId'] = tmpHomeTeamId
    #       tmpEvent['awayTeamId'] = tmpAwayTeamId
    #       tmpEvent['matchStatus'] = tmpStatus
    #   mysqlEventList      -- List of eventIds
    #   mysqlDateList       -- List of Dates in US EST
    #   mysqlSeasonList     -- List of SeasonTypes
    #   mysqlLeagueList     -- List of LeagueIds
    #   mysqlMidSizeNameList-- List of league midsizenames
    #   mysqlTeamList       -- List of teams
    #

    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver = mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']

    err = 0
    mysqlImportDict = {}
    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    sql1 = f"""
            SELECT 
            eventId as id,
            st.year as year,
            date,
            DATE(CONVERT_TZ(date, 'UTC', 'US/Eastern')) AS matchDate_US_Eastern,
            TIME(CONVERT_TZ(date, 'UTC', 'US/Eastern')) AS matchTime_US_Eastern,
            l.name,
            f.seasonType,
            f.leagueId,
            l.name,
            l.midsizeName,
            h.teamId AS homeTeamId,
            h.name AS homeTeam,
            a.teamId AS awayTeamId,
            a.name AS awayTeam,
            statusId,
            s.name
        FROM
            Fixtures f
                JOIN
            Teams h ON h.teamId = f.homeTeamId
                JOIN
            Teams a ON a.teamId = f.awayTeamId
                JOIN
            Leagues l ON l.id = f.leagueId
                JOIN
            StatusType s ON s.id = f.statusId
                JOIN
            SeasonType st ON st.typeId = f.seasonType
        WHERE
            DATE(date) = CURRENT_DATE()
        ORDER BY date;
        """
    data1 = cursor.execute(sql1)
    #print(data1.rowcount)
    rs = data1.fetchall()

    # print(rs)
    for rs_row in rs:
        tmpEvent = {}
        # nOrder = rs_row[0]
        tmpEventId = rs_row[0]
        tmpYear = rs_row[1]
        tmpDateTime = rs_row[2]
        tmpDate = rs_row[3]
        tmpSeasonId = rs_row[6]
        tmpLeagueId = rs_row[7]
        tmpMidSizeLeagueName = rs_row[9]
        tmpHomeTeamId = rs_row[10]
        tmpAwayTeamId = rs_row[12]
        tmpStatus = rs_row[15]
        tmpEvent['id'] = tmpEventId
        tmpEvent['matchYear'] = tmpYear
        tmpEvent['matchDateTime'] = tmpDateTime
        tmpEvent['matchDate'] = tmpDate
        tmpEvent['seasonId'] = tmpSeasonId
        tmpEvent['leagueId'] = tmpLeagueId
        tmpEvent['midSizeName'] = tmpMidSizeLeagueName
        tmpEvent['homeTeamId'] = tmpHomeTeamId
        tmpEvent['awayTeamId'] = tmpAwayTeamId
        tmpEvent['matchStatus'] = tmpStatus
        #mysqlImportList.append(tmpEvent)
        if tmpEventId not in mysqlImportDict.keys():
            mysqlImportDict[tmpEventId] = tmpEvent
        else:
            print("duplicate enventId:",tmpEventId)
            err = 2
    # print(mysqlImportDict)
    # print('events', len(mysqlImportDict))
    conn.close()
    return(mysqlImportDict,err)
def importFixtures(directory,league,league_list,mysqlDateList):

    db_start_date = mysqlDateList[0]
    db_end_date = mysqlDateList[-1]

    start_date = db_start_date
    end_date = db_end_date

    #startDateObj = datetime.strptime(str(start_date), "%Y-%m-%d")
    #endDateObj = datetime.strptime(str(end_date), "%Y-%m-%d")

    # print("DB Start Date and DB End Date:")
    # print(db_start_date, db_end_date)
    # print("Import Start Date and Import End Date:")
    # print(start_date, end_date)
    # print(startDateObj, endDateObj)
    for single_date in mysqlDateList:
        myDate = single_date.strftime("%Y%m%d")
        #print("mysqlDate", single_date)
        #print("myDate=", myDate)
        fileDir = directory + "fixture/" + league + "/"
        fileDir1 = fileDir + myDate + "/"
        #print(fileDir1)
        #print(fileDir)
        # Delete files id.txt in date directory.  Then delete date directory.
        if os.path.isdir(fileDir1):
            filelist = [f for f in os.listdir(fileDir1)]
            for f in filelist:
                os.remove(os.path.join(fileDir1, f))
            os.rmdir(fileDir1)
        # Delete date.txt file
        filename = fileDir + myDate + ".txt"
        if os.path.isfile(filename):
            print('file deleted', filename)
            os.remove(filename)
    #
    # Import fixture from start_date to end_date
    #
    reloadDate = []
    i = 0
    for single_date in ESPNSoccer.daterange(start_date, end_date):
        reloadDate.append(single_date)

    nLimit = 900
    logFileName = directory + 'import_fixture_byDate.log'
    (nMatchDates,nTotFixtures,) = ESPNSoccer.importFixtureByDate(reloadDate, directory, nLimit, league, league_list, logFileName)
    #print("number of dates:   ", nMatchDates)
    #print("number of Fixtures:", nTotFixtures)
    #
    # Import event from event_start_date to event_end_date
    #
    # directory = "D:/Users/Jie/Documents/Python Scripts/espn_soccer/"
    fixtures = []
    updatedId = []

    #
    #  Append event id to updatedId.
    #    The appended event id's are between event_start_date and event_end_date.
    #    These are the event id's to be imported.
    #  Append event json to fixtures for all events between db_start_date to db_end_date
    #
    #currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    # for single_date in ESPNSoccer.daterange(db_start_date, db_end_date):
    for single_date in reloadDate:
        myDate = single_date.strftime("%Y%m%d")
        #print(myDate)
        filename = directory + 'fixture/' + league + '/' + myDate + '.txt'
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                Response = json.load(file)
            file.close()
            events = Response
            for event in events:
                if 'fileName' not in event:
                    event['fileName'] = myDate + ".txt"
                if 'updateTime' not in event:
                    event['updateTime'] = currentTime
                fixtures.append(event)
                #if single_date >= event_start_date and single_date <= event_end_date:
                #    for event in events:
                #        updatedId.append(event['id'])

    newFixtures = ESPNSoccer.removeDuplicateFixtures(fixtures, rootDir)
    #
    # save fixtures json data to all_fixtures_byDate.json'
    #
    allFixtureJsonFileName = directory + 'imported_fixtures_byDate.json'
    allFixtureJsonBakFileName = directory + 'imported_fixtures_byDate_bak.json'

    print(allFixtureJsonFileName)
    print(allFixtureJsonBakFileName)

    if os.path.isfile(allFixtureJsonFileName):
        with open(allFixtureJsonFileName, "r") as file:
            oldFixtures = json.load(file)
        file.close()

        with open(allFixtureJsonBakFileName, "w") as file:
            json.dump(oldFixtures, file)
        file.close()
        print("number of events from previous download:",len(oldFixtures))
        print("number of events from current  download:",len(newFixtures))
    else:
        oldFixtures = newFixtures
        print("number of events from previous download:","0")
        print("number of events from current  download:",len(newFixtures))
    with open(allFixtureJsonFileName, 'w') as file:
        json.dump(newFixtures, file)
    file.close()
    return(nMatchDates,nTotFixtures,newFixtures)

def importEvents(bCompare, bSlow, newFixtures,directory,dirSnapshots,mysqlImportDict,mysqlDateList):

    db_start_date = mysqlDateList[0]
    db_end_date = mysqlDateList[-1]

    start_date = db_start_date
    end_date = db_end_date

    startDateObj = datetime.strptime(str(start_date), "%Y-%m-%d")
    endDateObj = datetime.strptime(str(end_date), "%Y-%m-%d") + timedelta(hours=23,minutes=59,seconds=59)

    # print(len(oldFixtures),len(newFixtures))

    #
    # Compare downloaded fixtures with fixtures in the database, create a new list of eventId to download
    # between db_start_date and db_end_date.
    # The new list of eventIds are stored in importList
    #
    # 9/1/24: a simple compare of match status in the fixture files is not enough.  The event files are not synced
    #   with the fixture files.  When the status changed in the fixture files, the event files are not changed.
    #   Temporarily disable compare.  bDiff is always set at True in compareFixtures2.
    #
    print()
    print("open event snap shots")
    filename = dirSnapshots + "eventSnapshots.json"
    #snapshotsDict = {}
    #print(filename)
    #if os.path.isfile(filename):
    #    with open(filename,"r") as file:
    #        snapShots = json.load(file)
    #    file.close()
    #    for snapshot in snapShots:
    #        tmpId = snapshot['eventId']
    #        snapshotsDict[tmpId] = snapshot
    #else:
    #    print("no snapshots")

    diffList = []
    importEventIdList = []
    importEventIdDict = {}
    # print(sorted(mysqlEventList))
    # print(sorted(list(mysqlImportDict.keys())))
    i=0
    tmpOldFixture = {}
    for tmpNewFixture in newFixtures:
        i += 1
        strEvent="no change"
        tmpId = int(tmpNewFixture['id'])
        tmpNewFixture['id'] = tmpId
        if tmpId in mysqlImportDict.keys() and bCompare:
            # print("compare event:",tmpId)
            tmpOldFixture = mysqlImportDict[tmpId]
            msg, bDiff = ESPNSoccer.compareFixtures3(bSlow,tmpOldFixture, tmpNewFixture, endDateObj)
        else:
            bDiff = True
            strEvent = "new event"
            msg = "new event"
            print(i, "of", len(newFixtures), tmpId, strEvent)
        if bDiff:
            if strEvent == "no change":
                strEvent = "changed event"
                # check if tmpOldFixture['matchDateTime'] and tmpOldFixture['matchDate'] is valid datetime object
                # 11/19/2024
                tmpMatchDateTimeOld = tmpOldFixture['matchDateTime']
                tmpMatchDateOld = tmpOldFixture['matchDate']
                if isinstance(tmpMatchDateTimeOld,datetime):
                    tmpOldFixture['matchDateTime'] = tmpMatchDateTimeOld.strftime("%Y-%m-%dT%H:%MZ")
                else:
                    print("invalid datetime format from oldfixture in tmpOldFixture['matchDateTime']",
                          tmpMatchDateTimeOld,tmpOldFixture['id'])
                    # tmpOldFixture['matchDateTime'] = "not a valid datetime object"
                if isinstance(tmpMatchDateOld,datetime):
                    tmpOldFixture['matchDate'] = tmpMatchDateOld.strftime("%Y-%m-%d")
                else:
                    print("invalid datetime format from oldfixture in tmpOldFixture['matchDate']",
                          tmpMatchDateOld, tmpOldFixture['id'])
                    # tmpOldFixture['matchDate'] = "not a valid datetime object"
                # tmpOldFixture['matchDateTime'] = tmpOldFixture['matchDateTime'].strftime("%Y-%m-%dT%H:%MZ")
                # tmpOldFixture['matchDate'] = tmpOldFixture['matchDate'].strftime("%Y-%m-%d")
                print(tmpNewFixture)
                print(tmpOldFixture)
                diffList.append(tmpNewFixture)
                diffList.append(tmpOldFixture)
                print(i, "of", len(newFixtures), tmpId,
                      tmpOldFixture['midsizeName'], msg,
                      tmpOldFixture['matchDate'],
                      tmpOldFixture['matchStatus'],
                      tmpNewFixture['date'],
                      tmpNewFixture['status'],
                      tmpNewFixture['name'])
            else:
                diffList.append(tmpNewFixture)
                diffList.append({})
            importEventIdList.append(tmpId)
            importEventIdDict[tmpId]={"eventId":tmpId,"importReason": msg}

    #print(diffList)
    diffFileName=directory+'diff_json.txt'
    with open(diffFileName, 'w') as file:
       json.dump(diffList, file)
    file.close()

    #print(importEventIdList)
    # print('no of changed events', len(importEventIdList))
    print('no of changed events', len(importEventIdDict))
    #
    # Combine importList (fixtures that are changed) and updateId
    #
    print('process import list...')
    i = 0
    fixture = {}
    duplicateIndex = []
    # importEventIdListNew = []
    importEventIdDictNew = {}
    importTeamListNew = []
    importLeagueListNew = {}

    for fixtureTmp in newFixtures:
        #print(fixtureTmp)
        id = int(fixtureTmp['id'])
        # print (fixture[filter1],fixture[filter2])
        fixture[id] = fixtureTmp
        tmpHomeTeamId = int(fixtureTmp['hometeamId'])
        tmpAwayTeamId = int(fixtureTmp['awayteamId'])
        tmpLeagueId = int(fixtureTmp['leagueId'])
        tmpMidSizeLeagueName = fixtureTmp['league']
        # if id in importList or id in updatedId:
        if id in importEventIdDict:
        #if id in importEventIdList:
            # importEventIdListNew.append(int(id))
            importEventIdDictNew[int(id)] = importEventIdDict[id]
            if tmpHomeTeamId not in importTeamListNew:
                importTeamListNew.append(tmpHomeTeamId)
            if tmpAwayTeamId not in importTeamListNew:
                importTeamListNew.append(tmpAwayTeamId)
            if tmpLeagueId not in list(importLeagueListNew.keys()):
                importLeagueListNew[tmpLeagueId] = tmpMidSizeLeagueName
        i += 1
    # print('no of events to import: ', len(importEventIdListNew))
    print('no of events to import: ', len(importEventIdDictNew))
    print('no of teams to import:  ', len(importTeamListNew))
    print('no of leagues to import:', len(importLeagueListNew.keys()))

    i = 0
    delim = ','

    # irestart=0, start from beginning
    # irestart=id, restart from eventid=id
    # irestart=29045

    indexFileName = directory + 'events_index_byDate.csv'
    logFileName = directory + 'events_byDate.log'
    logFile = open(logFileName, 'w')
    errorCode = [404, 2502, 3001, 1008, 1018, 2500, 2999, 2504]

    # nTotal = len(importEventIdListNew)
    nTotal = len(importEventIdDictNew)
    # print("import List:",importList)
    # print("import List New:",importListNew)

    #
    # Import event id
    # 1. bRestart=True: Not in the restart and id in updatedId.
    # 2. Or bRestart=False: id in updatedId.
    #
    bDebug = False  # Do not import event from ESPN when bDebug=True
    bImport = True
    # bImport = False
    delim = ','  # change delim to ','
    k = 0
    reimportList = []
    snapshotsDict = {}
    with (open(indexFileName, 'w', newline='', encoding='utf8') as csv_file):
        indexWriter = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # for id in fixture.keys():
        # for id in importEventIdListNew:
        for id in importEventIdDictNew:
            row = []
            i += 1
            line1 = str(i) + delim + str(id)
            # if bImport and int(id) in importListNew:
            season2 = 0
            leagueId2 = 0
            midsizeName2 = "none"
            seasonType2 = 0
            fixture[id]['seasonType'] = seasonType2  # add 'seasonType' to fixture[id]
            if bImport:
                k += 1
                (importedEvent, code) = ESPNSoccer.import_event(id, directory, bDebug)
                line = str(i) + delim + str(id) + delim + today.strftime("%Y%m%d") + delim + str(code) + '\n'
                logFile.write(line)
                line1 = 'import ' + str(k) + ' of ' + str(nTotal) + delim + str(code)
                if code > 0:
                    reimportList.append(id)
                else:
                    snapshot = ESPNSoccer.eventSnapshot(importedEvent, id)
                    snapshotsDict[id]=snapshot
                    (importedLeague, importedSeason, err) = ESPNSoccer.extractLeagueSeasonFromEvent(importedEvent)
                    if importedSeason:
                        if 'year' in importedSeason:
                            season2 = importedSeason['year']
                        else:
                            season2 = 0
                        if 'type' in importedSeason:
                            seasonType2 = importedSeason['type']
                        else:
                            seasonType2 = 0
                        # importedSeasonName = importedSeason['name']
                        fixture[id]['season'] = season2
                        fixture[id]['seasonType'] = seasonType2
                    if importedLeague:
                        if 'id' in importedLeague:
                            leagueId2 = importedLeague['id']
                        else:
                            leagueId2 = 0
                        # importedLeagueName = importedLeague['name']
                        if 'midsizeName' in importedLeague:
                            midsizeName2 = importedLeague['midsizeName']
                        else:
                            midsizeName2 = ""
                        # importedLeagueSlug = importedLeague['slug']
                        # importedLeagueAbbr = importedLeague['abbreviation']
                        # importedLeagueIsTournament = importedLeague['isTournament']
                        fixture[id]['leagueId'] = leagueId2
                        fixture[id]['league'] = midsizeName2
            leagueId1 = fixture[id]['leagueId']
            midsizeName1 = fixture[id]['league']
            season1 = fixture[id]['season']
            seasonType1 = fixture[id]['seasonType']
            name = fixture[id]['name']
            myDate = fixture[id]['date']
            status = fixture[id]['status']
            venue = fixture[id]['venue']
            hometeam = fixture[id]['hometeam']
            awayteam = fixture[id]['awayteam']
            homegoal = fixture[id]['homegoal']
            awaygoal = fixture[id]['awaygoal']
            line = line1 + delim + midsizeName1+ delim + str(leagueId1)
            line = line + delim + str(season1) + delim + str(seasonType1) + delim + myDate
            line = line + delim + str(id) + delim + name + delim + venue + delim + status
            row = [i, league, season1, myDate, id, name, venue, status]
            if status == "STATUS_FULL_TIME":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FIRST_HALF":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_SECOND_HALF":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_AET":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_AGT":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_PEN":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            print(line)
            line = line + '\n'
            indexWriter.writerow(row)

    length = len(reimportList)
    k = 0
    while length > 0 and k < 5:
        for id in reimportList:
            (importedEvent, code) = ESPNSoccer.import_event(id, directory, bDebug)
            print("reImport:", k + 1, id, code)
            if code == 0:
                snapshot = ESPNSoccer.eventSnapshot(importedEvent, id)
                snapshotsDict[id] = snapshot
                (importedLeague, importedSeason, err) = ESPNSoccer.extractLeagueSeasonFromEvent(importedEvent)
                if importedSeason:
                    season2 = importedSeason['year']
                    seasonType2 = importedSeason['type']
                    # importedSeasonName = importedSeason['name']
                    fixture[id]['season'] = season2
                    fixture[id]['seasonType'] = seasonType2
                if importedLeague:
                    leagueId2 = importedLeague['id']
                    # importedLeagueName = importedLeague['name']
                    midsizeName2 = importedLeague['midsizeName']
                    # importedLeagueSlug = importedLeague['slug']
                    # importedLeagueAbbr = importedLeague['abbreviation']
                    # importedLeagueIsTournament = importedLeague['isTournament']
                    fixture[id]['leagueId'] = leagueId2
                    fixture[id]['league'] = midsizeName2
                reimportList.remove(id)
                line = (
                        str(i)
                        + delim
                        + str(id)
                        + delim
                        + today.strftime("%Y%m%d")
                        + delim
                        + str(code)
                        + "\n"
                )
                logFile.write(line)
                # (tmpStatus, err) = ESPNSoccer.inspect_event(importedEvent)
                # eventStatusDict[id]["status"] = tmpStatus["status"]
                # eventStatusDict[id]["hasOdds"] = tmpStatus["hasOdds"]
                # eventStatusDict[id]["hasRoster"] = tmpStatus["hasRoster"]
                # eventStatusDict[id]["code"] = err
                # eventStatusDict[id]["importTime"] = currentTime
        k += 1
        length = len(reimportList)
    logFile.close()
    #
    # save event snapshots
    #
    snapshotfile = dirSnapshots + "eventSnapshots.json"
    snapshotsList = []
    for tmpId in snapshotsDict:
        snapshot = snapshotsDict[tmpId]
        snapshotsList.append(snapshot)
    with open(snapshotfile,"w") as file:
        json.dump(snapshotsList, file)
    file.close()
    #
    # regen importEventIdList, importTeamList and importLeagueList
    #
    importedEventIds = []
    importedTeamIds = []
    importedLeagues = []
    # for id in importEventIdListNew:
    for id in importEventIdDictNew:
        #print(fixture[id])
        tmpFixture = fixture[id]
        matchDate = tmpFixture['date']
        if int(id) not in importedLeagues:
            # importedEventIds.append(int(id))
            importedEventIds.append(importEventIdDict[id])
        hometeamId = int(tmpFixture['hometeamId'])
        if hometeamId not in importedTeamIds:
            importedTeamIds.append(hometeamId)
        awayteamId = int(tmpFixture['awayteamId'])
        if awayteamId not in importedTeamIds:
            importedTeamIds.append(awayteamId)
        leagueId = int(tmpFixture['leagueId'])
        midsizeName = tmpFixture['league']
        year = int(tmpFixture['season'])
        seasonType = int(tmpFixture['seasonType'])
        fileName = tmpFixture['fileName']
        if tmpLeagueId not in importedLeagues:
            importedLeagues.append({'eventId': int(id),
                                    'date':matchDate,
                                    'year':year,
                                    'seasonType':seasonType,
                                    'leagueId':leagueId,
                                    'midsizeName':midsizeName,
                                    'fileName':fileName})
    msg="Evnet download complete"
    return (importedEventIds,importedTeamIds,importedLeagues,msg)
def importEvents1(directory,league,league_list, mysqlDateList):

    db_start_date = mysqlDateList[0]
    db_end_date = mysqlDateList[-1]

    start_date = db_start_date
    end_date = db_end_date

    event_start_date = start_date
    event_end_date = end_date

    startDateObj = datetime.strptime(str(start_date), "%Y-%m-%d")
    endDateObj = datetime.strptime(str(end_date), "%Y-%m-%d")

    # print("DB Start Date and DB End Date:")
    # print(db_start_date, db_end_date)
    # print("Import Start Date and Import End Date:")
    # print(start_date, end_date)
    # print(startDateObj, endDateObj)
    for single_date in mysqlDateList:
        myDate = single_date.strftime("%Y%m%d")
        #print("mysqlDate", single_date)
        print("myDate=", myDate)
        fileDir = directory + "fixture/" + league + "/"
        fileDir1 = fileDir + myDate + "/"
        print(fileDir1)
        print(fileDir)
        # Delete files id.txt in date directory.  Then delete date directory.
        if os.path.isdir(fileDir1):
            filelist = [f for f in os.listdir(fileDir1)]
            for f in filelist:
                os.remove(os.path.join(fileDir1, f))
            os.rmdir(fileDir1)
        # Delete date.txt file
        filename = fileDir + myDate + ".txt"
        if os.path.isfile(filename):
            print('file deleted', filename)
            os.remove(filename)
    #
    # Import fixture from start_date to end_date
    #
    reloadDate = []
    i = 0
    for single_date in ESPNSoccer.daterange(start_date, end_date):
        reloadDate.append(single_date)

    nLimit = 900
    logFileName = directory + 'import_fixture_byDate.log'
    k = ESPNSoccer.importFixtureByDate(reloadDate, directory, nLimit, league, league_list, logFileName)
    #print("number of dates:", k)
    #
    # Import event from event_start_date to event_end_date
    #
    # directory = "D:/Users/Jie/Documents/Python Scripts/espn_soccer/"
    fixtures = []
    updatedId = []

    #
    #  Append event id to updatedId.
    #    The appended event id's are between event_start_date and event_end_date.
    #    These are the event id's to be imported.
    #  Append event json to fixtures for all events between db_start_date to db_end_date
    #
    #currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    # for single_date in ESPNSoccer.daterange(db_start_date, db_end_date):
    for single_date in reloadDate:
        myDate = single_date.strftime("%Y%m%d")
        #print(myDate)
        filename = directory + 'fixture/' + league + '/' + myDate + '.txt'
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                Response = json.load(file)
            file.close()
            events = Response
            for event in events:
                if 'fileName' not in event:
                    event['fileName'] = myDate + ".txt"
                if 'updateTime' not in event:
                    event['updateTime'] = currentTime
                fixtures.append(event)
                #if single_date >= event_start_date and single_date <= event_end_date:
                #    for event in events:
                #        updatedId.append(event['id'])

    newFixtures = ESPNSoccer.removeDuplicateFixtures(fixtures, rootDir)
    #
    # save fixtures json data to all_fixtures_byDate.json'
    #
    allFixtureJsonFileName = directory + 'imported_fixtures_byDate.json'
    allFixtureJsonBakFileName = directory + 'imported_fixtures_byDate_bak.json'

    print(allFixtureJsonFileName)
    print(allFixtureJsonBakFileName)

    if os.path.isfile(allFixtureJsonFileName):
        with open(allFixtureJsonFileName, "r") as file:
            oldFixtures = json.load(file)
        file.close()

        with open(allFixtureJsonBakFileName, "w") as file:
            json.dump(oldFixtures, file)
        file.close()
        print("number of events from previous download:",len(oldFixtures))
        print("number of events from current  download:",len(newFixtures))
    else:
        oldFixtures = newFixtures
        print("number of events from previous download:","0")
        print("number of events from current  download:",len(newFixtures))
    with open(allFixtureJsonFileName, 'w') as file:
        json.dump(newFixtures, file)
    file.close()

    # print(len(oldFixtures),len(newFixtures))

    #
    # Compare downloaded fixtures with fixtures in the base, create a new list of eventId to download
    # between db_start_date and db_end_date.
    # The new list of eventIds are stored in importList
    #
    diffList = []
    importEventIdList = []
    # print(sorted(mysqlEventList))
    # print(sorted(list(mysqlImportDict.keys())))
    i=0
    for tmpNewFixture in newFixtures:
        i += 1
        strEvent="no change"
        tmpId = int(tmpNewFixture['id'])
        tmpNewFixture['id'] = tmpId
        if tmpId in mysqlImportDict.keys():
            # print("compare event:",tmpId)
            tmpOldFixture = mysqlImportDict[tmpId]
            bDiff = ESPNSoccer.compareFixtures2(tmpOldFixture, tmpNewFixture, startDateObj)
        else:
            bDiff = True
            strEvent="new event"
        if bDiff:
            if strEvent == "no change":
                strEvent = "changed event"
                # check if tmpOldFixture['matchDateTime'] and tmpOldFixture['matchDate'] is valid datetime object
                # 11/19/2024
                tmpMatchDateTimeOld = tmpOldFixture['matchDateTime']
                tmpMatchDateOld = tmpOldFixture['matchDate']
                if isinstance(tmpMatchDateTimeOld,datetime):
                    tmpOldFixture['matchDateTime'] = tmpMatchDateTimeOld.strftime("%Y-%m-%dT%H:%MZ")
                else:
                    print("invalid datetime format from oldfixture", tmpMatchDateTimeOld)
                    # tmpOldFixture['matchDateTime'] = "not a valid datetime object"
                if isinstance(tmpMatchDateOld,datetime):
                    tmpOldFixture['matchDate'] = tmpMatchDateOld.strftime("%Y-%m-%d")
                else:
                    print("invalid datetime format from oldfixture", tmpMatchDateOld)
                    # tmpOldFixture['matchDate'] = "not a valid datetime object"
                # tmpOldFixture['matchDateTime'] = tmpOldFixture['matchDateTime'].strftime("%Y-%m-%dT%H:%MZ")
                # tmpOldFixture['matchDate'] = tmpOldFixture['matchDate'].strftime("%Y-%m-%d")
                # print(tmpNewFixture)
                # print(tmpOldFixture)
                diffList.append(tmpNewFixture)
                diffList.append(tmpOldFixture)
            else:
                diffList.append(tmpNewFixture)
                diffList.append({})
            importEventIdList.append(tmpId)
        print(i,"of", len(newFixtures),tmpId,strEvent,"different?",bDiff)
    # print(diffList)

    diffFileName=directory+'diff_json.txt'
    with open(diffFileName, 'w') as file:
       json.dump(diffList, file)
    file.close()

    #print(importEventIdList)
    print('no of changed events', len(importEventIdList))
    #
    # Combine importList (fixtures that are changed) and updateId
    #
    print('process import list...')
    i = 0
    fixture = {}
    duplicateIndex = []
    importEventIdListNew = []
    importTeamListNew = []
    importLeagueListNew = {}

    for fixtureTmp in newFixtures:
        #print(fixtureTmp)
        id = int(fixtureTmp['id'])
        # print (fixture[filter1],fixture[filter2])
        fixture[id] = fixtureTmp
        tmpHomeTeamId = int(fixtureTmp['hometeamId'])
        tmpAwayTeamId = int(fixtureTmp['awayteamId'])
        tmpLeagueId = int(fixtureTmp['leagueId'])
        tmpMidSizeLeagueName = fixtureTmp['league']
        # if id in importList or id in updatedId:
        if id in importEventIdList:
            importEventIdListNew.append(int(id))
            if tmpHomeTeamId not in importTeamListNew:
                importTeamListNew.append(tmpHomeTeamId)
            if tmpAwayTeamId not in importTeamListNew:
                importTeamListNew.append(tmpAwayTeamId)
            if tmpLeagueId not in list(importLeagueListNew.keys()):
                importLeagueListNew[tmpLeagueId] = tmpMidSizeLeagueName
        i += 1
    print('no of events to import: ', len(importEventIdListNew))
    print('no of teams to import:  ', len(importTeamListNew))
    print('no of leagues to import:', len(importLeagueListNew.keys()))

    i = 0
    delim = ','

    # irestart=0, start from beginning
    # irestart=id, restart from eventid=id
    # irestart=29045

    indexFileName = directory + 'events_index_byDate.csv'
    logFileName = directory + 'events_byDate.log'
    logFile = open(logFileName, 'w')
    errorCode = [404, 2502, 3001, 1008, 1018, 2500, 2999, 2504]

    nTotal = len(importEventIdListNew)
    # print("import List:",importList)
    # print("import List New:",importListNew)

    #
    # Import event id
    # 1. bRestart=True: Not in the restart and id in updatedId.
    # 2. Or bRestart=False: id in updatedId.
    #
    bDebug = False  # Do not import event from ESPN when bDebug=True
    bImport = True
    #bImport = False
    delim = ','  # change delim to ','
    k = 0
    reimportList = []
    with (open(indexFileName, 'w', newline='', encoding='utf8') as csv_file):
        indexWriter = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # for id in fixture.keys():
        for id in importEventIdListNew:
            row = []
            i += 1
            line1 = str(i) + delim + str(id)
            # if bImport and int(id) in importListNew:
            season2 = 0
            leagueId2 = 0
            midsizeName2 = "none"
            seasonType2 = 0
            fixture[id]['seasonType'] = seasonType2  # add 'seasonType' to fixture[id]
            if bImport:
                k += 1
                (importedEvent, code) = ESPNSoccer.import_event(id, directory, bDebug)
                line = str(i) + delim + str(id) + delim + today.strftime("%Y%m%d") + delim + str(code) + '\n'
                logFile.write(line)
                line1 = 'import ' + str(k) + ' of ' + str(nTotal) + delim + str(code)
                if code > 0:
                    reimportList.append(id)
                else:
                    (importedLeague, importedSeason, err) = ESPNSoccer.extractLeagueSeasonFromEvent(importedEvent)
                    if importedSeason:
                        season2 = importedSeason['year']
                        seasonType2 = importedSeason['type']
                        # importedSeasonName = importedSeason['name']
                        fixture[id]['season'] = season2
                        fixture[id]['seasonType'] = seasonType2
                    if importedLeague:
                        leagueId2 = importedLeague['id']
                        # importedLeagueName = importedLeague['name']
                        midsizeName2 = importedLeague['midsizeName']
                        # importedLeagueSlug = importedLeague['slug']
                        # importedLeagueAbbr = importedLeague['abbreviation']
                        # importedLeagueIsTournament = importedLeague['isTournament']
                        fixture[id]['leagueId'] = leagueId2
                        fixture[id]['league'] = midsizeName2
            leagueId1 = fixture[id]['leagueId']
            midsizeName1 = fixture[id]['league']
            season1 = fixture[id]['season']
            seasonType1 = fixture[id]['seasonType']
            name = fixture[id]['name']
            myDate = fixture[id]['date']
            status = fixture[id]['status']
            venue = fixture[id]['venue']
            hometeam = fixture[id]['hometeam']
            awayteam = fixture[id]['awayteam']
            homegoal = fixture[id]['homegoal']
            awaygoal = fixture[id]['awaygoal']
            line = line1 + delim + midsizeName1+ delim + str(leagueId1)
            line = line + delim + str(season1) + delim + str(seasonType1) + delim + myDate
            line = line + delim + str(id) + delim + name + delim + venue + delim + status
            row = [i, league, season1, myDate, id, name, venue, status]
            if status == "STATUS_FULL_TIME":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FIRST_HALF":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_SECOND_HALF":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_AET":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_AGT":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            elif status == "STATUS_FINAL_PEN":
                score = homegoal + ":" + awaygoal
                line = line + delim + score
                row.append(score)
            print(line)
            line = line + '\n'
            indexWriter.writerow(row)

    length = len(reimportList)
    k = 0
    while length > 0 and k < 5:
        for id in reimportList:
            (importedEvent, code) = ESPNSoccer.import_event(id, directory, bDebug)
            print("reImport:", k + 1, id, code)
            if code == 0:
                (importedLeague, importedSeason, err) = ESPNSoccer.extractLeagueSeasonFromEvent(importedEvent)
                if importedSeason:
                    season2 = importedSeason['year']
                    seasonType2 = importedSeason['type']
                    # importedSeasonName = importedSeason['name']
                    fixture[id]['season'] = season2
                    fixture[id]['seasonType'] = seasonType2
                if importedLeague:
                    leagueId2 = importedLeague['id']
                    # importedLeagueName = importedLeague['name']
                    midsizeName2 = importedLeague['midsizeName']
                    # importedLeagueSlug = importedLeague['slug']
                    # importedLeagueAbbr = importedLeague['abbreviation']
                    # importedLeagueIsTournament = importedLeague['isTournament']
                    fixture[id]['leagueId'] = leagueId2
                    fixture[id]['league'] = midsizeName2
                reimportList.remove(id)
                line = (
                        str(i)
                        + delim
                        + str(id)
                        + delim
                        + today.strftime("%Y%m%d")
                        + delim
                        + str(code)
                        + "\n"
                )
                logFile.write(line)
                # (tmpStatus, err) = ESPNSoccer.inspect_event(importedEvent)
                # eventStatusDict[id]["status"] = tmpStatus["status"]
                # eventStatusDict[id]["hasOdds"] = tmpStatus["hasOdds"]
                # eventStatusDict[id]["hasRoster"] = tmpStatus["hasRoster"]
                # eventStatusDict[id]["code"] = err
                # eventStatusDict[id]["importTime"] = currentTime
        k += 1
        length = len(reimportList)
    logFile.close()
    #
    # regen importEventIdList, importTeamList and importLeagueList
    #
    importedEventIds = []
    importedTeamIds = []
    importedLeagues = []
    for id in importEventIdListNew:
        #print(fixture[id])
        tmpFixture = fixture[id]
        matchDate = tmpFixture['date']
        if int(id) not in importedLeagues:
            importedEventIds.append(int(id))
        hometeamId = int(tmpFixture['hometeamId'])
        if hometeamId not in importedTeamIds:
            importedTeamIds.append(hometeamId)
        awayteamId = int(tmpFixture['awayteamId'])
        if awayteamId not in importedTeamIds:
            importedTeamIds.append(awayteamId)
        leagueId = int(tmpFixture['leagueId'])
        midsizeName = tmpFixture['league']
        year = int(tmpFixture['season'])
        seasonType = int(tmpFixture['seasonType'])
        fileName = tmpFixture['fileName']
        if tmpLeagueId not in importedLeagues:
            importedLeagues.append({'eventId': int(id),
                                    'date':matchDate,
                                    'year':year,
                                    'seasonType':seasonType,
                                    'leagueId':leagueId,
                                    'midsizeName':midsizeName,
                                    'fileName':fileName})
    msg="Evnet download complete"
    return (importedEventIds,importedTeamIds,importedLeagues,msg)
def checkEvents(direcotry):
    indexfilename = directory + 'events_index_byDate.csv'
    indexfile = open(indexfilename, 'r')
    delim = '|'

    filelist = [f for f in os.listdir(dirEvents)]
    i = 0
    k = 0
    error_codes = {}
    missing_event_list = []
    open_event_list = []
    kTotal = len(filelist)

    indexlist = []
    with open(indexfilename, 'r', encoding='utf8') as csv_file:
        indexReader = csv.reader(csv_file, delimiter=',')
        for row in indexReader:
            k += 1
            dateStr = row[3]
            id = row[4]
            filenameTmp = str(id) + '.txt'
            indexlist.append(filenameTmp)
            if filenameTmp in filelist:
                open_event_list.append(id)
            else:
                missing_event_list.append(id)
    print()
    i = 0
    # for idFile in filelist:
    #    if idFile not in indexlist:
    #        i +=1
    #        os.remove(os.path.join(dir2, idFile))
    #        print('Remove extra events file',i,idFile)
    print()
    print('Total No of events:', len(filelist))
    print('Total No of items in event index file', k)
    print('Total No of events to open', len(open_event_list))
    print('Total No of events missing', len(missing_event_list))
    print('Removed No of events not in event index file', i)
    k = 0
    print('scan events for error')
    for id in open_event_list:
        (event, code) = ESPNSoccer.open_event(id, directory)
        if code != 0:
            i += 1
            if code not in error_codes:
                error_codes[code] = []
            error_codes[code].append(id)
            if 'detail' in event:
                detail = event['detail']
            else:
                detail = 'no detail'
            # print(k,i,id,code,detail)
        k += 1
        # print('Processed',k, 'out of', len(open_event_list))
    print()

    k = 0
    for error_code in error_codes.keys():
        for id in error_codes[error_code]:
            k += 1
            print(k, error_code, id)

    codeImportList = [404, 2502, 3001, 1008, 1018, 2500, 2999, 2504]

    failedImport = {}
    for codeImport in codeImportList:
        if codeImport in error_codes.keys():
            i = 0
            length = len(error_codes[codeImport])

            for id in error_codes[codeImport]:
                i += 1
                (event, code) = ESPNSoccer.import_event(id, directory)
                print(i, length, id, code)
                if code != 0:
                    failedImport[id] = code
        else:
            print("Code " + str(codeImport) + " does not exist.")

    i = 0
    length = len(missing_event_list)
    for id in missing_event_list:
        i += 1
        (event, code) = ESPNSoccer.import_event(id, directory)
        if code == 0:
            if 'boxscore' in event.keys():
                print('missing', i, length, id, event['boxscore']['teams'][0]['team']['name'], 'v',
                      event['boxscore']['teams'][1]['team']['name'], code)
            else:
                print('missing', length, id, 'missing keys')
        else:
            print('missing', i, id, code)
            failedImport[id] = code

    #filename = directory + 'failedImport.txt'
    #with open(filename, 'w') as file:
    #    json.dump(failedImport, file)
    #file.close()

    newFailedImport = ESPNSoccer.importFailedAttempts(directory,failedImport)
    print(len(newFailedImport))
    return ("Event Data Check complete",newFailedImport)
def importStandings(rootDir,importedLeagues):
    dir1 = rootDir
    dir2 = rootDir + "fixture/all/"
    dir3 = rootDir + "teams/"
    dir4 = rootDir + "standings/"
    dir5 = rootDir + "leagues/"

    filename1 = dir5 + "leagues.json"
    filename2 = dir5 + "seasons.json"
    filename3 = dir5 + "leagueLogos.json"
    filename4 = dir5 + "leagueCalendar.json"

    # leagueList = ESPNSoccer.openLeagueList(dir1)
    # print(leagueList)

    importLeagueList = {}
    for tmpLeague in importedLeagues:
        leagueId = tmpLeague['leagueId']
        if leagueId not in importLeagueList:
            importLeagueList[leagueId]={'midsizeName':tmpLeague['midsizeName'],
                                        'seasonType':tmpLeague['seasonType']}
    seasonTypeList = []
    seasons = []
    logos = []
    leagues = []
    calendar = []
    errorList = []
    teamsInLeagues = {}
    n = 0
    m = 0
    today = date.today()
    calendarYear = today.year
    i = 0
    nImportLeagues = len(importLeagueList)
    for leagueId in importLeagueList:
        i += 1
        midSizeLeagueName = importLeagueList[leagueId]['midsizeName']
        seasonType1 = importLeagueList[leagueId]['seasonType']
        # print(i, 'of', len(importLeagueList), leagueId, midSizeLeagueName)
        if midSizeLeagueName == "none":
            err1 = 3
        else:
            (tmpLeague, tmpSeasons, tmpLogos, matchCalendar, tmpSeasonTypeList, err1) \
                = ESPNSoccer.import_league_status_espn(midSizeLeagueName)
        if err1 == 0:
            # There is possibility to that the seasonType from ['season']['type'] and
            # ['leagues'][0]['season']'['type']['type'] do not match.
            # In such cases,
            # 1. there will be two both seasons are added in tmpSeasons
            # 2. Use ['leagues'][0]['season']'['type']['type'] as the typeId for standings
            #    This is the last item in tmpSeasons
            seasonYear = tmpSeasons[-1]['year']
            seasonType = tmpSeasons[-1]['typeId']
            if seasonType1 != seasonType:
                print("seasonType mismatch.",midSizeLeagueName,
                      "Old seasonType = ",seasonType1, "New seasonType = ", seasonType, tmpSeasonTypeList)
            for tmpSeasonType in tmpSeasonTypeList:
                seasonTypeList.append(tmpSeasonType)
            for tmpSeason in tmpSeasons:
                seasons.append(tmpSeason)
            for tmpLogo in tmpLogos:
                logos.append(tmpLogo)
            for tmpCalendar in matchCalendar:
                calendar.append(tmpCalendar)
            leagues.append(tmpLeague)
            # print(tmpLeague)
            errmsg = 'status no errors'
            n += 1
        elif err1 == -1:
            errmsg = 'status no items in record'
            seasonYear = calendarYear
            seasonType = 0
            errorList.append({'seasonType': seasonType,
                              'midsizeLeagueName': midSizeLeagueName,
                              'leagueId': leagueId,
                              'teamId': -1,
                              'err': err1,
                              'errMsg1': errmsg,
                              'errMsg2': ""})
        elif err1 == 3:
            errmsg = 'status midSizeName error ' +  midSizeLeagueName
            seasonYear = calendarYear
            seasonType = 0
            errorList.append({'seasonType': seasonType,
                              'midsizeLeagueName': midSizeLeagueName,
                              'leagueId': leagueId,
                              'teamId': -1,
                              'err': err1,
                              'errMsg1': errmsg,
                              'errMsg2': ""})
        else:
            errmsg = 'status unknown error'
            seasonYear = calendarYear
            seasonType = 0
            errorList.append({'seasonType': seasonType,
                              'midsizeLeagueName': midSizeLeagueName,
                              'leagueId': leagueId,
                              'teamId': -1,
                              'err': err1,
                              'errMsg1': errmsg,
                              'errMsg2': ""})
        #print('league status',n,  'of', len(importLeagueList), "seasonType=",seasonType,
        #      "leagueId=",leagueId, midSizeLeagueName, errmsg)
        if midSizeLeagueName == "none":
            err2=3
        else:
            (standings,tmpTeams,err2,tmpErrList) \
                = ESPNSoccer.import_league_table_espn(seasonYear, seasonType, dir4, midSizeLeagueName,leagueId)
            n1 = standings['sports'][0]['leagues'][0]['teamsInLeague']
            n2 = standings['sports'][0]['leagues'][0]['teamsHasRecords']
            n3 = standings['sports'][0]['leagues'][0]['defaultLeague']
            outFileName = standings['sports'][0]['leagues'][0]['outputFileName']
            print(i, "of", nImportLeagues,"err=",err2,
                  "no of teams in", league, n1,"With Record", n2, "defaultLeague", n3,
                  "output file name:",outFileName)
        if err2 == 0:
            errmsg = 'standings no errors'
            teamsInLeagues[seasonType] = tmpTeams
            m += 1
        elif err2 == 3:
            errmsg = 'standings midSizeName error ' + midSizeLeagueName
            errorList.append({'seasonType': seasonType,
                               'midsizeLeagueName': midSizeLeagueName,
                               'leagueId': leagueId,
                               'teamId': -1,
                               'err': err,
                               'errMsg1': errmsg,
                               'errMsg2': ""})
        else:
            for tmpErr in tmpErrList:
                errorList.append(tmpErr)
        #print('league table ',n, 'of', len(importLeagueList), "seasonType=",seasonType,
        #      "leagueId=",leagueId,midSizeLeagueName, errmsg)
    #print(teamsInLeagues)

    print("Items in Error List:")
    for error in errorList:
        print(error)

    filename1 = dir5 + "leagues.json"
    filename2 = dir5 + "seasons.json"
    filename3 = dir5 + "leagueLogos.json"
    filename4 = dir5 + "leagueCalendar.json"

    print(filename1)
    with open(filename1, 'w') as file:
        json.dump(leagues, file)
    file.close()

    print(filename2)
    with open(filename2, 'w') as file:
        json.dump(seasons, file)
    file.close()

    print(filename3)
    with open(filename3, 'w') as file:
        json.dump(logos, file)
    file.close()

    print(filename4)
    with open(filename4, 'w') as file:
        json.dump(calendar, file)
    file.close()
    msg = "standings download complete"
    return (nImportLeagues,teamsInLeagues,msg)

def importRoster(rootDir,teamsInLeagues):
    dir1 = rootDir + "teams/"

    athleteDBFileName = 'athleteDB.txt'

    iAthlete = 0
    k = 0
    codeImportList = [404, 2502, 3001, 1008, 2500, 2999, 2504, 990, 980, 970]
    errorImport = {}
    statNames = {}
    nLeagues=len(teamsInLeagues)
    for tmpSeasonType in teamsInLeagues.keys():
        errorImport[tmpSeasonType] = []
        k += 1
        athletesDB = []
        tmpTeams = teamsInLeagues[tmpSeasonType]
        nTeams = len(tmpTeams)
        i = 0
        reimportList = []
        for tmpTeam in tmpTeams:
            i += 1
            teamId = tmpTeam['teamId']
            midSizeName = tmpTeam['midSizeLeagueName']
            (roster, code) = ESPNSoccer.import_team(teamId, midSizeName)
            teamName = tmpTeam['name']
            if code == 0:
                (athletesDB, iAthlete, statNames) = ESPNSoccer.saveAthleteDB(roster,
                                                                             k,
                                                                             nLeagues,
                                                                             i,
                                                                             nTeams,
                                                                             iAthlete,
                                                                             athletesDB,
                                                                             midSizeName,
                                                                             teamId,
                                                                             teamName,
                                                                             tmpSeasonType,
                                                                             statNames)
            elif code in codeImportList:
                reimportList.append(teamId)
        if not reimportList:
            for teamId in reimportList:
                (roster, code) = ESPNSoccer.import_team(teamId, midSizeName)
                print(code, teamId)
                if code == 0:
                    (athletesDB, iAthlete, statNames) = ESPNSoccer.saveAthleteDB(roster,
                                                                                 k,
                                                                                 nLeagues,
                                                                                 i,
                                                                                 nTeams,
                                                                                 iAthlete,
                                                                                 athletesDB,
                                                                                 midSizeName,
                                                                                 teamId,
                                                                                 teamName,
                                                                                 tmpSeasonType,
                                                                                 statNames)
                elif code in codeImportList:
                    errorImport[midSizeName].append(code, teamId)
            #    if iAthlete>200:
            #        break
        athleteDBFileName = dir1 + midSizeName + "_" + str(tmpSeasonType) + '.txt'
        print(athleteDBFileName)
        with open(athleteDBFileName, 'w') as file:
            json.dump(athletesDB, file)
        file.close()

    errorFileName = dir1 + 'import_error.txt'
    print(errorFileName)
    with open(errorFileName, 'w') as file:
        json.dump(errorImport, file)
    file.close()

    statFileName = dir1 + 'statNames.txt'
    print(statFileName)
    with open(statFileName, 'w') as file:
        json.dump(statNames, file)
    file.close()

    return("download roster complete!")
def downloadLogos(logoDir,teams):
    logoStatusFileName = logoDir + 'logoStatus.json'
    #currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    logoStatus = {}
    err1 = -1
    err2 = -1
    if os.path.isfile(logoStatusFileName):
        with open(logoStatusFileName, 'r') as file:
            logoStatusList = json.load(file)
        file.close()
        #print(logoStatusList)
        for tmpId in logoStatusList.keys():
            tmpLogoStatus=logoStatusList[tmpId]
            id = int(tmpLogoStatus['id'])
            #if 'err' in tmpLogoStatus:
            #    err = tmpLogoStatus['err']
            if 'err1' in tmpLogoStatus:
                err1 = tmpLogoStatus['err1']
            if 'err2' in tmpLogoStatus:
                err2 = tmpLogoStatus['err2']
            downloadTime = tmpLogoStatus['downloadTime']
            logoStatus[id] = {"id": id,
                              "err1": err1,
                              "err2": err2,
                              "downloadTime":downloadTime}
    n=0
    nLogo = 0
    nTeams = len(teams)
    for teamId in teams:
        n += 1
        team = teams[teamId]
        id = int(team['id'])
        name = team['name']
        logoUrl1 = team['logoUrl1']
        logoUrl2 = team['logoUrl2']
        #print(n,'id:', id, 'name:', name, 'id:',id)
        if id not in logoStatus.keys():
            bDownload = True
        elif logoStatus[id]["err1"] < 0 or logoStatus[id]["err2"] < 0:
            bDownload = True
        else:
            bDownload = False
        if bDownload:
            if logoUrl1 != "":
                logoFileName1 = logoDir + str(id) + "_1.png"
                err1 = ESPNSoccer.downloadFilefromUrl(logoUrl1, logoFileName1)
            if logoUrl2 != "":
                logoFileName2 = logoDir + str(id) + "_2.png"
                err2 = ESPNSoccer.downloadFilefromUrl(logoUrl2, logoFileName2)
            if err1 == 0 or err2 == 0:
                nLogo += 1
            logoStatus[id] = {"id":id,"err1":err1,"err2":err2,"downloadTime": currentTime}
            print(str(n).ljust(5), "of", str(nTeams).ljust(5),
                  "id:",str(id).ljust(6),"name:",name.ljust(25),
                  "err:",err1,err2,
                  'logoUrls:', logoUrl1,logoUrl2)
    msg = "logos download complete"
    with open(logoStatusFileName, 'w') as file:
        json.dump(logoStatus, file)
    file.close()
    return(msg,nLogo)


#
# Main program
#
timeObj1 = datetime.now(ZoneInfo("America/New_York"))

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

#
# Download mode
# nDownload = 1: (default)
#   start date = today's date + offsetDays
#   end date = dbEndDate 
# nDownload = 2: 
#   start date = dbStartDate
#   end date = dbEndDate 
# nDownload = 3:
#   start date = today's date
#   end date = today's date + 1
#
nDownload = 1    # default
#nDownload = 3
#nDownload = 0

bImport = True
#bImport = False
#
#bCompare = True, compare with snapshot and only import changed events
#bCompare = False, import all fixtures and events
#
bCompare = True
#bCompare = False
#
#bSlow = True, download all past events 
#bSlow = False, download events with incomplete data set 
#
bSlow = True    # download all past events
#bSlow = False    # download events with incomplete data set

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

#year=Response['year']

# start date and end date as defined in config_db.json
offsetDays=Response['offsetDays']
dBStartDateStr=Response['dbStartDateStr']
dBEndDateStr=Response['dbEndDateStr']
#offsetDays=Response['offsetDays']
#eventOffsetDaysMinus=Response['eventOffsetDaysMinus']
#eventOffsetDaysPlus=Response['eventOffsetDaysPlus']

directory = rootDir
directory2 = rootDir2
dirEvents = directory + "events/"
dirSnapshots = directory2 + "tables/"
dirTmp = directory + "tmp/"

# set import dates.
today=date.today()
print("todaysdate (system_date/US_EASTERN):",today)
print()

db_start_date = datetime.strptime(dBStartDateStr, "%Y-%m-%d").date()
db_end_date = datetime.strptime(dBEndDateStr, "%Y-%m-%d").date()

print("DB Start Date and DB End Date:")
print(db_start_date,db_end_date)

# import events from yesterday, today and tomorrow
#end_date = today + timedelta(days=1)
#start_date =today - timedelta(days=1)

# import events from today+offsetDays to db_end_date
if nDownload ==1:
    start_date = today+timedelta(days=offsetDays)
    end_date = db_end_date
elif nDownload ==2:
    start_date = db_start_date
    end_date = db_end_date
elif nDownload ==3:
    start_date = today
    end_date = today + timedelta(days=1)
else:
    start_date = today
    end_date = today + timedelta(days=0)

importDates = []
for single_date in ESPNSoccer.daterange(start_date,end_date):
    importDates.append(single_date)
# print(importDates)
#
# import Full Leaguelist
#
(league_list,msg) = importLeagues(directory)
print(msg)
print()

#
#  1. Downloads fixtures and events from start_date to end_date
#  2. Fixtures are saved in the sub directory fixture/all.
#  3. Fixtures are arranged in date.txt.
#  4. Each date.txt contains the info of each event on that date.
#  5. The events in that date in json format is saved in a sub directory
#     with the date as the sub dir name.
#  6. Before downloading the fixture events, the program deletes the date.txt and all events of that date, if date
#       is between start_date and end_date.
#  7. Downloads events based on event id from event_start_date to event_end_date
#  8. Event id is extracted from the event id in the fixture sub directory.
#  9. The events in json format is saved in a sub directory events. id.txt is the name of the file
#     with the date as the sub dir name.
#  10. This program only downloads the events between start_date and end_date.
#  11. The program creates an "events_index.txt" file that lists all the events that have been downloaded.
#

league='all'  #process all leagues
startDateStr = dBStartDateStr 
endDateStr = dBEndDateStr 
if bImport:
    (nMatchDates,nTotFixtures,newFixtures) = importFixtures(directory,league,league_list, importDates)
    tmpEventList = []
    for tmpFixture in newFixtures:
        tmpEventList.append(tmpFixture["id"])
    print("number of fixtures imported:", len(tmpEventList))
    #print(tmpEventList)
    #
    # query database today's matches
    #   mysqlImportList -- List of events of today's matches
    #       tmpEvent['id'] = tmpEventId
    #       tmpEvent['matchYear'] = tmpYear
    #       tmpEvent['matchDateTime'] = tmpDateTime
    #       tmpEvent['matchDate'] = tmpDate
    #       tmpEvent['seasonId'] = tmpSeasonId
    #       tmpEvent['leagueId'] = tmpLeagueId
    #       tmpEvent['midSizeName'] = tmpMidSizeLeagueName
    #       tmpEvent['homeTeamId'] = tmpHomeTeamId
    #       tmpEvent['awayTeamId'] = tmpAwayTeamId
    #       tmpEvent['matchStatus'] = tmpStatus
    #   mysqlEventList      -- List of eventIds
    #   mysqlDateList       -- List of Dates in US EST
    #   mysqlSeasonList     -- List of SeasonTypes
    #   mysqlLeagueList     -- List of LeagueIds
    #   mysqlMidSizeNameList-- List of league midsizenames
    #   mysqlTeamList       -- List of teams
    #
    # (mysqlImportDict,err) = todaysMatch(mysqlDict)
    (mysqlImportDict, err) = todaysMatchInDB(mysqlDict,tmpEventList)
    mysqlEventList = []
    mysqlDateList = []
    mysqlSeasonList = []
    mysqlLeagueList = []
    mysqlMidSizeNameList = []
    mysqlTeamList = []
    if err == 0:
        for tmpEventId in mysqlImportDict:
            tmpEvent = mysqlImportDict[tmpEventId]
            tmpYear = tmpEvent['matchYear']
            tmpDateTime = tmpEvent['matchDateTime']
            tmpDate = tmpEvent['matchDate']
            tmpSeasonId = tmpEvent['seasonId']
            tmpLeagueId = tmpEvent['leagueId']
            tmpMidSizeLeagueName = tmpEvent['midsizeName']
            tmpHomeTeamId = tmpEvent['homeTeamId']
            tmpAwayTeamId = tmpEvent['awayTeamId']
            tmpStatus = tmpEvent['matchStatus']
            if tmpEventId not in mysqlEventList:
                mysqlEventList.append(tmpEventId)
            if tmpDate not in mysqlDateList:
                mysqlDateList.append(tmpDate)
            if tmpSeasonId not in mysqlSeasonList:
                mysqlSeasonList.append(tmpSeasonId)
            if tmpLeagueId not in mysqlLeagueList:
                mysqlLeagueList.append(tmpLeagueId)
            if tmpMidSizeLeagueName not in mysqlMidSizeNameList:
                mysqlMidSizeNameList.append(tmpMidSizeLeagueName)
            if tmpHomeTeamId not in mysqlTeamList:
                mysqlTeamList.append(tmpHomeTeamId)
            if tmpAwayTeamId not in mysqlTeamList:
                mysqlTeamList.append(tmpAwayTeamId)
            #tmpEvent['id'] = tmpEventId
            #tmpEvent['matchYear'] = tmpYear
            #tmpEvent['matchDateTime'] = tmpDateTime
            #tmpEvent['matchDate'] = tmpDate
            #tmpEvent['seasonId'] = tmpSeasonId
            #tmpEvent['leagueId'] = tmpLeagueId
            #tmpEvent['midSizeName'] = tmpMidSizeLeagueName
            #tmpEvent['homeTeamId'] = tmpHomeTeamId
            #tmpEvent['awayTeamId'] = tmpAwayTeamId
            #tmpEvent['matchStatus'] = tmpStatus
        # print(mysqlImportDict)
        startDateObj = mysqlDateList[0]
        startDateStr = startDateObj.strftime("%Y%m%d")
        endDateObj = mysqlDateList[-1]
        endDateStr = endDateObj.strftime("%Y%m%d")
        print("first match date in US_EASTERN:", startDateStr)
        print("last  match date in US_EASTERN:", endDateStr)
        print('total events in database:    ', len(mysqlEventList))
        print('total season ids in database: ', len(mysqlSeasonList))
        print('total league ids in database: ', len(mysqlLeagueList))
        print('total leagueNames in database:', len(mysqlMidSizeNameList))
        print('total teams in database:      ', len(mysqlTeamList))
    print()
    print("compare imported fixtures with database.  downloaded new and changed fixtures.")
    (importedEventIds,importedTeamIds,importedLeagues,msg) = importEvents(bCompare, bSlow,
                                                                          newFixtures,
                                                                          directory,
                                                                          dirSnapshots,
                                                                          mysqlImportDict,
                                                                          mysqlDateList)
    print(msg)
    nEvents = len(importedEventIds)
    #
    # check Events file integrity.
    # delete redundant event files
    # download events with errors
    #
    print ("check event file integrity...")
    msg,failedImport = checkEvents(directory)
    print(msg)
    print("No failed Imports",len(failedImport),failedImport)
    print("No of ImportedEventIds (including failed imports):", len(importedEventIds))

    for tmpId in failedImport:
        failedImportEventId = int(tmpId)
        if failedImportEventId in importedEventIds:
            importedEventIds.remove(failedImportEventId)
            print('removed',failedImportEventId, "from importedEventIds")
        i = 0
        iPop = -1
        for importedLeaguesItem in importedLeagues:
            tmpEventId = importedLeaguesItem['eventId']
            if tmpEventId == failedImportEventId:
                iPop = i
                break
            i += 1
        if iPop >= 0:
            importedLeagues.pop(iPop)
            print('removed', failedImportEventId, "from importedLeagues")
    #
    filename = dirTmp + "importedEventIds.json"
    with open(filename, 'w') as file:
        json.dump(importedEventIds, file)
    file.close()
    print(filename)

    filename = dirTmp + "importedTeamIds.json"
    with open(filename, 'w') as file:
        json.dump(importedTeamIds, file)
    file.close()
    print(filename)

    filename = dirTmp + "importedLeagues.json"
    with open(filename, 'w') as file:
        json.dump(importedLeagues, file)
    file.close()
    print(filename)

    filename = directory + 'failedImport.txt'
    with open(filename, 'w') as file:
        json.dump(failedImport, file)
    file.close()
    print(filename)
    #
    # Import Standdings
    #
    print ("import standings...")
    (nImportLeagues, teamsInLeagues, msg)=importStandings(directory,importedLeagues)
    print(msg)

    nLeagues = len(teamsInLeagues)

    teamLogoDir = rootDir + 'team_logo/'

    filename = dirTmp + "teamsInLeagues.json"
    with open(filename, 'w') as file:
        json.dump(teamsInLeagues, file)
    file.close()
    print(filename)

    msg=importRoster(directory,teamsInLeagues)
    print(msg)
else:
    nMatchDates = 0
    nTotFixtures = 0
    nImportLeagues = 0
    nLeagues = 0
    nEvents = 0

leagueLogoDir = rootDir + 'league_logo/'
teamLogoDir = rootDir + 'team_logo/'
#
# Download team logos
#
filename = dirTmp + "teamsInLeagues.json"
with open(filename, 'r') as file:
    Response = json.load(file)
file.close()
teamsInLeagues = Response

filename = dirTmp + "importedTeamIds.json"
with open(filename, 'r') as file:
    Response = json.load(file)
file.close()
importedTeamIds = Response

filename = dirTmp + "importedLeagues.json"
with open(filename, 'r') as file:
    Response = json.load(file)
file.close()
importedLeagues = Response

importedLeagueList = []
for tmpLeague in importedLeagues:
    leagueId = int(tmpLeague['leagueId'])
    if leagueId not in importedLeagueList:
        importedLeagueList.append(leagueId)
#print(importedLeagueList)

teamList = {}
i = 0
for tmpSeasonType in teamsInLeagues:
    teams = teamsInLeagues[tmpSeasonType]
    for team in teams:
        tmpTeamId = int(team['teamId'])
        tmpTeamName = team['name']
        logoUrl1 = team['logoUrl1']
        logoUrl2 = team['logoUrl2']
        if tmpTeamId in importedTeamIds and tmpTeamId not in teamList.keys():
            i += 1
            tmpTeam={"id": tmpTeamId, "name": tmpTeamName,
                       "logoUrl1": logoUrl1,"logoUrl2": logoUrl2}
            teamList[tmpTeamId] = tmpTeam

#print(teamList)
sortedTeamList = dict(sorted(teamList.items()))

#print(sortedTeamList)
#print(len(sortedTeamList))
msg,nLogo = downloadLogos(teamLogoDir,sortedTeamList)
print(msg)
print("number of teams with new logos:", nLogo)

filename = rootDir + "leagueList.txt"
with open(filename, 'r') as file:
    Response = json.load(file)
file.close()
leagues = Response

leagueList = {}
i = 0
for n in leagues:
    league = leagues[n]
    leagueId = int(league['id'])
    #print(leagueId, league)
    if int(leagueId) in importedLeagueList and int(leagueId) not in leagueList.keys():
        # print(i, team)
        tmpLeagueId = int(leagueId)
        tmpLeagueName = league['name']
        tmpMidSizeLeagueName = league['midsizeName']
        if 'logos' in league.keys():
            logoUrl1 = ""
            logoUrl2 = ""
            i += 1
            if len(league['logos']) >=2:
                logoUrl2 = league['logos'][1]['href']
            if len(league['logos']) >=1:
                logoUrl1 = league['logos'][0]['href']
            tmpLeague={"id": tmpLeagueId, "name": tmpLeagueName,
                       "logoUrl1": logoUrl1,"logoUrl2": logoUrl2}
            #print(i, tmpLeague)
            leagueList[leagueId] = tmpLeague

sortedLeagueList = dict(sorted(leagueList.items()))

#print(sortedLeagueList)
print(len(sortedLeagueList))
msg = downloadLogos(leagueLogoDir,sortedLeagueList)
print(msg)

#msg = extractESPNData01.extractFixtures1(rootDir,rootDir2,importedLeagues,importLeagueFilter,startDateStr,endDateStr)
dataSet,errLog,msg = extractESPNData01.extractESPNData(rootDir,rootDir2,
                                                       importedLeagues,importLeagueFilter,
                                                       startDateStr,endDateStr)
print(msg)

nStart=1
nEnd=18
msg = sql_insert_all.Install_All(mysqlDict,dataSet,rootDir,rootDir2,nStart,nEnd)
print(msg)
timeObj2 = datetime.now(ZoneInfo("America/New_York"))
elapsedTime = timeObj2 - timeObj1

print()
print("import start date:                 ", start_date.strftime("%Y-%m-%d"))
print("import end date:                   ", end_date.strftime("%Y-%m-%d"))
print("number of match dates:             ", nMatchDates)
print("number of total events:            ", nTotFixtures)
print("number of new or changed events:   ", nEvents)
print("number of leagues with standings:  ", nImportLeagues)
print("number of leagues with team roster:", nLeagues)
print("number of new team logos:          ", nLogo)
print()
print("import start time:         ", timeObj1.strftime("%Y-%m-%d %H:%M:%S"))
print("import end time:           ", timeObj2.strftime("%Y-%m-%d %H:%M:%S"))
print("elapsed time:              ", elapsedTime)
