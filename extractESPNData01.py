import json
import os
import csv
import datetime
from datetime import timezone, datetime, timedelta

import ESPNSoccer


def openIndexFile(indexfilename, leagueList):
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    k = 0
    with open(indexfilename, 'r', encoding='utf8') as csv_file:
        indexReader = csv.reader(csv_file, delimiter=',')
        events = {}
        log = {}
        for row in indexReader:
            k += 1
            no = row[0]
            league = row[1]
            year = row[2]
            updateTime = row[3]
            id = row[4]
            name = row[5]
            homeTeam = name.split(' at ')[1]
            awayTeam = name.split(' at ')[0]
            venue = row[6]
            status = row[7]
            if len(row) == 9:
                score = row[8]
                homeScore = score.split(':')[0]
                awayScore = score.split(':')[0]
            else:
                homeScore = 'none'
                awayScore = 'none'
            if id not in events:
                if league in leagueList or len(leagueList) == 0:
                    events[id] = {'league': league,
                                  'year': year,
                                  'updateTime': updateTime,
                                  'homeTeam': homeTeam,
                                  'awayTeam': awayTeam,
                                  'venue': venue,
                                  'status': status,
                                  'homeScore': homeScore,
                                  'awayScore': awayScore}
                else:
                    log[id] = {'status': 'not selected', 'logTime': currentTime}
            else:
                print(id, 'duplicate', events[id])
                print(name, status, score)
                log[id] = {'status': 'duplicate', 'logTime': currentTime}
    return (events, log)


def open_league_list(dir):
    filename = dir + 'leagueList.txt'
    err = 0
    #print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            leagueList = json.load(file)
        file.close()
    else:
        print("file", filename, "not found")
        err = -1
        leagueList = []
    return (leagueList, err)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def readUid(uidStr):
    tmpId = uidStr.split("~")
    uid = {}
    for item in tmpId:
        key = item.split(":")[0]
        value = item.split(":")[1]
        uid[key] = value
    return (uid)


def openFixture(id, saveFileName, fixtureDir):
    err = 0
    errEvent = {}
    dateStr = saveFileName.split(".")[0]
    dateDir = fixtureDir + dateStr + '/'
    filenamefull1 = dateDir + str(id) + ".txt"
    #print(id, filenamefull1)
    if os.path.isfile(filenamefull1):
        with open(filenamefull1, 'r') as file:
            Response = json.load(file)
        file.close()
        fixture = Response
    else:
        fixture = {}
        err = -1
        errEvent = {'id': id, 'event in fixtures dir': 'not found'}
    return err, fixture, errEvent


def openEvent(id, openDir,eventImportDir):
    filename2 = openDir + str(id) + '.txt'
    errEvent = {}
    if os.path.exists(filename2):
        with open(filename2, 'r') as file:
            Response = json.load(file)
        file.close()
        hasEvents = True
        event = Response
    else:
        print(filename2)
        print(id, 'not found.')
        (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
        if code == 0:
            hasEvents = True
        else:
            errEvent = {'id': id, 'event file': 'event not found'}
            hasEvents = False

    if 'error' in event.keys():
        (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
        if code == 0:
            hasEvents = True
        else:
            print("error in event.","Re-import code", code, hasEvents)
            errEvent = {'id': id, 'event file': 'error in event'}
    elif 'code' in event.keys():
        oldCode = Response['code']
        if oldCode != '0':
            (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
            if code == 0:
                hasEvents = True
            else:
                errEvent = {'id': id, 'event file': 'event not found'}
                print("error in event. error code:", oldCode, "Re-import code", code, hasEvents)
    return hasEvents, event,errEvent
def openTeamRoster(typeId,midsizeName,openDir):
    err = 0
    errEvent = {}
    filename = midsizeName + "_" + str(typeId) + ".txt"
    filename = openDir + filename
    print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            Response = json.load(file)
        file.close()
        team = Response
    else:
        team ={}
        err = -1
        errEvent={"typeId":typeId,"misizeName":midsizeName,"teamRoster":"no team roster"}
    return err,team, errEvent
def openStandings(typeId,year,midsizeName,openDir):
    err = 0
    errEvent = {}
    filename = midsizeName + "_" + str(year) + "_" + str(typeId) + "_table.txt"
    filename = openDir + filename
    print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            Response = json.load(file)
        file.close()
        standings = Response
    else:
        print("standings: file does not exist:",filename)
        standings ={}
        err = -1
        errEvent={"typeId":typeId,"misizeName":midsizeName,"standings":"file does not exist"}
    return err,standings, errEvent
def extractPlay(id, nOrder, play, teamDict):
    tmpPlay = {}
    tmpPlayType = {}
    tmpPlaySource = {}
    nParticipants = 0
    tmpPlayParticipants = []
    tmpPlay['eventId'] = id
    tmpPlay['order'] = nOrder
    for key in play:
        if key == 'id':
            playId = play[key]
            tmpPlay[key] = playId
        elif key == 'type':
            tmpPlayType = play[key]
            if 'id' in tmpPlayType.keys():
                playTypeId = tmpPlayType['id']
                tmpPlay['typeId'] = playTypeId
            #if 'text' in keyEvent[key].keys():
            #    playTypeText = tmpPlayType['text']
            #tmpPlay['typeText'] = playTypeText
            #     if keyEventTypeId not in keyEventTypeDB.keys():
            #         keyEventTypeDB[keyEventTypeId] = {'id': keyEventTypeId,
            #                                           'text': keyEventTypeText}
        elif key == 'text':
            playText = play[key]
            tmpPlay['text'] = playText
        elif key == 'shortText':
            playShortText = play[key]
            tmpPlay['shortText'] = playShortText
        elif key == 'period':
            playPeriod = play[key]['number']
            tmpPlay['period'] = playPeriod
        elif key == 'clock':
            playClock = play[key]
            if playClock['value'] == '':
                tmpPlay['clockValue'] = 0
            else:
                tmpPlay['clockValue'] = playClock['value']
            tmpPlay['clockDisplayValue'] = playClock['displayValue']
        elif key == 'team':
            playTeam = play[key]
            if 'id' in playTeam.keys():
                tmpPlay['teamId'] = playTeam['id']
            elif 'displayName' in playTeam.keys():
                tmpPlay['teamDisplayName'] = playTeam['displayName']
                if playTeam['displayName'] == 'Crystal Palace U21' and playTeam['displayName'] not in teamDict:
                    teamDict[playTeam['displayName']] = '21033'
                    print('Crystal Palace U21', id)
                if playTeam['displayName'] == 'Ole Miss' and playTeam['displayName'] not in teamDict:
                    teamDict[playTeam['displayName']] = '20509'
                    print('Ole Miss', id)
                if playTeam['displayName'] == 'Notre Dame Fighting Irish' and playTeam['displayName'] not in teamDict:
                    teamDict[playTeam['displayName']] = '5604'
                    print('Notre Dame Fighting Irish', id)
                if playTeam['displayName'] == 'LSU' and playTeam['displayName'] not in teamDict:
                    teamDict[playTeam['displayName']] = '20536'
                    print('LSU', id)
                if playTeam['displayName'] not in teamDict:
                    teamDict[playTeam['displayName']] = ''
                    print(playTeam['displayName'], id)
                tmpPlay['teamId'] = teamDict[playTeam['displayName']]
            else:
                tmpPlay['teamDisplayName'] = ''
                tmpPlay['teamId'] = ''
            if 'displayName' in playTeam.keys():
                tmpPlay['teamDisplayName'] = playTeam['displayName']
            else:
                tmpPlay['teamDisplayName'] = ''
        elif key == 'participants':
            playParticipants = play[key]
            nParticipants = len(playParticipants)
        elif key == 'source':
            playSource = play[key]
            if 'id' in playSource.keys():
                sourceId = playSource['id']
            else:
                sourceId = 'none'
            tmpPlay['sourceId'] = sourceId
            if 'description' in playSource.keys():
                sourceDescription = playSource['description']
            else:
                sourceDescription = 'none'
            if sourceId not in tmpPlaySource:
                tmpPlaySource[sourceId] = sourceDescription
        elif key == 'shootout':
            playShootout = play[key]
            tmpPlay['shootout'] = playShootout
        elif key == 'scoringPlay':
            scoringPlay = play[key]
            tmpPlay['scoringPlay'] = scoringPlay
        else:
            tmpPlay[key] = play[key]

    if nParticipants >= 1:
        nP = 0
        for participant in playParticipants:
            nP += 1
            tmpPlayParticipant = {}
            tmpPlayParticipant['eventId'] = id
            tmpPlayParticipant['id'] = playId
            tmpPlayParticipant['order'] = nP
            # for key in tmpKeyEvent:
            #    tmpKeyEventParticipants[key]=tmpKeyEvent[key]
            displayName = participant['athlete']['displayName']
            tmpPlayParticipant['participant'] = displayName
            # print(displayName,tmpKeyEventParticipants)
            tmpPlayParticipants.append(tmpPlayParticipant)

    if not tmpPlayType:
        tmpPlayType['id'] = 9999
        tmpPlayType['text'] = 'unknown'

    if 'scoringPlay' not in tmpPlay.keys():
        tmpPlay['scoringPlay'] = ''
    if 'shootout' not in tmpPlay.keys():
        tmpPlay['shootout'] = ''

    return (playId, tmpPlay, tmpPlayType, tmpPlayParticipants, tmpPlaySource)


def writeOddsDB(provider, oddsDB, bettingOddsDB, playerOddsDB, saveDir):
    filename1 = saveDir + 'oddsProvider.json'
    filename2 = saveDir + 'odds.json'
    filename3 = saveDir + 'bettingOdds.json'
    filename4 = saveDir + 'playerOdds.json'
    err = 0
    if os.path.exists(saveDir):
        with open(filename1, 'w') as file:
            json.dump(provider, file)
        file.close()
        print("odds provider", filename1)

        with open(filename2, 'w') as file:
            json.dump(oddsDB, file)
        file.close()
        print("odds         ", filename2)

        with open(filename3, 'w') as file:
            json.dump(bettingOddsDB, file)
        file.close()
        print("betting odds ", filename3)

        with open(filename4, 'w') as file:
            json.dump(playerOddsDB, file)
        file.close()
        print("player odds  ", filename4)
    else:
        err = -1
        print('error', saveDir)
    return (err)


def readOddsDB(saveDir):
    filename1 = saveDir + 'oddsProvider.json'
    filename2 = saveDir + 'odds.json'
    err = 0
    if os.path.exists(saveDir):
        with open(filename1, 'r') as file:
            Response = json.load(file)
        file.close()
        oddsProvider = Response
    else:
        oddsProvider = []
        err = 1
    if os.path.exists(saveDir):
        with open(filename2, 'r') as file:
            Response = json.load(file)
        file.close()
        odds = Response
    else:
        odds = []
        err = 1
    return (oddsProvider, odds, err)


def oddsValue(odds, key, suffix):
    if key in odds:
        value = odds[key]
    else:
        value = ''
    if suffix:
        outKey = suffix + key[0].upper() + key[1:]
    else:
        outKey = key
    return (value, outKey)
def clockDisplay2Value(displayValue):
    if "+" in displayValue:
        gameTime=int(displayValue.rsplit("+")[0].rstrip("'"))
        extraTime=int(displayValue.rsplit("+")[1].rstrip("'"))
        clockValue = gameTime+extraTime
    elif "'" in displayValue:
        clockValue=int(displayValue.rstrip("'"))
    else:
        print(displayValue)
        clockValue = 0
    return clockValue
def extractFixtures1(dir1, dir2, importedLeagues, importLeagueFilter, start_date_Str, end_date_Str):
    #dir1 = rootDir
    #dir2 = rootDir2
    eventDir = dir1 + 'events/'
    saveDirFixture = dir2 + "tables/fixtures/"
    saveDirTable = dir2 + "tables/"
    saveDirBoxscore = dir2 + "tables/boxscore/"
    saveDirGameInfo = dir2 + "tables/gameInfo/"
    saveDirKeyEvents = dir2 + "tables/keyEvents/"
    saveDirOdds = dir2 + "tables/odds/"

    saveDirErrLog = dir2 + "tables/"

    #eventImportDir = directory.rstrip(directory[-1])
    eventImportDir = dir1
    #indexfilename= rootDir +'events_index.csv'

    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    today = datetime.today()
    todayStr = datetime.strftime(today, "%Y%m%d")

    #start_date = datetime.strptime(start_date_Str,"%Y-%m-%d").date()
    #end_date = datetime.strptime(end_date_Str,"%Y-%m-%d").date()
    # start_date = datetime.strptime(dBStartDateStr, "%Y-%m-%d").date()
    # end_date = datetime.strptime(dBEndDateStr, "%Y-%m-%d").date()
    '''
    29745,ENG.1,2021,2022-05-22T15:00Z,605668,Wolverhampton Wanderers at Liverpool,Anfield,STATUS_FULL_TIME,3:1
    29746,ENG.1,2021,2022-05-22T15:00Z,605659,Aston Villa at Manchester City,Etihad Stadium,STATUS_FULL_TIME,3:2
    29747,ENG.1,2021,2022-05-22T15:00Z,605665,Tottenham Hotspur at Norwich City,Carrow Road,STATUS_FULL_TIME,0:5
    29748,ESP.1,2021,2022-05-22T15:30Z,610395,Getafe at Elche,Martínez Valero,STATUS_FULL_TIME,3:1
    29749,ESP.1,2021,2022-05-22T18:00Z,610392,Cádiz at Alavés,Mendizorroza,STATUS_FULL_TIME,0:1
    '''

    #start_date_Str = datetime.strftime(start_date,"%Y%m%d")
    #end_date_Str = datetime.strftime(end_date,"%Y%m%d")
    statusStr = "started"

    dataSet = {"startDate": start_date_Str,
               "endDate": end_date_Str,
               "extractionDate": todayStr,
               "status": statusStr}

    print(todayStr)
    print(start_date_Str, end_date_Str)
    print(dataSet)

    filename = saveDirTable + 'dataSet.json'
    print('dataSet', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(dataSet, file)
        file.close()
    #
    # Open leaague list and generate leagueDB
    #
    (leagueListFromFile, err) = open_league_list(dir1)
    leagueIndex = {}
    leagueDB1 = []
    for i in leagueListFromFile:
        tmpLeague = {}
        if 'id' in leagueListFromFile[i]:
            tmpLeague['id'] = leagueListFromFile[i]['id']
        else:
            tmpLeague['id'] = 'none'
        if 'alternateId' in leagueListFromFile[i]:
            tmpLeague['alternateId'] = leagueListFromFile[i]['alternateId']
        else:
            tmpLeague['alternateId'] = 'none'
        if 'name' in leagueListFromFile[i]:
            tmpLeague['name'] = leagueListFromFile[i]['name']
        else:
            tmpLeague['name'] = 'none'
        if 'abbreviation' in leagueListFromFile[i]:
            tmpLeague['abbreviation'] = leagueListFromFile[i]['abbreviation']
        else:
            tmpLeague['abbreviation'] = 'none'
        if 'shortName' in leagueListFromFile[i]:
            tmpLeague['shortName'] = leagueListFromFile[i]['shortName']
        else:
            tmpLeague['shortName'] = 'none'
        if 'midsizeName' in leagueListFromFile[i]:
            tmpLeague['midsizeName'] = leagueListFromFile[i]['midsizeName']
            midsizeName = leagueListFromFile[i]['midsizeName']
        else:
            tmpLeague['midsizeName'] = 'none'
            midsizeName = 'none'
        if 'slug' in leagueListFromFile[i]:
            tmpLeague['slug'] = leagueListFromFile[i]['slug']
        else:
            tmpLeague['slug'] = 'none'
        if 'season' in leagueListFromFile[i]:
            if 'type' in leagueListFromFile[i]['season']:
                tmpLeague['seasonTypeId'] = leagueListFromFile[i]['season']['type']['id']
                tmpLeague['seasonHasStandings'] = leagueListFromFile[i]['season']['type']['hasStandings']
            else:
                tmpLeague['seasonTypeId'] = 0
                tmpLeague['seasonHasStandings'] = False
        else:
            tmpLeague['seasonTypeId'] = 0
            tmpLeague['seasonHasStandings'] = False
        if "logos" in leagueListFromFile[i]:
            try:
                logo1 = leagueListFromFile[i]['logos'][0]["href"]
                logo1LastUpdated = leagueListFromFile[i]['logos'][0]["lastUpdated"]
                # print(logo1, logo1LastUpdated)
            except:
                logo1 = ""
                logo1LastUpdated = ""
            if len(leagueListFromFile[i]['logos']) == 2:
                try:
                    logo2 = leagueListFromFile[i]['logos'][1]["href"]
                    logo2LastUpdated = leagueListFromFile[i]['logos'][1]["lastUpdated"]
                    # print(logo2, logo2LastUpdated)
                except:
                    logo2 = ""
                    logo2LastUpdated = ""
            else:
                logo2 = ""
                logo2LastUpdated = ""
        else:
            logo1 = ""
            logo1LastUpdated = ""
            logo2 = ""
            logo2LastUpdated = ""

        tmpLeague["logoUrl1"] = logo1
        tmpLeague["logoUrl1LastUpdated"] = logo1LastUpdated
        tmpLeague["logoUrl2"] = logo2
        tmpLeague["logoUrl2LastUpdated"] = logo2LastUpdated

        if 'hasStandings' in leagueListFromFile[i]:
            tmpLeague['hasStandings'] = leagueListFromFile[i]['hasStandings']
        else:
            tmpLeague['hasStandings'] = 'none'
        if 'updateTime' in leagueListFromFile[i]:
            tmpLeague['updateTime'] = leagueListFromFile[i]['updateTime']
        else:
            tmpLeague['updateTime'] = 'none'
        leagueIndex[tmpLeague['id']] = midsizeName
        leagueDB1.append(tmpLeague)

    #print(leagueDB1)
    #print(leagueIndex)

    #
    # open a list of fixtures from last import
    #

    #allFixturesFileName = dir1 + 'imported_fixtures.json'
    #with open(allFixturesFileName,'r') as file:
    #    Response = json.load(file)
    #file.close()

    #allFixtures={}
    #for tmpResponse in Response:
    #   id = tmpResponse['id']
    #   allFixtures[id] = tmpResponse

    #
    # open fixtures by date.
    # read in a list of events on each date
    # find and open each event in event directory.  re-import event if there's error in event
    # unpack events
    #

    fixture = {}
    teamsDict = {}
    teamList = {}
    teamDB = {}
    fixtures = []
    statusType = {}
    season = []
    stats = []
    details = []
    detailType = {}
    importEventIndex = []
    statLabel = {}
    venueDBFixture = {}

    leagueDB = {}
    leagueList = {}
    seasonDB = {}
    teamDB = {}
    teamUniformDB = {}
    stats = []
    statLabel = {}

    venueDBGameInfo = {}
    venueEvent = {}
    officialDB = {}
    attendanceDB = {}

    keyEventDB = {}
    commentaryDB = []
    playDB = {}
    keyEventTypeDB = {}
    keyEventSourceDB = {}
    keyEventParticipantsDB = {}
    playParticipantsDB = {}

    oddsProviderList = []
    oddsProviderDB = []
    oddsDB = []
    bettingOddsDB = []
    playerOddsDB = []
    nPlayerOdds = 0

    bUpdateOdds = False

    errLog = []

    delim = ','  # change delim to ','
    k = 0
    i = 0
    importLogFileName = saveDirFixture + 'events.log'
    logFile = open(importLogFileName, 'w')
    badEvents = []
    for tmpLeague in importedLeagues:
        tmpFixture = {}
        id = tmpLeague["eventId"]

        keyEventDB[id] = {}
        playDB[id] = {}
        keyEventParticipantsDB[id] = {}
        playParticipantsDB[id] = {}

        matchDate = tmpLeague['date']
        leagueId = tmpLeague["leagueId"]
        midsizeLeagueName = tmpLeague["midsizeName"]
        saveFileName = tmpLeague['fileName']
        #print(i,id,fixture['date'],len(list(fixture.keys())),list(fixture.keys()))
        #uid = readUid(fixture['uid'])
        #matchDate = fixture['date']
        #name = fixture['name']
        #dateStr = datetime.strptime(matchDate,"%Y-%m-%dT%H:%MZ").strftime("%Y%m%d")
        dateStr = saveFileName.split(".")[0]
        dateDir = dir1 + 'fixture/all/' + dateStr + '/'
        filenamefull1 = dateDir + str(id) + ".txt"
        if os.path.isfile(filenamefull1):
            with open(filenamefull1, 'r') as file:
                Response = json.load(file)
            file.close()
            fixture = Response
        else:
            badEvents.append(tmpLeague)
            print("file", filenamefull1, "not found")
            errEvent = {'id': id, 'event in fixtures dir': 'not found'}
            errLog.append(errEvent)
            continue
            #return("error")
        #filenamefull2 = eventDir + '/' + str(id) + ".txt"
        #print(filenamefull2)
        #if os.path.isfile(filenamefull2):
        #    with open(filenamefull2, 'r') as file:
        #        Response = json.load(file)
        #    file.close()
        #    bFile = True
        #else:
        #    bFile = False
        #print(Response)
        (bFile, event) = openEvent(id, eventDir)
        if 'error' in event.keys():
            k += 1
            (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
            line = str(i) + delim + str(id) + delim + today.strftime("%Y%m%d") + delim + str(code) + ' error' + '\n'
            logFile.write(line)
            line1 = 'import error re-import ' + str(id) + ' ' + str(k) + delim + str(code)
            if code == 0:
                bFile = True
            print(bFile, line1)
        elif not bFile:
            k += 1
            (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
            line = str(i) + delim + str(id) + delim + today.strftime("%Y%m%d") + delim + str(code) + ' nofile' + '\n'
            logFile.write(line)
            line1 = 'no file found re-import ' + str(id) + ' ' + str(k) + delim + str(code)
            if code == 0:
                bFile = True
            print(bFile, line1)
        elif 'code' in event.keys():
            oldCode = Response['code']
            if oldCode != '0':
                k += 1
                (event, code) = ESPNSoccer.import_event(id, eventImportDir, False)
                line = str(i) + delim + str(id) + delim + today.strftime("%Y%m%d") + delim + str(code) + \
                       delim + str(oldCode) + '\n'
                logFile.write(line)
                line1 = 'bad code re-import ' + str(id) + ' ' + str(k) + delim + str(code)
                if code == 0:
                    bFile = True
                print(bFile, line1)

        print(i, id, bFile, list(event.keys()))

        if not bFile:
            print("id", id, "event file not found")
            errEvent = {'id': id, 'event file': 'not found'}
            errLog.append(errEvent)
            continue
        #if 'shortName' in fixture.keys():
        #    shortName = fixture['shortName']
        seasonTmp = fixture['season']
        if seasonTmp not in season:
            season.append(seasonTmp)
        year = seasonTmp['year']
        seasonType = seasonTmp['type']
        seasonSlug = seasonTmp['slug']
        competitions1 = fixture['competitions'][0]
        if 'updateTime' in event:
            updateTime = event['updateTime']
        else:
            #updateTime = allFixtures[id]['updateTime']
            updateTime = currentTime
            #print(updateTime)
        bEvent = False
        header = {}
        competitions2 = {}
        if 'header' in event:
            header = event['header']
            if 'competitions' in header:
                competitions2 = event['header']['competitions'][0]
                bEvent = True
        status = fixture['status']
        if 'league' in header:
            league = header['league']
            if 'id' in league:
                if leagueId not in leagueList.keys():
                    league.pop('links', None)
                    # league['updateTime'] = updateTime
                    # print(k,league)
                    leagueList[leagueId] = league
                    leagueDB[leagueId] = {}
        if 'season' in header:
            seasonTmp2 = header['season']
            seasonYear = seasonTmp2['year']
            typeId = seasonTmp2['type']
            seasonName = seasonTmp2['name']
            if typeId not in seasonDB.keys():
                seasonDB[typeId] = {'year': seasonYear,
                                    'type': typeId,
                                    'name': seasonName,
                                    'slug': seasonSlug}
            if typeId not in leagueDB[leagueId]:
                leagueDB[leagueId][typeId] = []
                #print(leagueId, seasonType, leagueDB[leagueId][seasonType])
        else:
            print("id", id, "header not found in event file")
            errEvent = {'id': id, 'header': 'not found'}
            errLog.append(errEvent)

        #print(competitions2)
        #if 'l' in uid.keys():
        #    leagueId = uid['l']
        #else:
        #    #print(uid)
        #    leagueId = 9999
        #if leagueId in leagueIndex:
        #    midsizeLeagueName=leagueIndex[leagueId]
        #else:
        #    midsizeLeagueName= str(leagueId)
        #importEventIndex.append({'id':id,'updateTime':updateTime})
        #updateTime = allFixtures[id]['updateTime']
        importEventIndex.append({'id': id, 'updateTime': updateTime})
        tmpFixture['eventId'] = id
        tmpFixture['leagueId'] = leagueId
        tmpFixture['uid'] = competitions1['uid']
        #tmpFixture['uid']=uid
        tmpFixture['attendance'] = competitions1['attendance']
        #tmpFixture['midsizeLeagueName']=midsizeLeagueName
        tmpFixture['date'] = competitions1['date']
        tmpFixture['startDate'] = competitions1['startDate']
        if bEvent:
            tmpFixture['neutralSite'] = competitions2['neutralSite']
            if tmpFixture['neutralSite'] == '':
                tmpFixture['neutralSite'] = 'false'
            tmpFixture['conferenceCompetition'] = competitions2['conferenceCompetition']
            tmpFixture['boxscoreAvailable'] = competitions2['boxscoreAvailable']
            tmpFixture['commentaryAvailable'] = competitions2['commentaryAvailable']
            tmpFixture['recent'] = competitions2['recent']
            tmpFixture['boxscoreSource'] = competitions2['boxscoreSource']
            tmpFixture['playByPlaySource'] = competitions2['playByPlaySource']
        #tmpFixture['year']=year
        tmpFixture['seasonType'] = seasonType
        #tmpFixture['seasonSlug']=seasonSlug
        statusTypeId = status['type']['id']
        tmpFixture['statusId'] = statusTypeId
        if statusTypeId not in statusType.keys():
            statusType[statusTypeId] = status['type']
        tmpFixture['clock'] = status['clock']
        tmpFixture['displayClock'] = status['displayClock']
        if 'period' in status:
            tmpFixture['period'] = status['period']
        else:
            tmpFixture['period'] = 'unknown'
        if 'venue' in competitions1.keys():
            venueTmp = competitions1['venue']
            venueId = venueTmp['id']
            venueFullName = venueTmp['fullName']
            if 'address' in venueTmp.keys():
                if 'city' in venueTmp['address'].keys():
                    venueCity = venueTmp['address']['city']
                else:
                    venueCity = None
            else:
                venueCity = None
            if venueId not in venueDBFixture.keys():
                venueDBFixture[venueId] = {'id': venueId,
                                           'fullName': venueFullName,
                                           'city': venueCity}
            tmpFixture['venueId'] = venueId
        competitors1 = competitions1['competitors']
        for competitor1 in competitors1:
            #print(list(competitor.keys()))
            homeAway = competitor1['homeAway']
            if homeAway == 'home':
                homeTeam = competitor1['team']
                if 'venue' in homeTeam:
                    venueId = homeTeam['venue']['id']
                    homeTeam.pop('venue')
                    homeTeam['venueId'] = venueId
                else:
                    homeTeam['venueId'] = None
                homeTeam.pop('links')
                homeTeam['updateTime'] = updateTime

                homeTeamId1 = competitor1['id']
                homeTeamId2 = competitor1['team']['id']
                homeTeamUid = competitor1['uid']
                homeTeamOrder = competitor1['order']
                homeTeamWinner = competitor1['winner']
                homeTeamScore = competitor1['score']
                if homeTeamId2 not in teamDB.keys():
                    teamDB[homeTeamId2] = homeTeam
                elif homeTeam['venueId'] != None and teamDB[homeTeamId2]['venueId'] == None:
                    teamDB[homeTeamId2] = homeTeam
                if 'shootoutScore' in competitor1.keys():
                    homeTeamShootoutScore = competitor1['shootoutScore']
                else:
                    homeTeamShootoutScore = ''
                homeTeamId = homeTeamId1
                tmpFixture['homeTeamId'] = homeTeamId
                tmpFixture['homeTeamUid'] = homeTeamUid
                tmpFixture['homeTeamOrder'] = homeTeamOrder
                tmpFixture['homeTeamWinner'] = homeTeamWinner
                tmpFixture['homeTeamScore'] = homeTeamScore
                tmpFixture['homeTeamShootoutScore'] = homeTeamShootoutScore
                if 'form' in competitor1:
                    homeTeamForm = competitor1['form']
                    tmpFixture['homeTeamForm'] = homeTeamForm
                if 'statistics' in competitor1:
                    homeTeamStats = competitor1['statistics']
                    if len(homeTeamStats) > 0:
                        tmpStats = {}
                        tmpStats['eventId'] = id
                        tmpStats['teamId'] = homeTeamId
                        tmpStats['homeAway'] = homeAway
                        tmpStats['statusId'] = tmpFixture['statusId']
                        for stat in homeTeamStats:
                            statName = stat['name']
                            statAbv = stat['abbreviation']
                            statDisplayValue = stat['displayValue']
                            tmpStats[statName] = statDisplayValue
                            if statName not in statLabel.keys():
                                statLabel[statName] = statAbv
                        tmpStats['updateTime'] = updateTime
                        stats.append(tmpStats)
                        bHomeTeamStats = True
                    else:
                        bHomeTeamStats = False
                else:
                    bHomeTeamStats = False
                #print(homeAway,homeTeam['displayName'], competitor['id'], competitor['uid'], competitor['order'],
                #      competitor['winner'],competitor['score'],competitor['form'])
            if homeAway == 'away':
                awayTeam = competitor1['team']
                if 'venue' in awayTeam:
                    venueId = awayTeam['venue']['id']
                    awayTeam.pop('venue')
                    awayTeam['venueId'] = venueId
                else:
                    awayTeam['venueId'] = None
                awayTeam.pop('links')
                awayTeam['updateTime'] = updateTime
                awayTeamId = competitor1['team']['id']
                awayTeamId1 = competitor1['id']
                awayTeamId2 = competitor1['team']['id']
                awayTeamUid = competitor1['uid']
                awayTeamOrder = competitor1['order']
                awayTeamWinner = competitor1['winner']
                awayTeamScore = competitor1['score']
                if awayTeamId2 not in teamDB.keys():
                    teamDB[awayTeamId2] = awayTeam
                elif awayTeam['venueId'] != None and teamDB[awayTeamId2]['venueId'] == None:
                    teamDB[awayTeamId2] = awayTeam
                if 'shootoutScore' in competitor1.keys():
                    awayTeamShootoutScore = competitor1['shootoutScore']
                else:
                    awayTeamShootoutScore = ''
                awayTeamId = awayTeamId1
                tmpFixture['awayTeamId'] = awayTeamId
                tmpFixture['awayTeamUid'] = awayTeamUid
                tmpFixture['awayTeamOrder'] = awayTeamOrder
                tmpFixture['awayTeamWinner'] = awayTeamWinner
                tmpFixture['awayTeamScore'] = awayTeamScore
                tmpFixture['awayTeamShootoutScore'] = awayTeamShootoutScore
                if 'form' in competitor1:
                    awayTeamForm = competitor1['form']
                    tmpFixture['awayTeamForm'] = awayTeamForm
                if 'statistics' in competitor1:
                    awayTeamStats = competitor1['statistics']
                    if len(awayTeamStats) > 0:
                        tmpStats = {}
                        tmpStats['eventId'] = id
                        tmpStats['teamId'] = awayTeamId
                        tmpStats['homeAway'] = homeAway
                        tmpStats['statusId'] = tmpFixture['statusId']
                        for stat in awayTeamStats:
                            statName = stat['name']
                            statAbv = stat['abbreviation']
                            statDisplayValue = stat['displayValue']
                            tmpStats[statName] = statDisplayValue
                        tmpStats['updateTime'] = updateTime
                        stats.append(tmpStats)
                        bAwayTeamStats = True
                    else:
                        bAwayTeamStats = False
                else:
                    bAwayTeamStats = False
                tmpFixture['hasStats'] = 'no'
                if bHomeTeamStats and bAwayTeamStats:
                    tmpFixture['hasStats'] = 'yes'

                #print(homeAway,awayTeam['displayName'], competitor['id'],competitor['uid'],competitor['order'],
                #      competitor['winner'],competitor['score'],competitor['form'])
        if 'details' in competitions1.keys():
            nYellowCard = {}
            nYellowCard[homeTeamId] = 0
            nYellowCard[awayTeamId] = 0
            nRedCard = {}
            nRedCard[homeTeamId] = 0
            nRedCard[awayTeamId] = 0
            tmpDetails = competitions1['details']
            nDetails = 0
            for tmpDetail in tmpDetails:
                tmpDetailDict = {}
                tmpDetailDict['eventId'] = id
                tmpDetailDict['order'] = nDetails
                for tmpKey in tmpDetail:
                    if tmpKey == 'type':
                        tmpDetailType = tmpDetail['type']
                        tmpDetailDict['typeId'] = tmpDetailType['id']
                        tmpDetailDict['typeText'] = tmpDetailType['text']
                        if tmpDetailType['id'] not in detailType.keys():
                            detailType[tmpDetailType['id']] = tmpDetailType['text']
                    elif tmpKey == 'clock':
                        tmpDetailDict['clockValue'] = tmpDetail['clock']['value']
                        tmpDetailDict['clockDisplayValue'] = tmpDetail['clock']['displayValue']
                    elif tmpKey == 'team':
                        tmpDetailDict['teamId'] = tmpDetail['team']['id']
                    elif tmpKey == 'athletesInvolved':
                        tmpDetailDict['athletesInvolved'] = tmpDetail['athletesInvolved'][0]['id']
                    else:
                        tmpDetailDict[tmpKey] = tmpDetail[tmpKey]
                if tmpDetailDict['yellowCard'] == True:
                    if tmpDetailDict['teamId'] in nYellowCard.keys():
                        nYellowCard[tmpDetailDict['teamId']] += 1
                    else:
                        nYellowCard[tmpDetailDict['teamId']] = 1
                        print(tmpDetailDict['teamId'], homeTeamId, awayTeamId)
                if tmpDetailDict['redCard'] == True:
                    if tmpDetailDict['teamId'] in nRedCard.keys():
                        nRedCard[tmpDetailDict['teamId']] += 1
                    else:
                        nRedCard[tmpDetailDict['teamId']] = 1
                        print(tmpDetailDict['teamId'], homeTeamId, awayTeamId)
                details.append(tmpDetailDict.copy())
                nDetails += 1
        else:
            print(i, id, 'no details')
            errEvent = {'id': id, 'details': 'not found'}
            errLog.append(errEvent)

        homeYellowCard = 0
        awayYellowCard = 0
        homeRedCard = 0
        awayRedCard = 0
        for teamId in nYellowCard.keys():
            if teamId == homeTeamId:
                homeYellowCard = nYellowCard[teamId]
                homeRedCard = nRedCard[teamId]
            if teamId == awayTeamId:
                awayYellowCard = nYellowCard[teamId]
                awayRedCard = nRedCard[teamId]
        tmpFixture['homeYellowCard'] = homeYellowCard
        tmpFixture['homeRedCard'] = homeRedCard
        tmpFixture['awayYellowCard'] = awayYellowCard
        tmpFixture['awayRedCard'] = awayRedCard
        tmpFixture['updateTime'] = updateTime
        fixtures.append(tmpFixture)

        print(i, id, homeTeam['name'], awayTeam['name'], midsizeLeagueName, leagueId, year, seasonType)

        if (leagueId, seasonType) not in teamsDict.keys():
            teamsDict[leagueId, seasonType] = []
            teamList[leagueId, seasonType] = []
        if homeTeamId not in teamList[leagueId, seasonType]:
            teamsDict[leagueId, seasonType].append(homeTeam)
            teamList[leagueId, seasonType].append(homeTeamId)
        if awayTeamId not in teamList[leagueId, seasonType]:
            teamsDict[leagueId, seasonType].append(awayTeam)
            teamList[leagueId, seasonType].append(awayTeamId)
        if 'boxscore' in event:
            boxscore = event['boxscore']
            #print(boxscore)
            teams = boxscore['teams']
            teamUniformDB[id] = {}
            #if leagueId == '700':
            #    print('eventId',id,'leagueId',leagueId,'seasonType',seasonType)
            iOrder = 0
            for item in teams:
                tmpTeam = {}
                team = item['team']
                teamId = team['id']
                teamUid = team['uid']
                teamSlug = team['slug']
                teamLocation = team['location']
                teamName = team['name']
                if 'abbreviation' in team:
                    teamAbbreviation = team['abbreviation']
                else:
                    teamAbbreviation = ''
                teamDisplayName = team['displayName']
                teamShortDisplayName = team['shortDisplayName']
                if 'color' in team:
                    teamColor = team['color']
                else:
                    teamColor = ''
                if 'alternateColor' in team:
                    teamAlternateColor = team['alternateColor']
                else:
                    teamAlternateColor = ''
                if 'logo' in team:
                    teamLogo = team['logo']
                else:
                    teamLogo = ''
                if 'uniform' in team:
                    teamUniformType = team['uniform']['type']
                    if 'color' in team['uniform']:
                        teamUniformColor = team['uniform']['color']
                    else:
                        teamUniformColor = ''
                    if 'alternateColor' in team['uniform']:
                        teamUniformAlternateColor = team['uniform']['alternateColor']
                    else:
                        teamUniformType = ''
                        teamUniformColor = ''
                        teamUniformAlternateColor = ''
                else:
                    teamUniformType = ''
                    teamUniformColor = ''
                    teamUniformAlternateColor = ''
                teamOrder = i
                team['leagueId'] = leagueId
                team['updateTime'] = updateTime
                tmpTeam = {'id': teamId,
                           'uid': teamUid,
                           'slug': teamSlug,
                           'location': teamLocation,
                           'name': teamName,
                           'abbreviation': teamAbbreviation,
                           'displayName': teamShortDisplayName,
                           'shortDisplayName': teamShortDisplayName,
                           'logo': teamLogo}
                teamUniformDB[id][teamOrder] = {'id': id,
                                                'teamId': teamId,
                                                'teamOrder': teamOrder,
                                                'teamColor': teamColor,
                                                'teamAlternateColor': teamAlternateColor,
                                                'uniformType': teamUniformType,
                                                'uniformColor': teamUniformColor,
                                                'uniformAlternateColor': teamUniformAlternateColor}
                if teamId not in teamDB.keys():
                    teamDB[teamId] = tmpTeam
                if teamId not in leagueDB[leagueId][seasonType]:
                    leagueDB[leagueId][seasonType].append(teamId)
                tmpStat = {}
                tmpStat['eventId'] = id
                tmpStat['teamId'] = teamId
                tmpStat['teamOrder'] = iOrder
                tmpStat['updateTime'] = updateTime
                iOrder += 1
                if 'statistics' in item:
                    statistics = item['statistics']
                    tmpStat['hasStats'] = 'yes'
                    for stat in statistics:
                        name = stat['name']
                        label = stat['label']
                        displayValue = stat['displayValue']
                        tmpStat[name] = displayValue
                        if name not in statLabel:
                            statLabel[name] = label
                else:
                    tmpStat['hasStats'] = 'no'
                stats.append(tmpStat)
        else:
            print(i, 'no boxscore', id, len(event), list(event.keys()))
            errEvent = {'id': id, 'boxscore': 'not in event'}
            errLog.append(errEvent)
        if 'gameInfo' in event.keys():
            gameInfo = event['gameInfo']
            #print(event['gameInfo'])
            if 'venue' in gameInfo.keys():
                venue = gameInfo['venue']
                venueId = int(venue['id'])
                if 'fullName' in venue:
                    venueFullName = venue['fullName']
                else:
                    venueFullName = ''
                if 'shortName' in venue:
                    venueShortName = venue['shortName']
                else:
                    venueShortName = ''
                venueAddressCity = ''
                venueAddressCountry = ''
                if 'address' in venue:
                    if 'city' in venue['address']:
                        venueAddressCity = venue['address']['city']
                    if 'country' in venue['address']:
                        venueAddressCountry = venue['address']['country']
                venueCapacity = 0
                if 'capacity' in venue:
                    venueCapacity = venue['capacity']
                venueImages = ''
                if 'images' in venue:
                    venueImages = venue['images']
                venueEvent[id] = {'id': venueId}
                if venueId not in venueDBGameInfo.keys():
                    venueDBGameInfo[venueId] = {'id': venueId,
                                                'fullName': venueFullName,
                                                'shortName': venueShortName,
                                                'address.city': venueAddressCity,
                                                'address.country': venueAddressCountry,
                                                'capacity': venueCapacity,
                                                'images': ''
                                                }
            else:
                print(i, id, "venue not found")
                errLog.append({'id': id, 'venue': 'not found', 'logTime': currentTime})
            if 'attendance' in gameInfo.keys():
                attendance = gameInfo['attendance']
                attendanceDB[id] = {'attendance': attendance}
            if 'officials' in gameInfo.keys():
                officials = gameInfo['officials']
                officialDB[id] = officials
        else:
            print(i, id, "gameInfo not found")
            errLog.append({'id': id, 'gameInfo': 'not found', 'logTime': currentTime})
        if 'keyEvents' in event:
            if 'boxscore' in event:
                teamDisplayName1 = event['boxscore']['teams'][0]['team']['displayName']
                teamId1 = event['boxscore']['teams'][0]['team']['id']
                teamDisplayName2 = event['boxscore']['teams'][1]['team']['displayName']
                teamId2 = event['boxscore']['teams'][1]['team']['id']
                teamNameDict = {teamDisplayName1: teamId1, teamDisplayName2: teamId2}
                keyEvents = event['keyEvents']
                iKeyEvents = 0
                nOrder = 0
                for keyEvent in keyEvents:
                    nOrder += 1
                    #                (tmpKeyEvent,tmpKeyEventParticipants,tmpKeyEventSourceDB, keyEventTypeId,keyEventTypeText) \
                    #                    = extractKeyEvents(id,nOrder,keyEvent)
                    # return (tmpPlay, tmpPlayType, tmpPlayParticipants, tmpPlaySource)
                    (keyEventId, tmpKeyEvent, tmpKeyEventType, tmpKeyEventParticipants, tmpKeyEventSourceDB) \
                        = extractPlay(id, nOrder, keyEvent, teamNameDict)
                    if keyEventId not in keyEventDB[id].keys():
                        keyEventDB[id][keyEventId] = tmpKeyEvent
                        tmpId = tmpKeyEventType['id']
                        tmpText = tmpKeyEventType['text']
                        if tmpId not in keyEventTypeDB.keys():
                            keyEventTypeDB[tmpId] = {'id': tmpId, 'text': tmpText}
                        if len(tmpKeyEventParticipants) >= 1:
                            keyEventParticipantsDB[id][keyEventId] = []
                            for keyEventParticipant in tmpKeyEventParticipants:
                                keyEventParticipantsDB[id][keyEventId].append(keyEventParticipant)
                        for tmpKeyEventSourceId in tmpKeyEventSourceDB.keys():
                            if tmpKeyEventSourceId not in keyEventSourceDB.keys():
                                keyEventSourceDB[tmpKeyEventSourceId] = tmpKeyEventSourceDB[tmpKeyEventSourceId]
            else:
                print(i, id, 'no boxscore')
                errEvent = {'id': id, 'key events': 'no boxscore'}
                errLog.append(errEvent)
        else:
            print(i, id, 'no key events')
            errEvent = {'id': id, 'key events': 'not found'}
            errLog.append(errEvent)
        if 'commentary' in event:
            commentaries = event['commentary']
            i = 0
            nPlay = 0
            for commentary in commentaries:
                # print(commentary)
                tmpCommentary = {}
                if 'sequence' in commentary:
                    nOrder = int(commentary['sequence'])
                else:
                    nOrder = i
                timeValue = commentary['time']['value']
                timeDisplayValue = commentary['time']['displayValue']
                commentaryText = commentary['text']
                tmpCommentary['eventId'] = id
                tmpCommentary['sequence'] = nOrder
                tmpCommentary['timeValue'] = timeValue
                tmpCommentary['timeDisplayValue'] = timeDisplayValue
                tmpCommentary['text'] = commentaryText
                if 'play' in commentary.keys():
                    if 'id' in commentary['play']:
                        tmpPlayId = commentary['play']['id']
                        if tmpPlayId not in playDB[id].keys():
                            nPlay += 1
                            (playId, tmpPlay, tmpPlayType, tmpPlayParticipants, tmpPlaySourceDB) \
                                = extractPlay(id, nPlay, commentary['play'], teamNameDict)
                            playDB[id][playId] = tmpPlay
                            tmpId = tmpPlayType['id']
                            tmpText = tmpPlayType['text']
                            if tmpId not in keyEventTypeDB.keys():
                                keyEventTypeDB[tmpId] = {'id': tmpId, 'text': tmpText}
                            if len(tmpPlayParticipants) >= 1:
                                playParticipantsDB[id][playId] = []
                                for playParticipant in tmpPlayParticipants:
                                    playParticipantsDB[id][playId].append(playParticipant)
                            for tmpPlaySourceId in tmpPlaySourceDB.keys():
                                if tmpPlaySourceId not in keyEventSourceDB.keys():
                                    keyEventSourceDB[tmpPlaySourceId] = tmpPlaySourceDB[tmpPlaySourceId]
                        tmpCommentary['id'] = tmpPlayId
                    else:
                        tmpCommentary['id'] = ''
                else:
                    tmpCommentary['id'] = ''
                commentaryDB.append(tmpCommentary)
                iKeyEvents += 1
        else:
            print(i, id, 'no commentary')
            errEvent = {'id': id, 'commentary': 'not found'}
            errLog.append(errEvent)
        hasOdds = 'odds' in event.keys() and event['odds']
        if hasOdds:
            odds = event['odds']
            i = 0
            for odd in odds:
                oddsPerProvider = {}
                if 'links' in odd:
                    odd.pop('links')
                providerId = odd['provider']['id']
                providerName = odd['provider']['name']
                providerPriority = odd['provider']['priority']
                oddsProvider = {'id': providerId,
                                'name': providerName,
                                'priority': providerPriority}
                overUnder = odd['provider']['id']
                #if str(id) == '604729' and str(providerId) == '2000':
                #    print(id,providerId,odd)
                if ('awayTeamOdds' in odd and odd['awayTeamOdds']) or \
                        ('homeTeamOdds' in odd and odd['homeTeamOdds']):
                    oddsPerProvider['oddId'] = str(id) + '-' + str(providerId)
                    oddsPerProvider['providerId'] = providerId
                    oddsPerProvider['index'] = 0
                    oddsPerProvider['eventId'] = id
                    oddsPerProvider['updateTime'] = updateTime
                else:
                    print(odd)
                if 'awayTeamOdds' in odd and odd['awayTeamOdds']:
                    tmpOdds = odd['awayTeamOdds']
                    (value, key) = oddsValue(tmpOdds, 'favorite', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'underdog', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'moneyLine', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'team', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'spreadOdds', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    if 'odds' in tmpOdds and tmpOdds['odds']:
                        tmpOdds2 = tmpOdds['odds']
                        if 'summary' in tmpOdds2:
                            oddsSummary = tmpOdds2['summary']
                        else:
                            print(i, nPlayerOdds, id, odd)
                        oddsValue1 = tmpOdds2['value']
                        oddsHandicap = tmpOdds2['handicap']
                        oddsPerProvider['oddsSummary'] = oddsSummary
                        oddsPerProvider['oddsValue'] = oddsValue1
                        oddsPerProvider['oddsHandicap'] = oddsHandicap
                if 'homeTeamOdds' in odd and odd['homeTeamOdds']:
                    tmpOdds = odd['homeTeamOdds']
                    (value, key) = oddsValue(tmpOdds, 'favorite', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'underdog', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'moneyLine', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'team', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'spreadOdds', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(odd, 'details', '')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(odd, 'spread', '')
                    if value:
                        oddsPerProvider[key] = value
                    if 'odds' in tmpOdds and tmpOdds['odds']:
                        tmpOdds2 = tmpOdds['odds']
                        if 'summary' in tmpOdds2:
                            oddsSummary = tmpOdds2['summary']
                        else:
                            print(i, nPlayerOdds, id, odd)
                        oddsValue1 = tmpOdds2['value']
                        oddsHandicap = tmpOdds2['handicap']
                        oddsPerProvider['oddsSummary'] = oddsSummary
                        oddsPerProvider['oddsValue'] = oddsValue1
                        oddsPerProvider['oddsHandicap'] = oddsHandicap
                if 'drawOdds' in odd and odd['drawOdds']:
                    (value, key) = oddsValue(odd, 'drawOdds', '')
                    if value:
                        if 'moneyLine' in value:
                            key1 = key + 'MoneyLine'
                            oddsPerProvider[key1] = value['moneyLine']
                bBettingOdds = False
                bPlayerOdds = False
                if 'bettingOdds' in odd and odd['bettingOdds']:
                    bBettingOdds = True
                    tmpBettingOdds = odd['bettingOdds']
                    homeTeamUrl = tmpBettingOdds['homeTeam']['$ref']
                    awayTeamUrl = tmpBettingOdds['awayTeam']['$ref']
                    bettingOddsPerProvider = {}
                    bettingOddsPerProvider['oddId'] = str(id) + '-' + str(providerId)
                    bettingOddsPerProvider['providerId'] = providerId
                    bettingOddsPerProvider['index'] = 0
                    bettingOddsPerProvider['eventId'] = id
                    bettingOddsPerProvider['updateTime'] = updateTime
                    if 'teamOdds' in tmpBettingOdds and tmpBettingOdds['teamOdds']:
                        tmpTeamOdds = tmpBettingOdds['teamOdds']
                        for key in tmpTeamOdds:
                            oddId = tmpTeamOdds[key]['oddId']
                            value = tmpTeamOdds[key]['value']
                            betSlipUrl = tmpTeamOdds[key]['betSlipUrl']
                            keyOddId = key + 'OddId'
                            keyValue = key + 'Value'
                            keyBetSlipUrl = key + 'BetSlipUrl'
                            bettingOddsPerProvider[keyOddId] = oddId
                            bettingOddsPerProvider[keyValue] = value
                            bettingOddsPerProvider[keyBetSlipUrl] = betSlipUrl
                    if 'playerOdds' in tmpBettingOdds and tmpBettingOdds['playerOdds']:
                        bPlayerOdds = True
                        tmpPlayerOdds = tmpBettingOdds['playerOdds']
                        tmpPlayerOddsList = []
                        tmpPlayerOddsComb = {}
                        #print(id,tmpPlayerOdds)
                        for key0 in tmpPlayerOdds:
                            players = tmpPlayerOdds[key0]
                            for player in players:
                                #print(nPlayerOdds,player)
                                if 'oddId' in player:
                                    oddId = player['oddId']
                                else:
                                    oddId = 'none'
                                if 'value' in player:
                                    value = player['value']
                                else:
                                    value = 'none'
                                if 'betSlipUrl' in player:
                                    betSlipUrl = player['betSlipUrl']
                                else:
                                    betSlipUrl = 'none'
                                playerName = player['player']
                                keyOddId = key0 + 'OddId'
                                keyValue = key0 + 'Value'
                                keyBetSlipUrl = key0 + 'BetSlipUrl'
                                keyPlayer = key0 + 'Player'
                                if playerName not in tmpPlayerOddsComb.keys():
                                    tmpPlayerOddsComb[playerName] = {}
                                    tmpPlayerOddsComb[playerName]['oddId'] = str(id) + '-' + str(providerId)
                                    tmpPlayerOddsComb[playerName]['providerId'] = providerId
                                    tmpPlayerOddsComb[playerName]['index'] = 0
                                    tmpPlayerOddsComb[playerName]['eventId'] = id
                                    tmpPlayerOddsComb[playerName]['player'] = playerName
                                    tmpPlayerOddsComb[playerName]['updateTime'] = updateTime
                                tmpPlayerOddsComb[playerName][keyOddId] = oddId
                                tmpPlayerOddsComb[playerName][keyValue] = value
                                tmpPlayerOddsComb[playerName][keyBetSlipUrl] = betSlipUrl
                                tmpPlayerOddsComb[playerName][keyPlayer] = playerName
                        for playerName in tmpPlayerOddsComb:
                            nPlayerOdds += 1
                            tmpPlayerOddsList.append(tmpPlayerOddsComb[playerName])
                i += 1
                if providerId not in oddsProviderList:
                    oddsProviderList.append(providerId)
                    oddsProviderDB.append(oddsProvider)
                oddsDB.append(oddsPerProvider)
                if bBettingOdds:
                    bettingOddsDB.append(bettingOddsPerProvider)
                if bPlayerOdds:
                    playerOddsDB.append(tmpPlayerOddsList)
    else:
        print("id", id, "odds not found")
        errEvent = {'id': id, 'odds': 'not found', 'logTime': currentTime}
        errLog.append(errEvent)

        i += 1
    logFile.close()

    filename = saveDirTable + 'leagueList.json'
    print('League List    ', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(leagueDB1, file)
        file.close()

    filename = saveDirTable + 'importedEvents.json'
    print('imported events', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(importEventIndex, file)
        file.close()

    newTeamDB = []
    for key in teamDB.keys():
        newTeamDB.append(teamDB[key])

    filename = saveDirFixture + 'teams.json'
    print('teams          ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newTeamDB, file)
        file.close()

    newTeams = []
    for key in teamsDict.keys():
        leagueId = key[0]
        seasonType = key[1]
        tmpTeams = teamsDict[key]
        for team in tmpTeams:
            teamId = team['id']
            team['leagueId'] = leagueId
            team['seasonType'] = seasonType
            #team['updateTime'] = currentTime
            newTeams.append(team)

    filename = saveDirFixture + 'teamSeason.json'
    print('teamsSeason    ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newTeams, file)
        file.close()

    filename = saveDirFixture + 'fixtures.json'
    print('fixtures       ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(fixtures, file)
        file.close()

    filename = saveDirFixture + 'statusType.json'
    print('statusType     ', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(statusType, file)
        file.close()

    filename = saveDirFixture + 'seasonType.json'
    print('seasonType     ', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(season, file)
        file.close()

    filename = saveDirFixture + 'teamStats.json'
    print('teamStats      ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(stats, file)
        file.close()

    filename = saveDirFixture + 'statLabel.json'
    print('statLabel      ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(statLabel, file)
        file.close()

    filename = saveDirFixture + 'details.json'
    print('details        ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(details, file)
        file.close()

    filename = saveDirFixture + 'detailTypes.json'
    print('detailTypes    ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(detailType, file)
        file.close()

    filename = saveDirFixture + 'venueDB.json'
    print('venueDB        ', filename)
    newVenueDB = []
    for venueId in venueDBFixture:
        newVenueDB.append(venueDBFixture[venueId])
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newVenueDB, file)
        file.close()

    dataSet['status       '] = "complete"

    filename = saveDirTable + 'dataSet.json'
    print('dataSet        ', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(dataSet, file)
        file.close()
    print("bad Events     ", badEvents)

    leagueList2 = leagueDB1

    leagues = []
    #print("leagueList2",leagueList2)
    #print("leagueDB",leagueDB)
    for tmpLeague in leagueList2:
        #print(tmpLeague)
        tmpLeagueId = tmpLeague['id']
        if tmpLeagueId in leagueList.keys():
            # print(leagueList[tmpLeagueId]['isTournament'])
            # print(leagueList2[tmpLeagueId])
            tmpLeague['isTournament'] = leagueList[tmpLeagueId]['isTournament']
            # tmpLeague['updateTime'] = leagueList[tmpLeagueId]['updateTime']
        else:
            tmpLeague['isTournament'] = None
            # tmpLeague['updateTime'] = None
        leagues.append(tmpLeague)

    filename = saveDirBoxscore + 'leagues.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(leagues, file)
        file.close()
    print('leagues        ', filename)

    teamList = []
    for leagueId in leagueDB:
        for seasonType in leagueDB[leagueId]:
            for teamId in leagueDB[leagueId][seasonType]:
                tmpTeam = {'leagueId': leagueId,
                           'seasonType': seasonType,
                           'teamId': teamId}
                teamList.append(tmpTeam)
    filename = saveDirBoxscore + 'leagueDB.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(teamList, file)
        file.close()
    print('leagueDB       ', filename)

    uniformList = []
    for eventId in teamUniformDB:
        for nOrder in teamUniformDB[eventId]:
            uniformList.append(teamUniformDB[eventId][nOrder])
    filename = saveDirBoxscore + 'teamUniform.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(uniformList, file)
        file.close()
    print('teamUniform    ', filename)

    seasons = []
    for seasonType in seasonDB:
        seasons.append(seasonDB[seasonType])
    filename = saveDirBoxscore + 'seasons.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(seasons, file)
        file.close()
    print('season         ', filename)

    teams = []
    for teamId in teamDB:
        teams.append(teamDB[teamId])
    filename = saveDirBoxscore + 'teams.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(teams, file)
        file.close()
    print('teams          ', filename)

    newStats = []
    for stat in stats:
        tmpStat = {}
        for statKey in stat.keys():
            if statKey not in statLabel.keys():
                tmpStat[statKey] = stat[statKey]
        for label in statLabel:
            if label in stat.keys():
                tmpStat[label] = stat[label]
            else:
                tmpStat[label] = ''
        newStats.append(tmpStat)

    filename = saveDirBoxscore + 'teamStats.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(newStats, file)
        file.close()
    print('stats          ', filename)

    filename = saveDirBoxscore + 'statLabel.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(statLabel, file)
        file.close()
    print('statLabel      ', filename)

    #filename = saveDirBoxscore + 'errLogBoxscore.json'
    #if os.path.exists(saveDirBoxscore):
    #    with open(filename, 'w') as file:
    #        json.dump(errLog, file)
    #    file.close()
    #print('errLog         ',filename)

    officialsTb = []
    # print(officialDB)
    for eventId in officialDB.keys():
        tmpOfficials = officialDB[eventId]
        for tmpOfficial in tmpOfficials:
            tmpTb = {}
            tmpTb['eventId'] = eventId
            for key in tmpOfficial.keys():
                tmpTb[key] = tmpOfficial[key]
            officialsTb.append(tmpTb)

    venueTb = []
    for venueId in venueDBGameInfo.keys():
        tmpTb = {}
        tmpTb['id'] = venueId
        for key in venueDBGameInfo[venueId].keys():
            tmpTb[key] = venueDBGameInfo[venueId][key]
        venueTb.append(tmpTb)

    venueEventTb = []
    for eventId in venueEvent.keys():
        tmpTb = {}
        tmpTb['eventId'] = eventId
        # print(eventId,venueEvent[eventId])
        for key in venueEvent[eventId].keys():
            tmpTb[key] = venueEvent[eventId][key]
        venueEventTb.append(tmpTb)
        # print(venueEventTb)

    attendanceTb = []
    for eventId in attendanceDB.keys():
        tmpTb = {}
        tmpTb['eventId'] = eventId
        for key in attendanceDB[eventId].keys():
            tmpTb[key] = attendanceDB[eventId][key]
        attendanceTb.append(tmpTb)

    # print('Write GameInfo DB...')

    filename = saveDirGameInfo + 'venueTb.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(venueTb, file)
        file.close()
    print('venue table    ', filename)

    filename = saveDirGameInfo + 'venue.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(venueEventTb, file)
        file.close()
    print('venue events   ', filename)

    filename = saveDirGameInfo + 'officials.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(officialsTb, file)
        file.close()
    print('officials      ', filename)

    filename = saveDirGameInfo + 'attendance.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(attendanceTb, file)
        file.close()
    print('attendance     ', filename)

    #filename = saveDirGameInfo + 'errLog_gameInfo.json'
    #if os.path.exists(saveDirGameInfo):
    #    with open(filename, 'w') as file:
    #        json.dump(errLog, file)
    #    file.close()
    #print('errors         ',filename)

    print()

    len1 = 0
    combinedPlayDB = {}
    combinedParticipantsDB = {}
    for id in playDB:
        combinedPlayDB[id] = {}
        combinedParticipantsDB[id] = {}
        combinedPlayList = {}
        combinedParticipantsList = {}
        for playId in playDB[id]:
            combinedPlayList[playId] = playDB[id][playId]
            if playId in playParticipantsDB[id].keys():
                combinedParticipantsList[playId] = playParticipantsDB[id][playId]
        for keyEventId in keyEventDB[id]:
            combinedPlayList[keyEventId] = keyEventDB[id][keyEventId]
            if keyEventId in keyEventParticipantsDB[id].keys():
                combinedParticipantsList[keyEventId] = keyEventParticipantsDB[id][keyEventId]
        tmpCombinedPlayList = []
        for playId in combinedPlayList.keys():
            tmpCombinedPlayList.append(combinedPlayList[playId])
        # print(tmpCombinedPlayList)
        sortedCombinedPlayList = sorted(tmpCombinedPlayList, key=lambda d: (d['period'], d['clockValue']))
        for sortedCombinedPlay in sortedCombinedPlayList:
            playId = sortedCombinedPlay['id']
            combinedPlayDB[id][playId] = sortedCombinedPlay
            if playId in combinedParticipantsList.keys():
                combinedParticipantsDB[id][playId] = combinedParticipantsList[playId]

    # print(keyEventDB)
    keyEventDBList = []
    for id in keyEventDB:
        for keyEventId in keyEventDB[id]:
            keyEventDBList.append(keyEventDB[id][keyEventId])

    filename = saveDirKeyEvents + 'keyEvent.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventDBList, file)
        file.close()
    print('key events             ', filename)

    filename = saveDirKeyEvents + 'commentary.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(commentaryDB, file)
        file.close()
    print('commentary             ', filename)

    playDBList = []
    for id in playDB:
        for playId in playDB[id]:
            playDBList.append(playDB[id][playId])

    filename = saveDirKeyEvents + 'plays.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(playDBList, file)
        file.close()

    combinedPlayDBList = []
    for id in combinedPlayDB:
        for playId in combinedPlayDB[id]:
            combinedPlayDBList.append(combinedPlayDB[id][playId])
    print('plays                  ', filename)

    filename = saveDirKeyEvents + 'combinedPlays.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(combinedPlayDBList, file)
        file.close()
    print('combined plays         ', filename)

    filename = saveDirKeyEvents + 'keyEventType.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventTypeDB, file)
        file.close()
    print('key events type        ', filename)

    filename = saveDirKeyEvents + 'keyEventSource.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventSourceDB, file)
        file.close()

    keyEventParticipantsDBList = []
    for id in keyEventParticipantsDB:
        for keyEventId in keyEventParticipantsDB[id]:
            for keyEventParticipant in keyEventParticipantsDB[id][keyEventId]:
                keyEventParticipantsDBList.append(keyEventParticipant)
    print('key events source      ', filename)

    filename = saveDirKeyEvents + 'keyEventParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventParticipantsDBList, file)
        file.close()

    playParticipantsDBList = []
    for id in playParticipantsDB:
        for playId in playParticipantsDB[id]:
            for playParticipant in playParticipantsDB[id][playId]:
                playParticipantsDBList.append(playParticipant)
    print('key events participants', filename)

    filename = saveDirKeyEvents + 'playParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(playParticipantsDBList, file)
        file.close()
    print('play participants      ', filename)

    combinedParticipantsDBList = []
    for id in combinedParticipantsDB:
        for playId in combinedParticipantsDB[id]:
            for playParticipant in combinedParticipantsDB[id][playId]:
                combinedParticipantsDBList.append(playParticipant)

    filename = saveDirKeyEvents + 'combinedParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(combinedParticipantsDBList, file)
        file.close()
    print('combined participants  ', filename)

    print()
    if bUpdateOdds:
        (oddsProviderDB, oddsDB, err) = readOddsDB(saveDirOdds)

        for odds in oddsDB:
            eventId = odds['id']

    err = writeOddsDB(oddsProviderDB, oddsDB, bettingOddsDB, playerOddsDB, saveDirOdds)
    print(err)

    print()
    filename = saveDirErrLog + 'errLog.json'
    if os.path.exists(saveDirErrLog):
        with open(filename, 'w') as file:
            json.dump(errLog, file)
        file.close()
    print('errors         ', filename)
    return ("event extractioin complete!")


def extractLeagues(dirLeagueList,saveDirTable):
    #
    # Open leaague list and generate leagueDB
    #
    (leagueListFromFile, err) = open_league_list(dirLeagueList)
    leagueIndex = {}
    leagueDB1 = []
    if err == 0:
        for i in leagueListFromFile:
            tmpLeague = {}
            if 'id' in leagueListFromFile[i]:
                tmpLeague['id'] = leagueListFromFile[i]['id']
            else:
                tmpLeague['id'] = 'none'
            if 'alternateId' in leagueListFromFile[i]:
                tmpLeague['alternateId'] = leagueListFromFile[i]['alternateId']
            else:
                tmpLeague['alternateId'] = 'none'
            if 'name' in leagueListFromFile[i]:
                tmpLeague['name'] = leagueListFromFile[i]['name']
            else:
                tmpLeague['name'] = 'none'
            if 'abbreviation' in leagueListFromFile[i]:
                tmpLeague['abbreviation'] = leagueListFromFile[i]['abbreviation']
            else:
                tmpLeague['abbreviation'] = 'none'
            if 'shortName' in leagueListFromFile[i]:
                tmpLeague['shortName'] = leagueListFromFile[i]['shortName']
            else:
                tmpLeague['shortName'] = 'none'
            if 'midsizeName' in leagueListFromFile[i]:
                tmpLeague['midsizeName'] = leagueListFromFile[i]['midsizeName']
                midsizeName = leagueListFromFile[i]['midsizeName']
            else:
                tmpLeague['midsizeName'] = 'none'
                midsizeName = 'none'
            if 'slug' in leagueListFromFile[i]:
                tmpLeague['slug'] = leagueListFromFile[i]['slug']
            else:
                tmpLeague['slug'] = 'none'
            if 'season' in leagueListFromFile[i]:
                if 'type' in leagueListFromFile[i]['season']:
                    tmpLeague['seasonTypeId'] = leagueListFromFile[i]['season']['type']['id']
                    tmpLeague['seasonHasStandings'] = leagueListFromFile[i]['season']['type']['hasStandings']
                else:
                    tmpLeague['seasonTypeId'] = 0
                    tmpLeague['seasonHasStandings'] = False
            else:
                tmpLeague['seasonTypeId'] = 0
                tmpLeague['seasonHasStandings'] = False
            if "logos" in leagueListFromFile[i]:
                try:
                    logo1 = leagueListFromFile[i]['logos'][0]["href"]
                    logo1LastUpdated = leagueListFromFile[i]['logos'][0]["lastUpdated"]
                    # print(logo1, logo1LastUpdated)
                except:
                    logo1 = ""
                    logo1LastUpdated = ""
                if len(leagueListFromFile[i]['logos']) == 2:
                    try:
                        logo2 = leagueListFromFile[i]['logos'][1]["href"]
                        logo2LastUpdated = leagueListFromFile[i]['logos'][1]["lastUpdated"]
                        # print(logo2, logo2LastUpdated)
                    except:
                        logo2 = ""
                        logo2LastUpdated = ""
                else:
                    logo2 = ""
                    logo2LastUpdated = ""
            else:
                logo1 = ""
                logo1LastUpdated = ""
                logo2 = ""
                logo2LastUpdated = ""

            tmpLeague["logoUrl1"] = logo1
            tmpLeague["logoUrl1LastUpdated"] = logo1LastUpdated
            tmpLeague["logoUrl2"] = logo2
            tmpLeague["logoUrl2LastUpdated"] = logo2LastUpdated

            if 'hasStandings' in leagueListFromFile[i]:
                tmpLeague['hasStandings'] = leagueListFromFile[i]['hasStandings']
            else:
                tmpLeague['hasStandings'] = 'none'
            if 'updateTime' in leagueListFromFile[i]:
                tmpLeague['updateTime'] = leagueListFromFile[i]['updateTime']
            else:
                tmpLeague['updateTime'] = 'none'
            leagueIndex[tmpLeague['id']] = midsizeName
            leagueDB1.append(tmpLeague)
        #print(leagueDB1)
        #print(leagueIndex)
    else:
        print("error extract league list")

    filename = saveDirTable + 'leagueList.json'
    print('League List    ', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(leagueDB1, file)
        file.close()

    return (leagueIndex, leagueDB1, err)


def genDataSet(saveDirTable, start_date_Str, end_date_Str,extractDateStr,statusStr):

    dataSet = {"startDate": start_date_Str,
               "endDate": end_date_Str,
               "extractionDate": extractDateStr,
               "status": statusStr}

    err = 0
    filename = saveDirTable + 'dataSet.json'
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(dataSet, file)
        file.close()
        print("dataset   ", filename)
    else:
        print("dataset save dir not exist")
        err = -1
    return (dataSet,err)
def extractFixtures(eventDir, fixtureDir, saveDirFixture, saveDirTable,
                    eventImportDir,importedLeagues,leagueList, errLog):
    description = "Extract Fixtures:"
    print(description, "eventDir",eventDir)
    print(description, "saveDirFixture",saveDirFixture)
    today = datetime.today()
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    #
    # open fixtures by date.
    # read in a list of events on each date
    # find and open each event in event directory.  re-import event if there's error in event
    # unpack events
    #

    fixture = {}
    teamsDict = {}
    teamList = {}
    teamDB = {}
    fixtures = []
    statusType = {}
    season = []
    stats = []
    details = []
    detailType = {}
    importEventIndex = []
    statLabel = {}
    venueDBFixture = {}

    leagueDB = {}
    #leagueList = {}
    seasonDB = {}

    delim = ','  # change delim to ','
    k = 0
    iEvent = 0
    #importLogFileName = saveDirFixture + 'events.log'
    #logFile = open(importLogFileName, 'w')
    badEvents = []
    for tmpLeague in importedLeagues:
        tmpFixture = {}
        id = tmpLeague["eventId"]

        matchDate = tmpLeague['date']
        leagueId = tmpLeague["leagueId"]
        midsizeLeagueName = tmpLeague["midsizeName"]
        saveFileName = tmpLeague['fileName']

        (err, fixture, errEvent) = openFixture(id, saveFileName, fixtureDir)
        if err < 0:
            badEvents.append(tmpLeague)
            print(description,errEvent)
            errLog.append(errEvent)
            continue

        (bFile, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not bFile:
            print(description,errEvent)
            errLog.append(errEvent)
            continue

        #if 'shortName' in fixture.keys():
        #    shortName = fixture['shortName']
        seasonTmp = fixture['season']
        if seasonTmp not in season:
            season.append(seasonTmp)
        year = seasonTmp['year']
        seasonType = seasonTmp['type']
        seasonSlug = seasonTmp['slug']
        competitions1 = fixture['competitions'][0]
        if 'updateTime' in event:
            updateTime = event['updateTime']
        else:
            #updateTime = allFixtures[id]['updateTime']
            updateTime = currentTime
            #print(updateTime)
        bEvent = False
        header = {}
        competitions2 = {}
        if 'header' in event:
            header = event['header']
            if 'competitions' in header:
                competitions2 = event['header']['competitions'][0]
                bEvent = True
        status = fixture['status']

        (leagueId,league,errEvent) = extractLeagueIdFromEvent(event)
        if leagueId > 0:
            if leagueId not in leagueList.keys():
                leagueList[leagueId] = league
                leagueDB[leagueId] = {}
        else:
            print(description, iEvent, "eventId", id, "leagueId", leagueId,"invalid leagueId")
            errEvent = {'id': id, 'headerr': 'invalid leagueId'}
            errLog.append(errEvent)

        (typeId,seasonTmp2,errEvent) = extractSeasonFromEvent(event)
        #print(iEvent,id,'typeId',typeId, leagueId)
        if typeId > 0:
            seasonTmp2 = header['season']
            seasonYear = seasonTmp2['year']
            seasonName = seasonTmp2['name']
            if typeId not in seasonDB.keys():
                seasonDB[typeId] = {'year': seasonYear,
                                    'type': typeId,
                                    'name': seasonName,
                                    'slug': seasonSlug}
            if leagueId > 0:
                if typeId not in leagueDB[leagueId]:
                    leagueDB[leagueId][typeId] = []
            else:
                print(description, iEvent, "eventId", id, "leagueId", leagueId,"invalid leagueId")
                errEvent = {'id': id, 'headerr': 'invalid leagueId'}
                errLog.append(errEvent)
        else:
            print(description, iEvent, "eventId", id, "seaonsType", typeId,"invalid seaonType")
            errEvent = {'id': id, 'header': 'invalid seasonType'}
            errLog.append(errEvent)

        importEventIndex.append({'id': id, 'updateTime': updateTime})
        tmpFixture['eventId'] = id
        tmpFixture['leagueId'] = leagueId
        tmpFixture['uid'] = competitions1['uid']
        #tmpFixture['uid']=uid
        tmpFixture['attendance'] = competitions1['attendance']
        #tmpFixture['midsizeLeagueName']=midsizeLeagueName
        tmpFixture['date'] = competitions1['date']
        tmpFixture['startDate'] = competitions1['startDate']
        if bEvent:
            tmpFixture['neutralSite'] = competitions2['neutralSite']
            if tmpFixture['neutralSite'] == '':
                tmpFixture['neutralSite'] = 'false'
            tmpFixture['conferenceCompetition'] = competitions2['conferenceCompetition']
            tmpFixture['boxscoreAvailable'] = competitions2['boxscoreAvailable']
            tmpFixture['commentaryAvailable'] = competitions2['commentaryAvailable']
            tmpFixture['recent'] = competitions2['recent']
            tmpFixture['boxscoreSource'] = competitions2['boxscoreSource']
            tmpFixture['playByPlaySource'] = competitions2['playByPlaySource']
        #tmpFixture['year']=year
        tmpFixture['seasonType'] = seasonType
        #tmpFixture['seasonSlug']=seasonSlug
        statusTypeId = status['type']['id']
        tmpFixture['statusId'] = statusTypeId
        if statusTypeId not in statusType.keys():
            statusType[statusTypeId] = status['type']
        tmpFixture['clock'] = status['clock']
        tmpFixture['displayClock'] = status['displayClock']
        if 'period' in status:
            tmpFixture['period'] = status['period']
        else:
            tmpFixture['period'] = 'unknown'
        if 'venue' in competitions1.keys():
            venueTmp = competitions1['venue']
            venueId = venueTmp['id']
            venueFullName = venueTmp['fullName']
            if 'address' in venueTmp.keys():
                if 'city' in venueTmp['address'].keys():
                    venueCity = venueTmp['address']['city']
                else:
                    venueCity = None
            else:
                venueCity = None
            if venueId not in venueDBFixture.keys():
                venueDBFixture[venueId] = {'id': venueId,
                                           'fullName': venueFullName,
                                           'city': venueCity}
            tmpFixture['venueId'] = venueId
        competitors1 = competitions1['competitors']
        for competitor1 in competitors1:
            #print(list(competitor.keys()))
            homeAway = competitor1['homeAway']
            if homeAway == 'home':
                homeTeam = competitor1['team']
                if 'venue' in homeTeam:
                    venueId = homeTeam['venue']['id']
                    homeTeam.pop('venue')
                    homeTeam['venueId'] = venueId
                else:
                    homeTeam['venueId'] = None
                homeTeam.pop('links')
                homeTeam['updateTime'] = updateTime

                homeTeamId1 = competitor1['id']
                homeTeamId2 = competitor1['team']['id']
                homeTeamUid = competitor1['uid']
                homeTeamOrder = competitor1['order']
                homeTeamWinner = competitor1['winner']
                homeTeamScore = competitor1['score']
                if homeTeamId2 not in teamDB.keys():
                    teamDB[homeTeamId2] = homeTeam
                elif homeTeam['venueId'] != None and teamDB[homeTeamId2]['venueId'] == None:
                    teamDB[homeTeamId2] = homeTeam
                if 'shootoutScore' in competitor1.keys():
                    homeTeamShootoutScore = competitor1['shootoutScore']
                else:
                    homeTeamShootoutScore = ''
                homeTeamId = homeTeamId1
                tmpFixture['homeTeamId'] = homeTeamId
                tmpFixture['homeTeamUid'] = homeTeamUid
                tmpFixture['homeTeamOrder'] = homeTeamOrder
                tmpFixture['homeTeamWinner'] = homeTeamWinner
                tmpFixture['homeTeamScore'] = homeTeamScore
                tmpFixture['homeTeamShootoutScore'] = homeTeamShootoutScore
                if 'form' in competitor1:
                    homeTeamForm = competitor1['form']
                    tmpFixture['homeTeamForm'] = homeTeamForm
                if 'statistics' in competitor1:
                    homeTeamStats = competitor1['statistics']
                    if len(homeTeamStats) > 0:
                        tmpStats = {}
                        tmpStats['eventId'] = id
                        tmpStats['teamId'] = homeTeamId
                        tmpStats['homeAway'] = homeAway
                        tmpStats['statusId'] = tmpFixture['statusId']
                        for stat in homeTeamStats:
                            statName = stat['name']
                            statAbv = stat['abbreviation']
                            statDisplayValue = stat['displayValue']
                            tmpStats[statName] = statDisplayValue
                            if statName not in statLabel.keys():
                                statLabel[statName] = statAbv
                        tmpStats['updateTime'] = updateTime
                        stats.append(tmpStats)
                        bHomeTeamStats = True
                    else:
                        bHomeTeamStats = False
                else:
                    bHomeTeamStats = False
                #print(homeAway,homeTeam['displayName'], competitor['id'], competitor['uid'], competitor['order'],
                #      competitor['winner'],competitor['score'],competitor['form'])
            if homeAway == 'away':
                awayTeam = competitor1['team']
                if 'venue' in awayTeam:
                    venueId = awayTeam['venue']['id']
                    awayTeam.pop('venue')
                    awayTeam['venueId'] = venueId
                else:
                    awayTeam['venueId'] = None
                awayTeam.pop('links')
                awayTeam['updateTime'] = updateTime
                awayTeamId = competitor1['team']['id']
                awayTeamId1 = competitor1['id']
                awayTeamId2 = competitor1['team']['id']
                awayTeamUid = competitor1['uid']
                awayTeamOrder = competitor1['order']
                awayTeamWinner = competitor1['winner']
                awayTeamScore = competitor1['score']
                if awayTeamId2 not in teamDB.keys():
                    teamDB[awayTeamId2] = awayTeam
                elif awayTeam['venueId'] != None and teamDB[awayTeamId2]['venueId'] == None:
                    teamDB[awayTeamId2] = awayTeam
                if 'shootoutScore' in competitor1.keys():
                    awayTeamShootoutScore = competitor1['shootoutScore']
                else:
                    awayTeamShootoutScore = ''
                awayTeamId = awayTeamId1
                tmpFixture['awayTeamId'] = awayTeamId
                tmpFixture['awayTeamUid'] = awayTeamUid
                tmpFixture['awayTeamOrder'] = awayTeamOrder
                tmpFixture['awayTeamWinner'] = awayTeamWinner
                tmpFixture['awayTeamScore'] = awayTeamScore
                tmpFixture['awayTeamShootoutScore'] = awayTeamShootoutScore
                if 'form' in competitor1:
                    awayTeamForm = competitor1['form']
                    tmpFixture['awayTeamForm'] = awayTeamForm
                if 'statistics' in competitor1:
                    awayTeamStats = competitor1['statistics']
                    if len(awayTeamStats) > 0:
                        tmpStats = {}
                        tmpStats['eventId'] = id
                        tmpStats['teamId'] = awayTeamId
                        tmpStats['homeAway'] = homeAway
                        tmpStats['statusId'] = tmpFixture['statusId']
                        for stat in awayTeamStats:
                            statName = stat['name']
                            statAbv = stat['abbreviation']
                            statDisplayValue = stat['displayValue']
                            tmpStats[statName] = statDisplayValue
                        tmpStats['updateTime'] = updateTime
                        stats.append(tmpStats)
                        bAwayTeamStats = True
                    else:
                        bAwayTeamStats = False
                else:
                    bAwayTeamStats = False
                tmpFixture['hasStats'] = 'no'
                if bHomeTeamStats and bAwayTeamStats:
                    tmpFixture['hasStats'] = 'yes'

                #print(homeAway,awayTeam['displayName'], competitor['id'],competitor['uid'],competitor['order'],
                #      competitor['winner'],competitor['score'],competitor['form'])
        if 'details' in competitions1.keys():
            nYellowCard = {}
            nYellowCard[homeTeamId] = 0
            nYellowCard[awayTeamId] = 0
            nRedCard = {}
            nRedCard[homeTeamId] = 0
            nRedCard[awayTeamId] = 0
            tmpDetails = competitions1['details']
            nDetails = 0
            for tmpDetail in tmpDetails:
                tmpDetailDict = {}
                tmpDetailDict['eventId'] = id
                tmpDetailDict['order'] = nDetails
                for tmpKey in tmpDetail:
                    if tmpKey == 'type':
                        tmpDetailType = tmpDetail['type']
                        tmpDetailDict['typeId'] = tmpDetailType['id']
                        tmpDetailDict['typeText'] = tmpDetailType['text']
                        if tmpDetailType['id'] not in detailType.keys():
                            detailType[tmpDetailType['id']] = tmpDetailType['text']
                    elif tmpKey == 'clock':
                        tmpDetailDict['clockValue'] = tmpDetail['clock']['value']
                        tmpDetailDict['clockDisplayValue'] = tmpDetail['clock']['displayValue']
                    elif tmpKey == 'team':
                        tmpDetailDict['teamId'] = tmpDetail['team']['id']
                    elif tmpKey == 'athletesInvolved':
                        tmpDetailDict['athletesInvolved'] = tmpDetail['athletesInvolved'][0]['id']
                    else:
                        tmpDetailDict[tmpKey] = tmpDetail[tmpKey]
                if tmpDetailDict['yellowCard'] == True:
                    if tmpDetailDict['teamId'] in nYellowCard.keys():
                        nYellowCard[tmpDetailDict['teamId']] += 1
                    else:
                        nYellowCard[tmpDetailDict['teamId']] = 1
                        print(description, tmpDetailDict['teamId'], homeTeamId, awayTeamId)
                if tmpDetailDict['redCard'] == True:
                    if tmpDetailDict['teamId'] in nRedCard.keys():
                        nRedCard[tmpDetailDict['teamId']] += 1
                    else:
                        nRedCard[tmpDetailDict['teamId']] = 1
                        print(description, tmpDetailDict['teamId'], homeTeamId, awayTeamId)
                details.append(tmpDetailDict.copy())
                nDetails += 1
        else:
            print(description, iEvent, id, 'no details')
            errEvent = {'id': id, 'details': 'not found'}
            errLog.append(errEvent)

        homeYellowCard = 0
        awayYellowCard = 0
        homeRedCard = 0
        awayRedCard = 0
        for teamId in nYellowCard.keys():
            if teamId == homeTeamId:
                homeYellowCard = nYellowCard[teamId]
                homeRedCard = nRedCard[teamId]
            if teamId == awayTeamId:
                awayYellowCard = nYellowCard[teamId]
                awayRedCard = nRedCard[teamId]
        tmpFixture['homeYellowCard'] = homeYellowCard
        tmpFixture['homeRedCard'] = homeRedCard
        tmpFixture['awayYellowCard'] = awayYellowCard
        tmpFixture['awayRedCard'] = awayRedCard
        tmpFixture['updateTime'] = updateTime
        fixtures.append(tmpFixture)

        print(description, iEvent + 1, id, homeTeam['name'], awayTeam['name'],
              midsizeLeagueName, leagueId, year, seasonType)

        if (leagueId, seasonType) not in teamsDict.keys():
            teamsDict[leagueId, seasonType] = []
            teamList[leagueId, seasonType] = []
        if homeTeamId not in teamList[leagueId, seasonType]:
            teamsDict[leagueId, seasonType].append(homeTeam)
            teamList[leagueId, seasonType].append(homeTeamId)
        if awayTeamId not in teamList[leagueId, seasonType]:
            teamsDict[leagueId, seasonType].append(awayTeam)
            teamList[leagueId, seasonType].append(awayTeamId)
        #errLog.append(errEvent)
        iEvent += 1
    #logFile.close()

    print()
    filename = saveDirTable + 'leagueList.json'
    print(description, 'League List', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(leagueDB, file)
        file.close()

    filename = saveDirTable + 'importedEvents.json'
    print(description,'imported events', filename)
    if os.path.exists(saveDirTable):
        with open(filename, 'w') as file:
            json.dump(importEventIndex, file)
        file.close()

    newTeamDB = []
    for key in teamDB.keys():
        newTeamDB.append(teamDB[key])

    filename = saveDirFixture + 'teams.json'
    print(description, 'teams          ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newTeamDB, file)
        file.close()

    newTeams = []
    for key in teamsDict.keys():
        leagueId = key[0]
        seasonType = key[1]
        tmpTeams = teamsDict[key]
        for team in tmpTeams:
            teamId = team['id']
            team['leagueId'] = leagueId
            team['seasonType'] = seasonType
            #team['updateTime'] = currentTime
            newTeams.append(team)

    filename = saveDirFixture + 'teamSeason.json'
    print(description, 'teamsSeason    ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newTeams, file)
        file.close()

    filename = saveDirFixture + 'fixtures.json'
    print(description, 'fixtures       ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(fixtures, file)
        file.close()

    filename = saveDirFixture + 'statusType.json'
    print(description, 'statusType     ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(statusType, file)
        file.close()

    filename = saveDirFixture + 'seasonType.json'
    print(description, 'seasonType     ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(season, file)
        file.close()

    seasons = []
    for seasonType in seasonDB:
        seasons.append(seasonDB[seasonType])
    filename = saveDirFixture + 'seasons.json'
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(seasons, file)
        file.close()
    print(description, 'season         ', filename)

    filename = saveDirFixture + 'teamStats.json'
    print(description, 'teamStats      ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(stats, file)
        file.close()

    filename = saveDirFixture + 'statLabel.json'
    print(description, 'statLabel      ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(statLabel, file)
        file.close()

    filename = saveDirFixture + 'details.json'
    print(description, 'details        ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(details, file)
        file.close()

    filename = saveDirFixture + 'detailTypes.json'
    print(description, 'detailTypes    ', filename)
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(detailType, file)
        file.close()

    filename = saveDirFixture + 'venueDB.json'
    print(description, 'venueDB        ', filename)
    newVenueDB = []
    for venueId in venueDBFixture:
        newVenueDB.append(venueDBFixture[venueId])
    if os.path.exists(saveDirFixture):
        with open(filename, 'w') as file:
            json.dump(newVenueDB, file)
        file.close()

    print(description, "bad Events     ", badEvents)
    msg = "fixtures imported"
    return msg,leagueDB,errLog

def extractLeagueIdFromEvent(event):
    leagueId = 0
    league = {}
    errEvent = {}
    if 'header' in event.keys():
        header = event['header']
        if 'league' in header:
            league = header['league']
            if 'id' in league:
                leagueId = int(league['id'])
    else:
        errEvent = {'id': id, 'header': 'not found'}
    return leagueId,league, errEvent
def extractSeasonFromEvent(event):
    typeId = 0
    season = {}
    errEvent = {}
    if 'header' in event.keys():
        header = event['header']
        if 'season' in header:
            season = header['season']
            if 'type' in season:
                typeId = int(season['type'])
    else:
        errEvent = {'id': id, 'header': 'not found'}
    return typeId,season, errEvent
def extractBoxscore(eventDir, saveDirBoxscore, eventImportDir,
                    importedLeagues,
                    leagueDB1, leagueDB, updateTime, errLog):
    description = "Extract Boxscore"

    leagueList = {}

    seasonDB = {}
    teamDB = {}
    teamUniformDB = {}
    stats = []
    statLabel = {}

    iEvent = 0
    for tmpLeague in importedLeagues:
        id = tmpLeague["eventId"]
        (bFile, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not bFile:
            print(description, errEvent)
            errLog.append(errEvent)
            continue

        (leagueId, league, errEvent) = extractLeagueIdFromEvent(event)

        if 'boxscore' in event:
            boxscore = event['boxscore']
            # print(boxscore)
            teams = boxscore['teams']
            teamUniformDB[id] = {}
            # if leagueId == '700':
            #    print('eventId',id,'leagueId',leagueId,'seasonType',seasonType)
            iOrder = 0
            for item in teams:
                tmpTeam = {}
                team = item['team']
                teamId = team['id']
                teamUid = team['uid']
                teamSlug = team['slug']
                teamLocation = team['location']
                teamName = team['name']
                if 'abbreviation' in team:
                    teamAbbreviation = team['abbreviation']
                else:
                    teamAbbreviation = ''
                teamDisplayName = team['displayName']
                teamShortDisplayName = team['shortDisplayName']
                if 'color' in team:
                    teamColor = team['color']
                else:
                    teamColor = ''
                if 'alternateColor' in team:
                    teamAlternateColor = team['alternateColor']
                else:
                    teamAlternateColor = ''
                if 'logo' in team:
                    teamLogo = team['logo']
                else:
                    teamLogo = ''
                if 'uniform' in team:
                    teamUniformType = team['uniform']['type']
                    if 'color' in team['uniform']:
                        teamUniformColor = team['uniform']['color']
                    else:
                        teamUniformColor = ''
                    if 'alternateColor' in team['uniform']:
                        teamUniformAlternateColor = team['uniform']['alternateColor']
                    else:
                        teamUniformType = ''
                        teamUniformColor = ''
                        teamUniformAlternateColor = ''
                else:
                    teamUniformType = ''
                    teamUniformColor = ''
                    teamUniformAlternateColor = ''
                teamOrder = iOrder
                team['leagueId'] = leagueId
                team['updateTime'] = updateTime
                tmpTeam = {'id': teamId,
                           'uid': teamUid,
                           'slug': teamSlug,
                           'location': teamLocation,
                           'name': teamName,
                           'abbreviation': teamAbbreviation,
                           'displayName': teamShortDisplayName,
                           'shortDisplayName': teamShortDisplayName,
                           'logo': teamLogo}
                teamUniformDB[id][teamOrder] = {'id': id,
                                                'teamId': teamId,
                                                'teamOrder': teamOrder,
                                                'teamColor': teamColor,
                                                'teamAlternateColor': teamAlternateColor,
                                                'uniformType': teamUniformType,
                                                'uniformColor': teamUniformColor,
                                                'uniformAlternateColor': teamUniformAlternateColor}
                (seasonType, seasonTmp2, errEvent) = extractSeasonFromEvent(event)
                if teamId not in teamDB.keys():
                    teamDB[teamId] = tmpTeam
                # print("eventId", id,"leagueId",leagueId, "seasonType",seasonType)
                # print(leagueDB[leagueId][seasonType])
                if seasonType > 0 and leagueId >0:
                    if teamId not in leagueDB[leagueId][seasonType]:
                        leagueDB[leagueId][seasonType].append(teamId)
                tmpStat = {}
                tmpStat['eventId'] = id
                tmpStat['teamId'] = teamId
                tmpStat['teamOrder'] = iOrder
                tmpStat['updateTime'] = updateTime
                iOrder += 1
                if 'statistics' in item:
                    statistics = item['statistics']
                    tmpStat['hasStats'] = 'yes'
                    for stat in statistics:
                        name = stat['name']
                        label = stat['label']
                        displayValue = stat['displayValue']
                        tmpStat[name] = displayValue
                        if name not in statLabel:
                            statLabel[name] = label
                else:
                    tmpStat['hasStats'] = 'no'
                stats.append(tmpStat)
        else:
            print(description, iEvent, 'no boxscore', id, len(event), list(event.keys()))
            errEvent = {'id': id, 'boxscore': 'not in event'}
            errLog.append(errEvent)
        iEvent += 1

    leagueList2 = leagueDB1

    leagues = []
    #print("leagueList2",leagueList2)
    #print("leagueDB",leagueDB)
    for tmpLeague in leagueList2:
        #print(tmpLeague)
        tmpLeagueId = tmpLeague['id']
        if tmpLeagueId in leagueList.keys():
            # print(leagueList[tmpLeagueId]['isTournament'])
            # print(leagueList2[tmpLeagueId])
            tmpLeague['isTournament'] = leagueList[tmpLeagueId]['isTournament']
            # tmpLeague['updateTime'] = leagueList[tmpLeagueId]['updateTime']
        else:
            tmpLeague['isTournament'] = None
            # tmpLeague['updateTime'] = None
        leagues.append(tmpLeague)

    filename = saveDirBoxscore + 'leagues.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(leagues, file)
        file.close()
    print(description, 'leagues        ', filename)

    teamList = []
    for leagueId in leagueDB:
        for seasonType in leagueDB[leagueId]:
            for teamId in leagueDB[leagueId][seasonType]:
                tmpTeam = {'leagueId': leagueId,
                           'seasonType': seasonType,
                           'teamId': teamId}
                teamList.append(tmpTeam)
    filename = saveDirBoxscore + 'leagueDB.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(teamList, file)
        file.close()
    print(description, 'leagueDB       ', filename)

    uniformList = []
    for eventId in teamUniformDB:
        for nOrder in teamUniformDB[eventId]:
            uniformList.append(teamUniformDB[eventId][nOrder])
    filename = saveDirBoxscore + 'teamUniform.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(uniformList, file)
        file.close()
    print(description, 'teamUniform    ', filename)

    teams = []
    for teamId in teamDB:
        teams.append(teamDB[teamId])
    filename = saveDirBoxscore + 'teams.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(teams, file)
        file.close()
    print(description, 'teams          ', filename)

    newStats = []
    for stat in stats:
        tmpStat = {}
        for statKey in stat.keys():
            if statKey not in statLabel.keys():
                tmpStat[statKey] = stat[statKey]
        for label in statLabel:
            if label in stat.keys():
                tmpStat[label] = stat[label]
            else:
                tmpStat[label] = ''
        newStats.append(tmpStat)

    filename = saveDirBoxscore + 'teamStats.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(newStats, file)
        file.close()
    print(description, 'stats          ', filename)

    filename = saveDirBoxscore + 'statLabel.json'
    if os.path.exists(saveDirBoxscore):
        with open(filename, 'w') as file:
            json.dump(statLabel, file)
        file.close()
    print(description, 'statLabel      ', filename)
    msg = "boxscore extracted"
    return msg,errLog
def extractGameInfo(eventDir, saveDirGameInfo,eventImportDir,importedLeagues, errLog, currentTime):
    description = "Extract Gameinfo"
    venueDBGameInfo = {}
    venueEvent = {}
    officialDB = {}
    attendanceDB = {}

    iEvent = 0
    for tmpLeague in importedLeagues:
        id = tmpLeague["eventId"]
        (bFile, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not bFile:
            print(description, errEvent)
            errLog.append(errEvent)
            continue
        if 'gameInfo' in event.keys():
            gameInfo = event['gameInfo']
            # print(event['gameInfo'])
            if 'venue' in gameInfo.keys():
                venue = gameInfo['venue']
                venueId = int(venue['id'])
                if 'fullName' in venue:
                    venueFullName = venue['fullName']
                else:
                    venueFullName = ''
                if 'shortName' in venue:
                    venueShortName = venue['shortName']
                else:
                    venueShortName = ''
                venueAddressCity = ''
                venueAddressCountry = ''
                if 'address' in venue:
                    if 'city' in venue['address']:
                        venueAddressCity = venue['address']['city']
                    if 'country' in venue['address']:
                        venueAddressCountry = venue['address']['country']
                venueCapacity = 0
                if 'capacity' in venue:
                    venueCapacity = venue['capacity']
                venueImages = ''
                if 'images' in venue:
                    venueImages = venue['images']
                venueEvent[id] = {'id': venueId}
                if venueId not in venueDBGameInfo.keys():
                    venueDBGameInfo[venueId] = {'id': venueId,
                                                'fullName': venueFullName,
                                                'shortName': venueShortName,
                                                'address.city': venueAddressCity,
                                                'address.country': venueAddressCountry,
                                                'capacity': venueCapacity,
                                                'images': ''
                                                }
            else:
                print(description, iEvent, id, "venue not found")
                errLog.append({'id': id, 'venue': 'not found', 'logTime': currentTime})
            if 'attendance' in gameInfo.keys():
                attendance = gameInfo['attendance']
                attendanceDB[id] = {'attendance': attendance}
            if 'officials' in gameInfo.keys():
                officials = gameInfo['officials']
                officialDB[id] = officials
        else:
            print(description, iEvent, id, "gameInfo not found")
            errLog.append({'id': id, 'gameInfo': 'not found', 'logTime': currentTime})
        iEvent += 1

    officialsTb = []
    # print(officialDB)
    for eventId in officialDB.keys():
        tmpOfficials = officialDB[eventId]
        for tmpOfficial in tmpOfficials:
            tmpTb = {}
            tmpTb['eventId'] = eventId
            for key in tmpOfficial.keys():
                tmpTb[key] = tmpOfficial[key]
            officialsTb.append(tmpTb)

    venueTb = []
    for venueId in venueDBGameInfo.keys():
        tmpTb = {}
        tmpTb['id'] = venueId
        for key in venueDBGameInfo[venueId].keys():
            tmpTb[key] = venueDBGameInfo[venueId][key]
        venueTb.append(tmpTb)

    venueEventTb = []
    for eventId in venueEvent.keys():
        tmpTb = {}
        tmpTb['eventId'] = eventId
        # print(eventId,venueEvent[eventId])
        for key in venueEvent[eventId].keys():
            tmpTb[key] = venueEvent[eventId][key]
        venueEventTb.append(tmpTb)
        # print(venueEventTb)

    attendanceTb = []
    for eventId in attendanceDB.keys():
        tmpTb = {}
        tmpTb['eventId'] = eventId
        for key in attendanceDB[eventId].keys():
            tmpTb[key] = attendanceDB[eventId][key]
        attendanceTb.append(tmpTb)

    # print('Write GameInfo DB...')

    filename = saveDirGameInfo + 'venueTb.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(venueTb, file)
        file.close()
    print(description, 'venue table    ', filename)

    filename = saveDirGameInfo + 'venue.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(venueEventTb, file)
        file.close()
    print(description, 'venue events   ', filename)

    filename = saveDirGameInfo + 'officials.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(officialsTb, file)
        file.close()
    print(description, 'officials      ', filename)

    filename = saveDirGameInfo + 'attendance.json'
    if os.path.exists(saveDirGameInfo):
        with open(filename, 'w') as file:
            json.dump(attendanceTb, file)
        file.close()
    print(description, 'attendance     ', filename)
    msg = "gameinfo extracted"
    return msg, errLog
def extractKeyEventsAndCommentary(eventDir,saveDirKeyEvents,eventImportDir,importedLeagues,errLog,currentTime):
    description = "Extract Keyevents and Commentary"
    keyEventDB = {}
    commentaryDB = []
    playDB = {}
    keyEventTypeDB = {}
    keyEventSourceDB = {}
    keyEventParticipantsDB = {}
    playParticipantsDB = {}

    iEvent = 0
    for tmpLeague in importedLeagues:
        id = tmpLeague["eventId"]
        keyEventDB[id] = {}
        playDB[id] = {}
        keyEventParticipantsDB[id] = {}
        playParticipantsDB[id] = {}
        (bFile, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not bFile:
            print(description, errEvent)
            errLog.append(errEvent)
            continue
        if 'keyEvents' in event:
            if 'boxscore' in event:
                teamDisplayName1 = event['boxscore']['teams'][0]['team']['displayName']
                teamId1 = event['boxscore']['teams'][0]['team']['id']
                teamDisplayName2 = event['boxscore']['teams'][1]['team']['displayName']
                teamId2 = event['boxscore']['teams'][1]['team']['id']
                teamNameDict = {teamDisplayName1: teamId1, teamDisplayName2: teamId2}
                keyEvents = event['keyEvents']
                #iKeyEvents = 0
                nOrder = 0
                for keyEvent in keyEvents:
                    nOrder += 1
                    (keyEventId, tmpKeyEvent, tmpKeyEventType, tmpKeyEventParticipants, tmpKeyEventSourceDB) \
                        = extractPlay(id, nOrder, keyEvent, teamNameDict)
                    if keyEventId not in keyEventDB[id].keys():
                        keyEventDB[id][keyEventId] = tmpKeyEvent
                        tmpId = tmpKeyEventType['id']
                        tmpText = tmpKeyEventType['text']
                        if tmpId not in keyEventTypeDB.keys():
                            keyEventTypeDB[tmpId] = {'id': tmpId, 'text': tmpText}
                        if len(tmpKeyEventParticipants) >= 1:
                            keyEventParticipantsDB[id][keyEventId] = []
                            for keyEventParticipant in tmpKeyEventParticipants:
                                keyEventParticipantsDB[id][keyEventId].append(keyEventParticipant)
                        for tmpKeyEventSourceId in tmpKeyEventSourceDB.keys():
                            if tmpKeyEventSourceId not in keyEventSourceDB.keys():
                                keyEventSourceDB[tmpKeyEventSourceId] = tmpKeyEventSourceDB[tmpKeyEventSourceId]
            else:
                print(description, iEvent, id, 'no boxscore')
                errEvent = {'id': id, 'key events': 'no boxscore'}
                errLog.append(errEvent)
        else:
            print(description, iEvent, id, 'no key events')
            errEvent = {'id': id, 'key events': 'not found'}
            errLog.append(errEvent)
        if 'commentary' in event:
            commentaries = event['commentary']
            iCommentary = 0
            nPlay = 0
            for commentary in commentaries:
                # print(commentary)
                tmpCommentary = {}
                if 'sequence' in commentary:
                    nOrder = int(commentary['sequence'])
                else:
                    nOrder = iCommentary
                timeValue = commentary['time']['value']
                timeDisplayValue = commentary['time']['displayValue']
                commentaryText = commentary['text']
                tmpCommentary['eventId'] = id
                tmpCommentary['sequence'] = nOrder
                tmpCommentary['timeValue'] = timeValue
                tmpCommentary['timeDisplayValue'] = timeDisplayValue
                tmpCommentary['text'] = commentaryText
                if 'play' in commentary.keys():
                    if 'id' in commentary['play']:
                        tmpPlayId = commentary['play']['id']
                        if tmpPlayId not in playDB[id].keys():
                            nPlay += 1
                            (playId, tmpPlay, tmpPlayType, tmpPlayParticipants, tmpPlaySourceDB) \
                                = extractPlay(id, nPlay, commentary['play'], teamNameDict)
                            playDB[id][playId] = tmpPlay
                            tmpId = tmpPlayType['id']
                            tmpText = tmpPlayType['text']
                            if tmpId not in keyEventTypeDB.keys():
                                keyEventTypeDB[tmpId] = {'id': tmpId, 'text': tmpText}
                            if len(tmpPlayParticipants) >= 1:
                                playParticipantsDB[id][playId] = []
                                for playParticipant in tmpPlayParticipants:
                                    playParticipantsDB[id][playId].append(playParticipant)
                            for tmpPlaySourceId in tmpPlaySourceDB.keys():
                                if tmpPlaySourceId not in keyEventSourceDB.keys():
                                    keyEventSourceDB[tmpPlaySourceId] = tmpPlaySourceDB[tmpPlaySourceId]
                        tmpCommentary['id'] = tmpPlayId
                    else:
                        tmpCommentary['id'] = ''
                else:
                    tmpCommentary['id'] = ''
                commentaryDB.append(tmpCommentary)
                iCommentary += 1
        else:
            print(description, iEvent, id, 'no commentary')
            errEvent = {'id': id, 'commentary': 'not found'}
            errLog.append(errEvent)
        iEvent += 1

    len1 = 0
    combinedPlayDB = {}
    combinedParticipantsDB = {}
    for id in playDB:
        combinedPlayDB[id] = {}
        combinedParticipantsDB[id] = {}
        combinedPlayList = {}
        combinedParticipantsList = {}
        for playId in playDB[id]:
            combinedPlayList[playId] = playDB[id][playId]
            if playId in playParticipantsDB[id].keys():
                combinedParticipantsList[playId] = playParticipantsDB[id][playId]
        for keyEventId in keyEventDB[id]:
            combinedPlayList[keyEventId] = keyEventDB[id][keyEventId]
            if keyEventId in keyEventParticipantsDB[id].keys():
                combinedParticipantsList[keyEventId] = keyEventParticipantsDB[id][keyEventId]
        tmpCombinedPlayList = []
        for playId in combinedPlayList.keys():
            tmpCombinedPlayList.append(combinedPlayList[playId])
        # print(tmpCombinedPlayList)
        sortedCombinedPlayList = sorted(tmpCombinedPlayList, key=lambda d: (d['period'], d['clockValue']))
        for sortedCombinedPlay in sortedCombinedPlayList:
            playId = sortedCombinedPlay['id']
            combinedPlayDB[id][playId] = sortedCombinedPlay
            if playId in combinedParticipantsList.keys():
                combinedParticipantsDB[id][playId] = combinedParticipantsList[playId]

    # print(keyEventDB)
    keyEventDBList = []
    for id in keyEventDB:
        for keyEventId in keyEventDB[id]:
            keyEventDBList.append(keyEventDB[id][keyEventId])

    filename = saveDirKeyEvents + 'keyEvent.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventDBList, file)
        file.close()
    print(description, 'key events             ', filename)

    filename = saveDirKeyEvents + 'commentary.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(commentaryDB, file)
        file.close()
    print(description, 'commentary             ', filename)

    playDBList = []
    for id in playDB:
        for playId in playDB[id]:
            playDBList.append(playDB[id][playId])

    filename = saveDirKeyEvents + 'plays.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(playDBList, file)
        file.close()

    combinedPlayDBList = []
    for id in combinedPlayDB:
        for playId in combinedPlayDB[id]:
            combinedPlayDBList.append(combinedPlayDB[id][playId])
    print(description, 'plays                  ', filename)

    filename = saveDirKeyEvents + 'combinedPlays.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(combinedPlayDBList, file)
        file.close()
    print(description, 'combined plays         ', filename)

    filename = saveDirKeyEvents + 'keyEventType.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventTypeDB, file)
        file.close()
    print(description, 'key events type        ', filename)

    filename = saveDirKeyEvents + 'keyEventSource.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventSourceDB, file)
        file.close()

    keyEventParticipantsDBList = []
    for id in keyEventParticipantsDB:
        for keyEventId in keyEventParticipantsDB[id]:
            for keyEventParticipant in keyEventParticipantsDB[id][keyEventId]:
                keyEventParticipantsDBList.append(keyEventParticipant)
    print(description, 'key events source      ', filename)

    filename = saveDirKeyEvents + 'keyEventParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(keyEventParticipantsDBList, file)
        file.close()

    playParticipantsDBList = []
    for id in playParticipantsDB:
        for playId in playParticipantsDB[id]:
            for playParticipant in playParticipantsDB[id][playId]:
                playParticipantsDBList.append(playParticipant)
    print(description, 'key events participants', filename)

    filename = saveDirKeyEvents + 'playParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(playParticipantsDBList, file)
        file.close()
    print(description, 'play participants      ', filename)

    combinedParticipantsDBList = []
    for id in combinedParticipantsDB:
        for playId in combinedParticipantsDB[id]:
            for playParticipant in combinedParticipantsDB[id][playId]:
                combinedParticipantsDBList.append(playParticipant)

    filename = saveDirKeyEvents + 'combinedParticipants.json'
    if os.path.exists(saveDirKeyEvents):
        with open(filename, 'w') as file:
            json.dump(combinedParticipantsDBList, file)
        file.close()
    print(description, 'combined participants  ', filename)
    msg = "keyevents and commentary extracted"
    return (msg,errLog)
def extractOdds(eventDir,saveDirOdds,eventImportDir,importedLeagues,errLog,currentTime):
    description = "Extract Odds"
    oddsProviderList=[]
    oddsProviderDB=[]
    oddsDB=[]
    bettingOddsDB=[]
    playerOddsDB=[]
    bUpdateOdds = False
    nPlayerOdds=0
    iEvent=0
    for tmpLeague in importedLeagues:
        id = tmpLeague["eventId"]
        (bFile, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not bFile:
            print(description, errEvent)
            errLog.append(errEvent)
            continue
        #print(id,len(event[id]),list(event[id].keys()))
        #print(event[id]['hasOdds'])
        updateTime=event['updateTime']
        hasOdds = 'odds' in event.keys() and event['odds']
        if hasOdds:
            odds=event['odds']
            iOdds = 0
            for odd in odds:
                oddsPerProvider = {}
                if 'links' in odd:
                    odd.pop('links')
                providerId=odd['provider']['id']
                providerName=odd['provider']['name']
                providerPriority=odd['provider']['priority']
                oddsProvider={'id':providerId,
                              'name':providerName,
                              'priority':providerPriority}
                overUnder=odd['provider']['id']
                #if str(id) == '604729' and str(providerId) == '2000':
                #    print(id,providerId,odd)
                if ('awayTeamOdds' in odd and odd['awayTeamOdds']) or \
                        ('homeTeamOdds' in odd and odd['homeTeamOdds']):
                    oddsPerProvider['oddId']=str(id)+'-'+str(providerId)
                    oddsPerProvider['providerId'] = providerId
                    oddsPerProvider['index'] = 0
                    oddsPerProvider['eventId'] = id
                    oddsPerProvider['updateTime'] = updateTime
                else:
                    print(description, odd)
                if 'awayTeamOdds' in odd and odd['awayTeamOdds']:
                    tmpOdds=odd['awayTeamOdds']
                    (value,key)=oddsValue(tmpOdds,'favorite','awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'underdog','awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'moneyLine','awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'team','awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'spreadOdds', 'awayTeam')
                    if value:
                        oddsPerProvider[key] = value
                    if 'odds' in tmpOdds and tmpOdds['odds']:
                        tmpOdds2 = tmpOdds['odds']
                        if 'summary' in tmpOdds2:
                            oddsSummary=tmpOdds2['summary']
                        else:
                            print(description, iOdds,nPlayerOdds,id,odd)
                        oddsValue1 = tmpOdds2['value']
                        oddsHandicap = tmpOdds2['handicap']
                        oddsPerProvider['oddsSummary'] = oddsSummary
                        oddsPerProvider['oddsValue'] = oddsValue1
                        oddsPerProvider['oddsHandicap'] = oddsHandicap
                if 'homeTeamOdds' in odd and odd['homeTeamOdds']:
                    tmpOdds=odd['homeTeamOdds']
                    (value,key)=oddsValue(tmpOdds,'favorite','homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'underdog','homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'moneyLine','homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value,key)=oddsValue(tmpOdds,'team','homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(tmpOdds, 'spreadOdds', 'homeTeam')
                    if value:
                        oddsPerProvider[key] = value
                    (value, key) = oddsValue(odd, 'details', '')
                    if value:
                         oddsPerProvider[key] = value
                    (value, key) = oddsValue(odd, 'spread', '')
                    if value:
                        oddsPerProvider[key] = value
                    if 'odds' in tmpOdds and tmpOdds['odds']:
                        tmpOdds2 = tmpOdds['odds']
                        if 'summary' in tmpOdds2:
                            oddsSummary = tmpOdds2['summary']
                        else:
                            print(description, iOdds,nPlayerOdds,id,odd)
                        oddsValue1 = tmpOdds2['value']
                        oddsHandicap = tmpOdds2['handicap']
                        oddsPerProvider['oddsSummary'] = oddsSummary
                        oddsPerProvider['oddsValue'] = oddsValue1
                        oddsPerProvider['oddsHandicap'] = oddsHandicap
                if 'drawOdds'  in odd and odd['drawOdds']:
                    (value, key) = oddsValue(odd, 'drawOdds', '')
                    if value:
                        if 'moneyLine' in value:
                            key1 = key+'MoneyLine'
                            oddsPerProvider[key1] = value['moneyLine']
                bBettingOdds = False
                bPlayerOdds = False
                if 'bettingOdds' in odd and odd['bettingOdds']:
                    bBettingOdds = True
                    tmpBettingOdds=odd['bettingOdds']
                    homeTeamUrl=tmpBettingOdds['homeTeam']['$ref']
                    awayTeamUrl=tmpBettingOdds['awayTeam']['$ref']
                    bettingOddsPerProvider = {}
                    bettingOddsPerProvider['oddId'] = str(id) + '-' + str(providerId)
                    bettingOddsPerProvider['providerId'] = providerId
                    bettingOddsPerProvider['index'] = 0
                    bettingOddsPerProvider['eventId'] = id
                    bettingOddsPerProvider['updateTime'] = updateTime
                    if 'teamOdds' in tmpBettingOdds and tmpBettingOdds['teamOdds']:
                        tmpTeamOdds = tmpBettingOdds['teamOdds']
                        for key in tmpTeamOdds:
                            oddId=tmpTeamOdds[key]['oddId']
                            value=tmpTeamOdds[key]['value']
                            betSlipUrl=tmpTeamOdds[key]['betSlipUrl']
                            keyOddId = key + 'OddId'
                            keyValue = key + 'Value'
                            keyBetSlipUrl = key + 'BetSlipUrl'
                            bettingOddsPerProvider[keyOddId] = oddId
                            bettingOddsPerProvider[keyValue] = value
                            bettingOddsPerProvider[keyBetSlipUrl] = betSlipUrl
                    if 'playerOdds' in tmpBettingOdds and tmpBettingOdds['playerOdds']:
                        bPlayerOdds = True
                        tmpPlayerOdds = tmpBettingOdds['playerOdds']
                        tmpPlayerOddsList= []
                        tmpPlayerOddsComb= {}
                        #print(id,tmpPlayerOdds)
                        for key0 in tmpPlayerOdds:
                            players=tmpPlayerOdds[key0]
                            for player in players:
                                #print(nPlayerOdds,player)
                                if 'oddId' in player:
                                    oddId=player['oddId']
                                else:
                                    oddId='none'
                                if 'value' in player:
                                    value=player['value']
                                else:
                                    value='none'
                                if 'betSlipUrl' in player:
                                    betSlipUrl=player['betSlipUrl']
                                else:
                                    betSlipUrl='none'
                                playerName=player['player']
                                keyOddId = key0 + 'OddId'
                                keyValue = key0 + 'Value'
                                keyBetSlipUrl = key0 + 'BetSlipUrl'
                                keyPlayer = key0 + 'Player'
                                if playerName not in tmpPlayerOddsComb.keys():
                                    tmpPlayerOddsComb[playerName] = {}
                                    tmpPlayerOddsComb[playerName]['oddId'] = str(id) + '-' + str(providerId)
                                    tmpPlayerOddsComb[playerName]['providerId'] = providerId
                                    tmpPlayerOddsComb[playerName]['index'] = 0
                                    tmpPlayerOddsComb[playerName]['eventId'] = id
                                    tmpPlayerOddsComb[playerName]['player'] = playerName
                                    tmpPlayerOddsComb[playerName]['updateTime'] = updateTime
                                tmpPlayerOddsComb[playerName][keyOddId] = oddId
                                tmpPlayerOddsComb[playerName][keyValue] = value
                                tmpPlayerOddsComb[playerName][keyBetSlipUrl] = betSlipUrl
                                tmpPlayerOddsComb[playerName][keyPlayer] = playerName
                        for playerName in tmpPlayerOddsComb:
                            nPlayerOdds += 1
                            tmpPlayerOddsList.append(tmpPlayerOddsComb[playerName])
                iOdds += 1
                if providerId not in oddsProviderList:
                    oddsProviderList.append(providerId)
                    oddsProviderDB.append(oddsProvider)
                oddsDB.append(oddsPerProvider)
                if bBettingOdds:
                    bettingOddsDB.append(bettingOddsPerProvider)
                if bPlayerOdds:
                    playerOddsDB.append(tmpPlayerOddsList)
        else:
            print(description, iEvent,id,"no odds in event")
            errEvent = {'id':id,'odds': 'no odds in event', 'logTime': currentTime}
            errLog.append(errEvent)
        iEvent += 1
    print()

    if bUpdateOdds:
        (oddsProviderDB, oddsDB, err) = readOddsDB(saveDirOdds)

        for odds in oddsDB:
            eventId = odds['id']

    err = writeOddsDB(oddsProviderDB, oddsDB, bettingOddsDB, playerOddsDB, saveDirOdds)
    print(description, err)
    msg = "odds extraction complete"
    return msg,errLog

def extractRoster(eventDir,saveDirRoster,eventImportDir,importedLeagues,errLog,currentTime):
    description = "Extract Roster"
    athleteDB = {}
    playerStats = []
    playerPlays = []
    statTypes = {}
    positionTypes = {}
    teams = []
    teamRoster = []
    iEvent = 0
    nTotEvents = len(importedLeagues)
    print(description,"nTotal Events",nTotEvents)
    for tmpLeague in importedLeagues:
        id = tmpLeague["eventId"]
        (hasEvent, event, errEvent) = openEvent(id, eventDir,eventImportDir)
        if not hasEvent:
            print(description, errEvent)
            errLog.append(errEvent)
            continue
        else:
            if "updateTime" in event:
                updateTime = event['updateTime']
            else:
                updateTime = "2019-07-01T05:00:00Z"
            # print(id,len(event[id]),list(event[id].keys()))
            if 'rosters' in event:
                # print(id,len(event[id]),event['rosters'])
                items = event['rosters']
                for item in items:
                    # print(id, len(items), list(item.keys()))
                    tmpTeam = {}
                    tmpPlayerPlaysTeam = []
                    nOrder = 0
                    teamId = 'none'
                    teamAbbreviation = ''
                    teamDisplayName = ''
                    teamColor = ''
                    teamAlternateColor = ''
                    formation = ""
                    homeAway = ""
                    winner = ""
                    # for key in item.keys():
                        # print(key)
                    if 'team' in item:
                        team = item['team']
                        # print('team',team)
                        if 'id' in team.keys():
                            teamId = team['id']
                        if 'abbreviation' in team.keys():
                            teamAbbreviation = team['abbreviation']
                        if 'displayName' in team.keys():
                            teamDisplayName = team['displayName']
                        if 'color' in team.keys():
                            teamColor = team['color']
                        if 'alternateColor' in team.keys():
                            teamAlternateColor = team['alternateColor']
                        tmpTeam['eventId'] = id
                        tmpTeam['teamId'] = teamId
                        tmpTeam['teamAbbreviation'] = teamAbbreviation
                        tmpTeam['teamDisplayName'] = teamDisplayName
                        tmpTeam['teamColor'] = teamColor
                        tmpTeam['teamAlternateColor'] = teamAlternateColor
                    if 'homeAway' in item:
                        homeAway = item['homeAway']
                        # print('homeAway',homeAway)
                        tmpTeam['homeAway'] = homeAway
                    if 'winner' in item:
                        winner = item['winner']
                        tmpTeam['winner'] = winner
                        # print('winner',winner)
                    uniformType = ""
                    uniformColor = ""
                    if 'uniform' in item:
                        uniform = item['uniform']
                        if 'type' in uniform:
                            uniformType = uniform['type']
                        if 'color' in uniform:
                            uniformColor = uniform['color']
                        # print('uniform',uniform)
                        tmpTeam['uniform'] = uniform
                    if 'formation' in item:
                        formation = item['formation']
                        tmpTeam['formation'] = formation
                        # print('formation',formation)
                    if 'roster' in item:
                        rosters = item['roster']
                        i = 0
                        for roster in rosters:
                            tmpPlayerStat = {}
                            tmpPlayerPlays = {}
                            tmpTeamRoster = {}
                            hasPlays = 'no'
                            hasStats = 'no'
                            # print(len(roster),roster)
                            i += 1
                            tmpPlayerStat['eventId'] = id
                            tmpPlayerStat['teamId'] = teamId
                            tmpPlayerPlays['eventId'] = id
                            tmpPlayerPlays['teamId'] = teamId
                            tmpPlayerPlays['homeAway'] = homeAway
                            tmpTeamRoster['eventId'] = id
                            tmpTeamRoster['teamId'] = teamId
                            tmpTeamRoster['uniformType'] = uniformType
                            tmpTeamRoster['uniformColor'] = uniformColor
                            tmpTeamRoster['homeAway'] = homeAway
                            tmpTeamRoster['winner'] = winner
                            tmpTeamRoster['formation'] = formation
                            if 'athlete' in roster:
                                athlete = roster['athlete']
                                athlete['updateTime'] = updateTime
                                # print(i,athlete.keys())
                                if 'links' in athlete.keys():
                                    athlete.pop('links')
                                if 'id' in athlete:
                                    athleteId = athlete['id']
                                    athleteDisplayName = athlete['displayName']
                                    tmpPlayerStat['athleteId'] = athleteId
                                    tmpPlayerStat['athleteDisplayName'] = athleteDisplayName
                                    tmpPlayerPlays['athleteId'] = athleteId
                                    tmpPlayerPlays['athleteDisplayName'] = athleteDisplayName
                                    tmpTeamRoster['athleteId'] = athleteId
                                    tmpTeamRoster['athleteDisplayName'] = athleteDisplayName
                                    # print(i,athleteDisplayName)
                                    if athleteId not in athleteDB.keys():
                                        athleteDB[athleteId] = athlete
                                    else:
                                        athleteUpdateTimeOld = datetime.strptime(
                                            athleteDB[athleteId]['updateTime'], '%Y-%m-%dT%H:%M:%SZ')
                                        athleteUpdateTimeNew = datetime.strptime(
                                            athlete['updateTime'], '%Y-%m-%dT%H:%M:%SZ')
                                        if athleteUpdateTimeNew > athleteUpdateTimeOld:
                                            athleteDB[athleteId] = athlete
                                else:
                                    print(description, athlete)
                            if 'active' in roster:
                                active = roster['active']
                                tmpTeamRoster['active'] = active
                                # print(i,'roster', key1,active)
                            if 'starter' in roster:
                                starter = roster['starter']
                                tmpTeamRoster['starter'] = starter
                                # print(i,'roster', key1, starter)
                            if 'jersey' in roster:
                                jersey = roster['jersey']
                                tmpTeamRoster['jersey'] = jersey
                                # print(i,'roster', key1, jersey)
                            if 'position' in roster:
                                position = roster['position']
                                name = position['name']
                                displayName = position['displayName']
                                abbreviation = position['abbreviation']
                                tmpTeamRoster['position'] = name
                                if name not in positionTypes:
                                    positionTypes[name] = {'name': name,
                                                           'displayName': displayName,
                                                           'abbreviation': abbreviation}
                                # print(i,'roster', key1, position)
                            if 'formationPlace' in roster:
                                formationPlace = roster['formationPlace']
                                tmpTeamRoster['formationPlace'] = formationPlace
                                # print(i,'roster', key1, formationPlace)
                            else:
                                tmpTeamRoster['formationPlace'] = 0
                            if 'subbedOutFor' in roster:
                                subbedOutFor = roster['subbedOutFor']
                                subbedOutForAthleteJersey = -1
                                if 'jersey' in subbedOutFor.keys():
                                    subbedOutForAthleteJersey = int(subbedOutFor['jersey'])
                                subbedOutForAthleteId = 0
                                if 'athlete' in subbedOutFor.keys():
                                    subbedOutForAthleteId = int(subbedOutFor['athlete']['id'])
                                tmpTeamRoster['subbedOutForAthleteId'] = subbedOutForAthleteId
                                tmpTeamRoster['subbedOutForAthleteJersey'] = subbedOutForAthleteJersey
                                # print(i,'roster', 'subbedOutFor', subbedOutFor)
                                # print(i,'roster', key1, subbedOutForAthleteId,subbedOutForAthleteJersey)
                            if 'subbedInFor' in roster:
                                subbedInFor = roster['subbedInFor']
                                subbedInForAthleteJersey = -1
                                if 'jersey' in subbedInFor.keys():
                                    subbedInForAthleteJersey = int(subbedInFor['jersey'])
                                subbedInForAthleteId = 0
                                if 'athlete' in subbedInFor.keys():
                                    subbedInForAthleteId = int(subbedInFor['athlete']['id'])
                                tmpTeamRoster['subbedInForAthleteId'] = subbedInForAthleteId
                                tmpTeamRoster['subbedInForAthleteJersey'] = subbedInForAthleteJersey
                                # print(i,'roster', 'subbedInFor', subbedInFor)
                                # print(i,'roster', key1, subbedInForAthleteId,subbedInForAthleteJersey)
                            if 'subbedIn' in roster:
                                subbedIn = roster['subbedIn']
                                tmpTeamRoster['subbedIn'] = subbedIn
                                # print(i,'roster', key1, subbedIn)
                            if 'subbedOut' in roster:
                                subbedOut = roster['subbedOut']
                                tmpTeamRoster['subbedOut'] = subbedOut
                                # print(i,'roster', key1, subbedOut)
                            if 'plays' in roster:
                                hasPlays = 'yes'
                                tmpPlayerPlays['hasPlays'] = hasPlays
                                tmpTeamRoster['hasPlays'] = hasPlays
                                plays = roster['plays']
                                for play in plays:
                                    for key2 in play.keys():
                                        if key2 == 'clock':
                                            tmpPlayerPlays['clockDisplayValue'] = play['clock']['displayValue']
                                            tmpPlayerPlays['clockValue'] = (
                                                clockDisplay2Value(play['clock']['displayValue']))
                                            tmpPlayerPlays['order'] = nOrder
                                        else:
                                            tmpPlayerPlays[key2] = play[key2]
                                    tmpPlayerPlays['updateTime'] = currentTime
                                    tmpPlayerPlaysTeam.append(tmpPlayerPlays.copy())
                                    nOrder += 1
                            if 'stats' in roster:
                                hasStats = 'yes'
                                stats = roster['stats']
                                j = 0
                                for stat in stats:
                                    j += 1
                                    # print(i,j,athleteDisplayName,stat)
                                    name = stat['name']
                                    displayName = stat['displayName']
                                    shortDisplayName = stat['shortDisplayName']
                                    description = stat['description']
                                    abbreviation = stat['abbreviation']
                                    value = stat['value']
                                    displayValue = stat['displayValue']
                                    if name not in statTypes:
                                        statTypes[name] = {'name': name,
                                                           'displayName': displayName,
                                                           'shortDisplayName': shortDisplayName,
                                                           'description': description,
                                                           'abbreviation': abbreviation}
                                    tmpPlayerStat[name] = value
                            tmpPlayerStat['hasStats'] = hasStats
                            tmpPlayerStat['updateTime'] = currentTime
                            tmpTeamRoster['hasStats'] = hasStats
                            tmpTeamRoster['hasPlays'] = hasPlays
                            tmpTeamRoster['updateTime'] = currentTime
                            # if id =='602885':
                            #    print('hasStats',hasStats)
                            #    print('hasPlays',hasPlays)
                            #    print(id, teamId, athleteId, athleteDisplayName, list(roster.keys()))
                            #    print(tmpTeamRoster)
                            # print(tmpTeamRoster)
                            if hasStats == 'yes':
                                playerStats.append(tmpPlayerStat)
                            #    print(id,teamId,athleteId,athleteDisplayName,list(roster.keys()))
                            teamRoster.append(tmpTeamRoster)
                    tmpPlayerPlaysTeamSorted = sorted(tmpPlayerPlaysTeam, key=lambda k: (k['clockValue'], k['order']))
                    nOrder = 0
                    for tmpPlayerPlaysItem in tmpPlayerPlaysTeamSorted:
                        tmpPlayerPlaysItem['order'] = nOrder
                        playerPlays.append(tmpPlayerPlaysItem)
                        nOrder += 1
                    tmpTeam['updateTime'] = currentTime
                    teams.append(tmpTeam)
            else:
                print(description, iEvent,id,"no roster")
                errEvent = {'id': id, 'roster': 'no roster'}
                errLog.append(errEvent)
        iEvent += 1
        if int(iEvent/1000)*1000 == iEvent or iEvent == nTotEvents:
            print(description, "processed event", iEvent, "of",nTotEvents)

    filename = saveDirRoster + 'playerStats.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(playerStats, file)
        file.close()
    print(description, 'player stats ',filename)

    filename = saveDirRoster + 'playerPlays.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(playerPlays, file)
        file.close()
    print(description, 'player plays ',filename)

    filename = saveDirRoster + 'teamRoster.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(teamRoster, file)
        file.close()
    print(description, 'teamRoster   ',filename)

    filename = saveDirRoster + 'teams.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(teams, file)
        file.close()
    print(description, 'teams        ',filename)

    filename = saveDirRoster + 'statsTypes.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(statTypes, file)
        file.close()
    print(description, 'statsTypes   ',filename)

    filename = saveDirRoster + 'positionTypes.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(positionTypes, file)
        file.close()
    print(description, 'positionTypes',filename)

    athleteList = []
    for id in athleteDB.keys():
        athleteList.append(athleteDB[id])

    filename = saveDirRoster + 'athletes.json'
    if os.path.exists(saveDirRoster):
        with open(filename, 'w') as file:
            json.dump(athleteList, file)
        file.close()
    print(description, 'athlete      ',filename)

    msg="roster extracted"
    return msg, errLog
def extractTeamRoster(teamsDir,saveDirTeams,importedLeagues,errLog,currentTime):
    description = "Extract Team Roster"
    seasons = {}
    for tmpLeague in importedLeagues:
        tmpSeasonType = tmpLeague['seasonType']
        tmpLeagueid = tmpLeague['leagueId']
        tmpMidsizeName = tmpLeague['midsizeName']
        if tmpSeasonType not in seasons:
            seasons[tmpSeasonType] = {'leagueId':tmpLeagueid,
                                         'midsizeName':tmpMidsizeName}
    i = 0
    j = 0
    playerDB = []
    playerStatDB = []
    playerInTeamDB = []
    positionDict = {}
    for tmpSeasonType in seasons:
        tmpMidsizeName = seasons[tmpSeasonType]['midsizeName']
        (err,team,errEvent) = openTeamRoster(tmpSeasonType,tmpMidsizeName,teamsDir)
        if err == 0:
            for player in team:
                tmpPlayerStat = {}
                tmpPlayerInTeam = {}
                playerId = player['id']
                playerUid = player['uid']
                playerDisplayName = player['displayName']
                if 'guid' in player:
                    playerGuid = player['guid']
                else:
                    playerGuid = ""
                if 'jersey' in player:
                    jersey = player['jersey']
                else:
                    jersey = -1
                if 'position' in player:
                    position = player['position']
                    positionId = position['id']
                    if 'leaf' in position:
                        position.pop('leaf')
                    if positionId not in positionDict.keys():
                        positionDict[positionId] = position
                else:
                    positionId = -1
                seasonYear = player['seasonYear']
                seasonType = player['seasonType']
                seasonName = player['seasonName']
                league = player['league']
                teamId = player['teamId']
                teamName = player['teamName']
                playerIndex = player['index']
                timestamp = player['timestamp']
                hasStats = 'no'
                tmpPlayerInTeam['athleteId'] = playerId
                tmpPlayerInTeam['teamId'] = teamId
                tmpPlayerInTeam['seasonType'] = seasonType
                tmpPlayerInTeam['seasonYear'] = seasonYear
                tmpPlayerInTeam['seasonName'] = seasonName
                tmpPlayerInTeam['league'] = league
                tmpPlayerInTeam['teamName'] = teamName
                tmpPlayerInTeam['playerIndex'] = playerIndex
                tmpPlayerInTeam['playerDisplayName'] = playerDisplayName
                tmpPlayerInTeam['jersey'] = jersey
                tmpPlayerInTeam['positionId'] = positionId
                tmpPlayerInTeam['hasStats'] = False
                tmpPlayerInTeam['timestamp'] = timestamp
                if 'links' in player.keys():
                    player.pop('links')
                if 'statistics' in player.keys():
                    if player['statistics']:
                        hasStats = 'yes'
                        tmpPlayerStat['id'] = playerId
                        tmpPlayerStat['uid'] = playerUid
                        tmpPlayerStat['guid'] = playerGuid
                        tmpPlayerStat['league'] = league
                        tmpPlayerStat['teamId'] = teamId
                        tmpPlayerStat['index'] = playerIndex
                        tmpPlayerStat['seasonYear'] = seasonYear
                        tmpPlayerStat['seasonType'] = seasonType
                        tmpPlayerStat['seasonName'] = seasonName
                        tmpPlayerStat['timestamp'] = timestamp
                        for key in player['statistics'].keys():
                            tmpPlayerStat[key] = player['statistics'][key]
                    player.pop('statistics')
                playerDB.append(player)
                if hasStats == 'yes':
                    tmpPlayerInTeam['hasStats'] = True
                    playerStatDB.append(tmpPlayerStat)
                playerInTeamDB.append(tmpPlayerInTeam)
                i += 1
            j += 1
        else:
            print(description, "team empty",tmpSeasonType,tmpMidsizeName)
            errEvent = {'id':'','seasonType': tmpSeasonType, 'teamRoster': 'team empty'}
            errLog.append(errEvent)

    filename = saveDirTeams + 'playerDB.json'
    with open(filename, 'w') as file:
        json.dump(playerDB, file)
    file.close()
    print(description, 'playDB         ',filename)

    filename = saveDirTeams + 'playerInTeamDB.json'
    with open(filename, 'w') as file:
        json.dump(playerInTeamDB, file)
    file.close()
    print(description, 'playerInTeamDB ',filename)

    filename = saveDirTeams + 'positionDB.json'
    with open(filename, 'w') as file:
        json.dump(positionDict, file)
    file.close()
    print(description, 'positionDB     ',filename)

    filename = saveDirTeams + 'playerStatDB.json'
    with open(filename, 'w') as file:
        json.dump(playerStatDB, file)
    file.close()
    print(description, 'playStatDB     ',filename)

    msg = "team roster extracted"
    return msg,errLog
def extractStandings(standingsDir,saveDirStandings,importedLeagues,errLog,currentTime):
    description = "Extract Standings"
    seasons = {}
    for tmpLeague in importedLeagues:
        tmpSeasonType = tmpLeague['seasonType']
        tmpLeagueid = tmpLeague['leagueId']
        tmpYear = tmpLeague['year']
        tmpMidsizeName = tmpLeague['midsizeName']
        if tmpSeasonType not in seasons:
            seasons[tmpSeasonType] = {'leagueId':tmpLeagueid,
                                      'year':tmpYear,
                                         'midsizeName':tmpMidsizeName}
    i = 0
    teamStandingsInLeague = {}
    teams = {}
    for tmpSeasonType in seasons:
        tmpLeagueId = seasons[tmpSeasonType]['leagueId']
        tmpMidsizeName = seasons[tmpSeasonType]['midsizeName']
        tmpYear = seasons[tmpSeasonType]['year']
        (err,standings,errEvent) = openStandings(tmpSeasonType,tmpYear,tmpMidsizeName,standingsDir)
        print(description, "err=", err,i, len(seasons), tmpMidsizeName, tmpYear, tmpSeasonType)
        i += 1
        if err == 0:
            # print(filename)
            # print(standings)
            if "id" in standings["sports"][0]["leagues"][0]:
                leagueId = standings["sports"][0]["leagues"][0]["id"]
            else:
                leagueId = tmpLeagueId
            if "uid" in standings["sports"][0]["leagues"][0]:
                leagueUid = standings["sports"][0]["leagues"][0]["uid"]
            else:
                leagueUid = ''
            if "leagueName" in standings["sports"][0]["leagues"][0]:
                leagueName = standings["sports"][0]["leagues"][0]["name"]
            else:
                leagueName = ''
            year = tmpYear
            if 'year' in standings["sports"][0]["leagues"][0]:
                year = standings["sports"][0]["leagues"][0]["year"]
            elif 'season' in standings["sports"][0]["leagues"][0]:
                if 'year' in standings["sports"][0]["leagues"][0]['season']:
                    year = standings["sports"][0]["leagues"][0]['season']['year']
            if 'seasonType' in standings["sports"][0]["leagues"][0]:
                typeId = standings["sports"][0]["leagues"][0]['seasonType']
            else:
                typeId = tmpSeasonType
            tmpTeams = standings["sports"][0]["leagues"][0]["teams"]
            timeStamp = standings["sports"][0]["leagues"][0]["timeStamp"]
            timeStampDT = datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%SZ")
            print(description, "typeId:", typeId, "league:", tmpMidsizeName,"no of teams in league:", len(tmpTeams))
            k = 0
            for item in tmpTeams:
                k += 1
                team = {}
                teamStanding = {}
                tmpTeam = item['team']
                tmpTeamId = tmpTeam['id']
                #tmpKeyLeagueYearTeamId = (int(leagueId), int(year), int(tmpTeamId))
                tmpKeySeasonTypeTeamId = (tmpSeasonType, int(tmpTeamId))
                bAppend = False
                # update 10/23/24:
                # change teams key from (int(leagueId), int(year), int(tmpTeamId) to tmpSeasonType
                # update 10/24/24:
                # change teams key from tmpSeasonType to int(tmpTeamId)
                if tmpKeySeasonTypeTeamId in teams:
                    # print(tmpSeasonType,tmpKeyLeagueYearTeamId,teams[tmpKeyLeagueYearTeamId])
                    if 'timeStamp' in teams[tmpKeySeasonTypeTeamId]:
                        oldTimeStamp = teams[tmpKeySeasonTypeTeamId]['timeStamp']
                        oldTimeStampDT = datetime.strptime(oldTimeStamp, "%Y-%m-%dT%H:%M:%SZ")
                        # print(oldTimeStampDT, timeStampDT)
                        if timeStampDT > oldTimeStampDT:
                            bAppend = True
                else:
                    bAppend = True
                if bAppend:
                    # if midSizeLeagueName == "ENG.1":
                    #    print(k,tmpKeyLeagueYearTeamId)
                    # print("typeId:", typeId,"year:", year, "league:", midSizeLeagueName,"team no:",k)
                    team = tmpTeam.copy()
                    team['leagueId'] = int(leagueId)
                    team['year'] = int(year)
                    team['midsizeLeagueName'] = tmpMidsizeName
                    team['seasonType'] = typeId
                    team['timeStamp'] = timeStamp
                    if 'logos' in team:
                        team.pop('logos')
                    if 'links' in team:
                        team.pop('links')
                    if 'record' in team:
                        if team['record'] != {}:
                            tmpTeamRecord = {}
                            team['hasRecord'] = True
                            teamStanding['year'] = year
                            teamStanding['leagueId'] = leagueId
                            teamStanding['midsizeLeagueName'] = tmpMidsizeName
                            teamStanding['seasonType'] = typeId
                            teamStanding['teamId'] = tmpTeamId
                            if 'items' in team['record']:
                                tmpTeamStats = team['record']['items'][0]['stats']
                                for tmpTeamStat in tmpTeamStats:
                                    tmpTeamRecord[tmpTeamStat['name']] = tmpTeamStat['value']
                            else:
                                tmpTeamRecord = team['record']
                                for tmpKey in tmpTeamRecord:
                                    teamStanding[tmpKey] = tmpTeamRecord[tmpKey]
                            teamStanding['timeStamp'] = timeStamp
                            # update 10/23/24:
                            # change teams key from (int(leagueId), int(year), int(tmpTeamId) to tmpSeason
                            # update 10/24/24:
                            # change teams key from tmpSeasonType to int(tmpTeamId)
                            teamStandingsInLeague[tmpKeySeasonTypeTeamId] = teamStanding
                        team.pop('record')
                    else:
                        team['hasRecord'] = False
                        print(description, i,k,"no record in teams",tmpTeamId, tmpSeasonType, tmpMidsizeName)
                        errEvent = {'id': '','teamId':tmpTeamId,
                                    'seasonType': tmpSeasonType,
                                    'standings': 'no record in teams'}
                        errLog.append(errEvent)
                    teams[tmpKeySeasonTypeTeamId] = team

    # print(teams[(700,2020,371)])
    # print(teams[(700,2021,371)])
    # print(teams[(700,2023,371)])

    filename = saveDirStandings + 'teamsInLeague.json'
    myTeams = []
    for tmpKey in teams.keys():
        tmpTeam = teams[tmpKey]
        myTeams.append(tmpTeam)
    with open(filename, 'w') as file:
        json.dump(myTeams, file)
    file.close()
    print(description, 'team in League          ',filename)

    filename = saveDirStandings + 'teamStandingsInLeague.json'
    myStandings = []
    for tmpKey in teamStandingsInLeague.keys():
        tmpStanding = teamStandingsInLeague[tmpKey]
        # print(description, tmpKey)
        #print(description, tmpStanding)
        myStandings.append(tmpStanding)
    with open(filename, 'w') as file:
        json.dump(myStandings, file)
    file.close()
    print(description, 'team standings in League',filename)

    msg = "standings extracted"
    return msg, errLog


def extractESPNData(dir1, dir2, importedLeagues, importLeagueFilter, start_date_Str, end_date_Str):
    description = "Extract ESPN Data"
    #dir1 = rootDir
    #dir2 = rootDir2
    eventDir = dir1 + 'events/'
    fixtureDir = dir1 + 'fixture/all/'
    teamsDir = dir1 + 'teams/'
    standingsDir = dir1 + 'standings/'
    saveDirFixture = dir2 + "tables/fixtures/"
    saveDirTable = dir2 + "tables/"
    saveDirBoxscore = dir2 + "tables/boxscore/"
    saveDirGameInfo = dir2 + "tables/gameInfo/"
    saveDirKeyEvents = dir2 + "tables/keyEvents/"
    saveDirOdds = dir2 + "tables/odds/"
    saveDirRoster = dir2 + "tables/roster/"
    saveDirTeams = dir2 + "tables/teams/"
    saveDirStandings = dir2 + "tables/standings/"
    saveDirErrLog = dir2 + "tables/"

    #eventImportDir = directory.rstrip(directory[-1])
    eventImportDir = dir1
    #indexfilename= rootDir +'events_index.csv'

    today = datetime.today()
    todayStr = datetime.strftime(today, "%Y%m%d")
    currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    dataSet, err = genDataSet(saveDirTable, start_date_Str, end_date_Str,todayStr,statusStr = "started")

    #
    # Open leaague list and generate leagueDB
    #
    (leagueList, leagueDB1, err) = extractLeagues(dir1,saveDirTable)
    #print(leagueList)
    print(description, err)
    errLog = []
    #(msg, leagueDB, errLog) = extractFixtures(dir1,dir2, importedLeagues, leagueList, errLog)
    (msg, leagueDB, errLog) = extractFixtures(eventDir,fixtureDir, saveDirFixture,saveDirTable, eventImportDir,
                                              importedLeagues, leagueList, errLog)
    print(description, msg)
    (msg,errLog) = extractBoxscore(eventDir, saveDirBoxscore, eventImportDir,
                                importedLeagues, leagueDB1, leagueDB, currentTime, errLog)
    print(description, msg)
    (msg,errLog) = extractGameInfo(eventDir,saveDirGameInfo,eventImportDir,
                                   importedLeagues,errLog,currentTime)
    print(description, msg)
    (msg,errLog) = extractKeyEventsAndCommentary(eventDir,saveDirKeyEvents,eventImportDir,
                                                 importedLeagues,errLog,currentTime)
    print(description, msg)
    (msg,errLog) = extractOdds(eventDir,saveDirOdds,eventImportDir,
                                                 importedLeagues,errLog,currentTime)
    print(description, msg)
    (msg,errLog) = extractRoster(eventDir,saveDirRoster,eventImportDir,
                               importedLeagues,errLog,currentTime)
    print(description, msg)
    (msg,errLog) = extractTeamRoster(teamsDir,saveDirTeams,
                                 importedLeagues,errLog,currentTime)
    print(description, msg)
    (msg,errLog) = extractStandings(standingsDir,saveDirStandings,
                                     importedLeagues,errLog,currentTime)
    print(description, msg)
    filename = saveDirErrLog + "errLog.json"
    with open(filename, 'w') as file:
        json.dump(errLog, file)
    file.close()
    print(description, "error logs:  ",filename)

    #for errMsg in errLog:
    #    print(errMsg)
    msg = 'extraction complete'
    return(dataSet, errLog, msg)
