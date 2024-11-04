from datetime import timedelta, date, datetime,timezone
import requests
from shutil import copyfile
from dateutil import tz
import json
import urllib
import os
from os.path import isfile, join
import sys
import csv
import time
import functools

def downloadFilefromUrl(url,filename):
    try:
        response = requests.get(url)
    except:
        print("error download file from", url)
        return(-1)
    try:
        with open(filename,'wb') as f:
            f.write(response.content)
        f.close()
    except:
        print("file save error! ", filename)
        return(-2)
    return(0)

def import_event(id, directory, bDebug=False):
    event = {}
    if bDebug == False:
        # uri="http://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/summary?event=573699"
        # uri="http://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/summary?event=576554"
        uri = "http://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/summary?event=" + str(id)
        # print (uri)
        currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        try:
            Response = requests.get(uri).json()
            Response['updateTime'] = currentTime
        except requests.exceptions.Timeout:
            code = 990
            event['code'] = code
            return(event,code)
        except requests.exceptions.ConnectionError:
            code = 980
            event['code'] = code
            return(event,code)
        except requests.exceptions.JSONDecodeError:
            code = 970
            event['code'] = code
            return (event, code)
        # print(Response.keys())

        code = 0

        if 'error' in Response:
            Response['code'] = 9999

        if 'code' in Response:
            code = Response['code']

        filename = directory + 'events/' + str(id) + '.txt'

        with open(filename, 'w') as file:
            json.dump(Response, file)
        file.close()
        event = Response
    else:
        code = 9999
        event['code'] = code
        event['detail'] = 'debugMode'
    return (event, code)
def extractLeagueSeasonFromEvent(event):
    err = 0
    league = {}
    season = {}
    if 'header' in event:
        header = event['header']
        if 'season' in header:
            season = header['season']
        else:
            err = 1
        if 'league' in header:
            league = header['league']
        else:
            err = err + 2
    else:
        err = 3
    return(league, season,err)


def importFailedAttempts(dir1,failedImport):
    #filename = dir1 + 'failedImport.txt'
    #with open(filename, 'r') as file:
    #    failedImport = json.load(file)
    #file.close()

    length = len(failedImport.keys())

    newFailedImport = {}
    codeImportList = [404, 2502, 3001, 1008, 1018, 2500, 2999, 2504]
    k = 0
    while length > 0 and k < 5:
        i = 0
        for id in failedImport.keys():
            i += 1
            errorCode = failedImport[id]
            if errorCode in codeImportList:
                (event, code) = import_event(id, dir1)
                if 'boxscore' in event:
                    print(i, length, id, event['boxscore']['teams'][0]['team']['name'], 'v',
                          event['boxscore']['teams'][1]['team']['name'], code)
                else:
                    print("attempt",k, i, length, id, code)
                    newFailedImport[id] = code
        length = len(newFailedImport)
        failedImport = newFailedImport
        newFailedImport = {}
        k += 1
        print(length, k)

    #filename = dir1 + 'failedImport.txt'
    #with open(filename, 'w') as file:
    #    json.dump(failedImport, file)
    #file.close()
    return (failedImport)
def open_league_list(dir):
    filename = dir + 'leagueList.txt'
    print(filename)
    with open(filename, 'r') as file:
        leagueList = json.load(file)
    file.close()
    return leagueList


def loadFixtureList(filename, key1, value1, key2, value2):
    print(filename)
    with open(filename, 'r') as file:
        fixtures = json.load(file)
    file.close()
    fixtureList = {}
    for fixture in fixtures:
        eventId = fixture['id']
        if key1 in fixture.keys() and key2 in fixture.keys():
            # print (fixture[key1],fixture[key1] == value1,fixture[key2], str(fixture[key2]) == value2)
            if value1 == fixture[key1] and value2 == str(fixture[key2]):
                fixtureList[eventId] = fixture
    return (fixtureList, len(fixtureList))


def get_league_list():
    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    uri = 'https://site.api.espn.com/apis/site/v2/leagues/dropdown?lang=en&region=us&calendartype=whitelist&limit=100&sport=soccer'
    Response = requests.get(uri)
    # print(Response.json().keys())
    leagues = Response.json()['leagues']
    leagueList = {}
    i = 0
    for league in leagues:
        #print(league)
        if 'id' in league:
            leagueList[i] = league
        elif 'alternateId' in league:
            league['id'] = league['alternateId']
            leagueList[i] = league
        else:
            league['id'] = -1
            leagueList[i] = league
        leagueList[i]['updateTime']= currentTime
        i = i + 1
    return leagueList


def readUid(uidStr):
    tmpId = uidStr.split("~")
    uid = {}
    for item in tmpId:
        key = item.split(":")[0]
        value = item.split(":")[1]
        uid[key] = value
    return (uid)


def findLeague(id, leagueList):
    # print ('League id= ',id)
    for i in range(len(leagueList)):
        league = leagueList[i]
        if 'id' in league:
            if id == league['id']:
                return (league, 0)
        else:
            # print (league)
            sys.exit()
    return ({}, -1)


def utc2pst(strTimeEST, strTimeFormatIn, strTimeFormatOut):
    # strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    # strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone = tz.gettz('America/Los_Angeles')
    from_zone = tz.gettz('America/New_York')
    #    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est = datetime.strptime(strTimeEST, strTimeFormatIn)
    est = est.replace(tzinfo=from_zone)
    uk = est.astimezone(to_zone)
    return uk.strftime(strTimeFormatOut)


def extractDateTime(strDateUTC, inFormat, outFormat):
    date = datetime.strptime(strDateUTC, inFormat)
    return (date.strftime(outFormat))

def importFixtureByDate(datelist, saveDir, nLimit,league,league_list, logFileName):
    logFile = open(logFileName, 'w')
    delim = ','
    #
    # Import fixture from start_date to end_date
    #
    length = len(datelist)
    #print(length,datelist)
    reloadDate = datelist
    today = date.today()
    k = 0
    i = 0
    nMatchDates = 0
    nTotFixtures = 0
    while length > 0 and k < 5:
        tmpReloadDate = []
        for single_date in reloadDate:
            import_date = single_date.strftime("%Y%m%d")
            fileDir = saveDir + "fixture/" + league + "/"
            if not (os.path.isdir(fileDir)):
                os.mkdir(fileDir)
                print("create directory",fileDir)
            (events, err) = downloadFixtureByDate(import_date, league, nLimit)
            while err == 2502 and nLimit > 0:
                (events, err) = downloadFixtureByDate(import_date, league, nLimit)
                nLimit = int(nLimit / 2)
            #print('err Code',err)
            if err == 0:
                fixturesByDate = saveFixtureByDate(import_date,events,fileDir,league,league_list)
                nMatchDates += 1
                nTotFixtures = nTotFixtures + len(fixturesByDate)
            elif err > 0:
                tmpReloadDate.append(single_date)
            #fileDir1 = fileDir + import_date + "/"
            #(fixturesByDate, err) = import_fixture(import_date, league, fileDir1, league_list, nLimit)
            #while err == 2502 and nLimit > 0:
            #    (fixturesByDate, err) = import_fixture(import_date, league, fileDir1, league_list, nLimit)
            #    nLimit = int(nLimit / 2)
            #if err == 0:
            #    filename = fileDir + import_date + '.txt'
            #    with open(filename, 'w') as file:
            #        json.dump(fixturesByDate, file)
            #    file.close()
            #    print("import_fixture", filename)
            #elif err > 0:
            #    tmpReloadDate.append(single_date)
            i += 1
            line = import_date + delim + str(err) + delim + today.strftime("%Y%m%d")
            print(k, i, line)
            line = line + '\n'
            logFile.write(line)
        k += 1
        reloadDate = tmpReloadDate
        length = len(tmpReloadDate)

    logFile.close()

    return(nMatchDates,nTotFixtures)
def import_league_table_espn(seasonYear,seasonType,directory,league):

    uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/'+league+'/teams'
    #uri ='http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/359?enable=record'

    # seasonType is not available from api.  Use the pre-assigned seasonType in file name
    outFileName = directory + league + '_' + str(seasonYear) + "_" + str(seasonType) + "_table.txt"
    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    err = -1
    try:
        Response = requests.get(uri)
        soccer = Response.json()
    except:
        print('error download teams in league!')
        msg1 = "league: " + league
        print(msg1)
        sys.stderr.write('error download teams in league!\n')
        sys.stderr.write(msg1)
        return ({},{},err)
    keys=list(soccer.keys())
    if 'code' in keys:
        print (league,{},soccer['code'])
        return(soccer,{},soccer['code'])
    elif 'error' in keys:
        print (league,soccer['error'],soccer['message'])
        return(soccer,{},err)
    else:
        err = 0
        delim = ","
        # print(soccer)
        # fix seasonYear and seasonType assignment.  10/26/24
        leagues = soccer['sports'][0]['leagues'][0]
        year1 = 0
        year2 = 0
        if 'year' in leagues:
            year1 = leagues['year']
        if 'season' in leagues:
            season = leagues['season']
            if 'year' in season:
                year2 = season['year']
        if year1 != seasonYear:
            print("year does match downloaded data!", seasonYear, seasonYear)
            soccer['sports'][0]['leagues'][0]['year'] = seasonYear
        if year2 != seasonYear:
            print("year does match downloaded data!", seasonYear, seasonYear)
            soccer['sports'][0]['leagues'][0]['season']['year'] = seasonYear
        # add more keys in leagues
        soccer['sports'][0]['leagues'][0]['midsizeName'] = league
        soccer['sports'][0]['leagues'][0]['seasonType'] = seasonType
        soccer['sports'][0]['leagues'][0]['timeStamp'] = currentTime
        teams = soccer['sports'][0]['leagues'][0]['teams']
        i = 0
        n = 0
        j = 0
        k = 0
        teamsInLeague = []
        # print(outFileName,"no of teams in", league, len(teams))
        for tmpTeam in teams:
            teamId=tmpTeam['team']['id']
            teamName=tmpTeam['team']['name']
            teamDisplayName=tmpTeam['team']['displayName']
            tmpTeamInLeague = {
                            'teamId':teamId,
                             'displayName':teamDisplayName,
                             'name':teamName,
                             'seasonType': seasonType,
                             'year': seasonYear,
                             'midSizeLeagueName':league,
                             'logoUrl1':'',
                             'logoUrl2':'',
                             'timeStamp':currentTime}
            err = -1
            uri ='http://site.api.espn.com/apis/site/v2/sports/soccer/'+league+'/teams/'+teamId
            try:
                Response = requests.get(uri)
                soccer2 = Response.json()
            except Exception as e:
                print(e)
                print('error download team!')
                msg1 = "league: " + league
                msg2 = "team: " + str(teamId)
                print(msg1)
                print(msg2)
                # sys.stderr.write(e)
                sys.stderr.write('error download team!\n')
                sys.stderr.write(msg1)
                sys.stderr.write(msg2)
                #return({},{},err)
                continue
            keys=list(soccer2.keys())
            if 'code' in keys:
                print(league, soccer2['code'])
                #return (soccer,{}, soccer2['code'])
                continue
            elif 'error' in keys:
                print(league, soccer2['error'], soccer2['message'])
                continue
                #return (soccer2,{}, err)
            elif 'team' in soccer2:
                team=soccer2['team']
                err = 0
            else:
                print("team key not in response")
                print(uri)
                print("seasonType", seasonType, "league",league,"teamId",teamId)
                print(soccer2)
                continue
            k += 1
            #print(uri)
            id=team['id']
            slug=team['slug']
            location=team['location']
            name=team['name']
            if 'nickname' in team.keys():
                nickname=team['nickname']
            else:
                nickname='none'
            if 'abbreviation' in team.keys():
                abbreviation=team['abbreviation']
            else:
                abbreviation='none'
            displayName=team['displayName']
            shortDisplayName=team['shortDisplayName']
            if 'color' in team.keys():
                color=team['color']
            else:
                color=''
            if 'alternateColor' in team.keys():
                alternateColor=team['alternateColor']
            else:
                alternateColor=''
            if 'isActive' in team.keys():
                isActive=team['isActive']
            else:
                isActive=''
            if 'isAllStar' in team.keys():
                isAllStar=team['isAllStar']
            else:
                isAllStar=''
            logoUrl1 = ""
            logoUrl2 = ""
            if 'logos' in team.keys():
                if team['logos']:
                    logoUrl1 = team['logos'][0]['href']
                    if len(team['logos']) >=2:
                        logoUrl2 = team['logos'][1]['href']
            if 'logos' in soccer['sports'][0]['leagues'][0]['teams'][i]['team'].keys():
                soccer['sports'][0]['leagues'][0]['teams'][i]['team'].pop('logos')
            else:
                print(i,soccer['sports'][0]['leagues'][0]['teams'][i]['team'])
            soccer['sports'][0]['leagues'][0]['teams'][i]['team']['logoUrl1'] = logoUrl1
            soccer['sports'][0]['leagues'][0]['teams'][i]['team']['logoUrl2'] = logoUrl2
            tmpTeamInLeague['logoUrl1'] = logoUrl1
            tmpTeamInLeague['logoUrl2'] = logoUrl2
            if 'links' in soccer['sports'][0]['leagues'][0]['teams'][i]['team'].keys():
                soccer['sports'][0]['leagues'][0]['teams'][i]['team'].pop('links')
            else:
                print(i,soccer['sports'][0]['leagues'][0]['teams'][i]['team'])
            #print (i,id,slug,location,name,nickname,abbreviation,displayName,shortDisplayName,color,alternateColor,isActive,isAllStar)
            if 'defaultLeague' in team.keys():
                defaultLeague=team['defaultLeague']
                defaultLeague['hasStandings'] = defaultLeague['season']['type']['hasStandings']
                defaultLeague.pop('season')
                if 'links' in defaultLeague.keys():
                    defaultLeague.pop('links')
                soccer['sports'][0]['leagues'][0]['teams'][i]['team']['defaultLeague'] = defaultLeague
                leagueLogoUrl1 = ""
                leagueLogoUrl2 = ""
                if 'logos' in defaultLeague.keys():
                    if defaultLeague['logos']:
                        leagueLogoUrl1 = defaultLeague['logos'][0]['href']
                        if len(defaultLeague['logos']) >= 2:
                            leagueLogoUrl2 = defaultLeague['logos'][1]['href']
                    defaultLeague.pop('logos')
                defaultLeague['logoUrl1'] = leagueLogoUrl1
                defaultLeague['logoUrl2'] = leagueLogoUrl2
                j += 1
            if 'record' in team.keys():
                if 'items' in team['record']:
                    item=team['record']['items'][0]
                    summary=item['summary']
                    statistics=getStats(item['stats'])
                    wins=int(statistics['wins'])
                    losses=int(statistics['losses'])
                    ties=int(statistics['ties'])
                    mp=int(statistics['gamesPlayed'])
                    gf=int(statistics['pointsFor'])
                    ga=int(statistics['pointsAgainst'])
                    gd=int(statistics['pointDifferential'])
                    streak=int(statistics['streak'])
                    rankChange=int(statistics['rankChange'])
                    rank=int(statistics['rank'])
                    homeGf=int(statistics['homePointsFor'])
                    homeGa=int(statistics['homePointsAgainst'])
                    homeWins=int(statistics['homeWins'])
                    homeLosses=int(statistics['homeLosses'])
                    homeTies=int(statistics['homeTies'])
                    homeMp=int(statistics['homeGamesPlayed'])
                    awayGf=int(statistics['awayPointsFor'])
                    awayGa=int(statistics['awayPointsAgainst'])
                    awayWins=int(statistics['awayWins'])
                    awayLosses=int(statistics['awayLosses'])
                    awayTies=int(statistics['awayTies'])
                    awayMp=int(statistics['awayGamesPlayed'])
                    #print (i,rank,id,slug,name,nickname,abbreviation,displayName,shortDisplayName,summary,wins,ties,losses,mp,gf,ga,gd,rankChange,streak)
                    soccer['sports'][0]['leagues'][0]['teams'][i]['team']['record'] = statistics
                    n += 1
                else:
                    err = 1
            else:
                err = 2
            teamsInLeague.append(tmpTeamInLeague)
            i += 1
        # print("no of teams in", league, k,"With Record",n, "defaultLeague", j,"output file name:",outFileName)
        soccer['sports'][0]['leagues'][0]['teamsInLeague'] = k
        soccer['sports'][0]['leagues'][0]['teamsHasRecords'] = n
        soccer['sports'][0]['leagues'][0]['defaultLeague'] = j
        soccer['sports'][0]['leagues'][0]['outputFileName'] = outFileName
        # writes soccer on disk
        with open(outFileName,'w') as file:
            json.dump(soccer,file)
        file.close()
        return(soccer,teamsInLeague,err)
def import_league_status_espn(strLeague):

    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/'+strLeague+'/scoreboard'
    #uri ='http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard'
    err = -1
    seasons = []
    logos = []
    matchCalendar = []
    seasonTypeList = []
    today = date.today()
    calendarYear = today.year
    try:
        Response = requests.get(uri)
        soccer = Response.json()
    except:
        print('error download league!')
        msg1 = "league: " + strLeague
        print(msg1)
        sys.stderr.write('error download league!\n')
        sys.stderr.write(msg1)
        return ({}, seasons, logos, matchCalendar, seasonTypeList, err)
    keys=list(soccer.keys())
    if 'code' in keys:
        print (strLeague,soccer['code'])
        return ({}, seasons, logos, matchCalendar, seasonTypeList, err)
    elif 'error' in keys:
        print (strLeague,soccer['error'],soccer['message'])
        return ({}, seasons, logos, matchCalendar, seasonTypeList, err)
    else:
        err = 0
        # print(soccer)
        i = 0
        league=soccer['leagues'][0]
        if 'id' in league:
            leagueId = league['id']
        else:
            leagueId = -1
        # print(league)
        if 'season' in soccer.keys():
            season1=soccer['season']
            season1TypeId=season1['type']
            season1Year=season1['year']
        else:
            season1TypeId = 0
            season1Year = calendarYear

        season2=league['season']
        season2Year=season2['year']
        season2StartDate=season2['startDate']
        season2EndDate=season2['endDate']
        season2DisplayName=season2['displayName']

        season2Type=season2['type']
        season2Id=season2Type['id']
        season2TypeId=season2Type['type']
        season2Name=season2Type['name']
        season2Abbreviation=season2Type['abbreviation']

        tmpSeason1={}
        tmpSeason2={}

        if season1TypeId == season2TypeId and season1TypeId not in seasonTypeList:
            tmpSeason1['year'] = season1Year
            tmpSeason1['typeId'] = season1TypeId
            tmpSeason1['name'] = season2DisplayName
            tmpSeason1['id'] = season2Id
            tmpSeason1['startDate']=season2StartDate
            tmpSeason1['endDate']=season2EndDate
            tmpSeason1['abbreviation']=season2Abbreviation
            tmpSeason1['slug']=season2Name
            tmpSeason1['midsizeName']=strLeague
            tmpSeason1['leagueId']=leagueId
            tmpSeason1['timeStamp']=currentTime
            seasons.append(tmpSeason1)
            seasonTypeList.append(season1TypeId)
        elif season1TypeId != season2TypeId:
            # if seaonType is different, add both to database
            if season1TypeId not in seasonTypeList:
                if season1TypeId != 0:
                    tmpSeason1['year'] = season1Year
                    tmpSeason1['typeId'] = season1TypeId
                else:
                    tmpSeason1['year'] = season2Year
                    tmpSeason1['typeId'] = season2TypeId
                tmpSeason1['name'] = season2DisplayName
                tmpSeason1['id'] = 0
                tmpSeason1['startDate']=season2StartDate
                tmpSeason1['endDate']=season2EndDate
                tmpSeason1['abbreviation']=""
                tmpSeason1['slug']=""
                tmpSeason1['midsizeName'] = strLeague
                tmpSeason1['leagueId'] = leagueId
                tmpSeason1['timeStamp'] = currentTime
                seasons.append(tmpSeason1)
                seasonTypeList.append(season1TypeId)
            if season2TypeId not in seasonTypeList:
                tmpSeason2['year']=season2Year
                tmpSeason2['typeId']=season2TypeId
                tmpSeason2['name']=season2DisplayName
                tmpSeason2['id'] = season2Id
                tmpSeason2['startDate']=season2StartDate
                tmpSeason2['endDate']=season2EndDate
                tmpSeason2['abbreviation']=season2Abbreviation
                tmpSeason2['slug']=season2Name
                tmpSeason2['midsizeName'] = strLeague
                tmpSeason2['leagueId'] = leagueId
                tmpSeason2['timeStamp'] = currentTime
                seasons.append(tmpSeason2)
                seasonTypeList.append(season2TypeId)
        league.pop('season')
        tmpLogos=league['logos']
        i = 0
        for logo in tmpLogos:
            logoId = i
            logoHref=logo['href']
            logoWidth = logo['width']
            logoHeight = logo['height']
            logoAlt=logo['alt']
            logoRel1=logo['rel'][0]
            logoRel2 = logo['rel'][1]
            logoLastUpdated=logo['lastUpdated']
            logos.append({'midSizeLeagueName':strLeague,
                          'typeId': season1TypeId,
                          'id': logoId,
                          'href':logoHref,
                          'width':logoWidth,
                          'height':logoHeight,
                          'alt':logoAlt,
                          'logoRel1':logoRel1,
                          'logoRel2':logoRel2,
                          'lastUpdated':logoLastUpdated,
                          'timeStamp':currentTime})
            i += 1
        league.pop('logos')
        league['year'] = season1Year
        league['typeId'] = season1TypeId
        league['timeStamp'] = currentTime
        calendarType=league['calendarType']
        calendarIsWhitelist=league['calendarIsWhitelist']
        calenderStartDate=league['calendarStartDate']
        calenderEndDate=league['calendarEndDate']
        if calendarType == "list":
            tmpCalendar = league['calendar'][0]
            # print(tmpCalendar)
            tmpEntries = tmpCalendar['entries']
            for tmpEntry in tmpEntries:
                entry = {}
                entry['midSizeLeagueName'] = strLeague
                entry['typeId'] = season1TypeId
                entry['leagueLabel'] = tmpCalendar['label']
                entry['calendarType'] = calendarType
                entry['calendarIsWhitelist'] = calendarIsWhitelist
                entry['seasonStartDate'] = tmpCalendar['startDate']
                entry['SeasonEndDate'] = tmpCalendar['endDate']
                for key in tmpEntry:
                    entry[key]=tmpEntry[key]
                entry['timeStamp'] = currentTime
                matchCalendar.append(entry)
        elif calendarType == "day":
            tmpCalendar = league['calendar']
            # print(tmpCalendar)
            i = 0
            for matchDate in tmpCalendar:
                matchCalendar.append({'midSizeLeagueName': strLeague,
                                      'typeId': season1TypeId,
                                      'leagueLabel': league['name'],
                                      'calendarType':calendarType,
                                      'calendarIsWhitelist':calendarIsWhitelist,
                                      'seasonStartDate':calenderStartDate,
                                      'seasonEndDate':calenderEndDate,
                                      'matchDay': i,
                                      'matchDate': matchDate,
                                      'timeStamp': currentTime})
                i += 1
        else:
            print("unknow calendar type", calendarType)
        league.pop('calendar')
        return (league, seasons, logos, matchCalendar, seasonTypeList, err)
def openLeagueList(directory):
    filename=directory+'leagueList.txt'
    leagueList={}
    with open (filename,'r') as file:
        Response=json.load(file)
    for i in range(len(Response)):
        leagueList[i]=Response[str(i)]
    file.close()
    return (leagueList)
def getStats(StatsList):
    stats={}
    for item in StatsList:
        stats[item['name']]=item['value']
    return(stats)
def downloadFixtureByDate(myDate, leagueMidsizeName, nLimit):
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=chn.1&calendartype=whitelist&limit=100&dates=20200912-20210101'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=eng.1&dates=20200919-20201010'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?league=ger.1&dates=20200919-20201010'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?league=ger.1&dates=20200919'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?limit=900&dates=20200927'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?limit=100&dates=20210201'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=eng.1&limit=900&dates=20200101-20200831'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?&limit=900&dates=20200101'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?&limit=900&dates=20201023'
    uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/' + leagueMidsizeName + '/scoreboard?&limit='+str(nLimit)+'&dates=' + myDate
    print('uri=', uri)
    try:
        Response = requests.get(uri)
    except requests.exceptions.Timeout:
        err_code = 990
        print(myDate, err_code)
        return ([], err_code)
    except requests.exceptions.ConnectionError:
        err_code = 980
        print(myDate, err_code)
        return ([], err_code)
    except requests.exceptions.JSONDecodeError:
        err_code = 970
        print(myDate, err_code)
        return ([], err_code)
    # print('uri', Response.json().keys())

    if 'code' in Response.json().keys():
        err_code = Response.json()['code']
        print(myDate, err_code)
        return ([], err_code)

    if 'events' not in Response.json().keys():
        err_code = 999
        print(myDate, err_code)
        return ([], err_code)

    events = Response.json()['events']
    leagues = Response.json()['leagues']
    if not bool(events):
        print(myDate, "empty")
        return ([], -1)

    err_code = 0
    return(events,err_code)

def saveFixtureByDate(myDate,events,fileDir,leagueMidsizeName,leagueList):
    i = 0
    fixturesByDate=[]
    saveDir = fileDir + myDate + '/'
    if not (os.path.isdir(saveDir)):
        os.mkdir(saveDir)
    for event in events:
        if 'id' in event.keys():
            i += 1
            eventId=event['id']
            #event['saveDir'] = myDate
            tmpFilename = saveDir + str(eventId) + '.txt'
            with open(tmpFilename, 'w') as file:
                json.dump(event, file)
            file.close()
            tmpFixture = readFixtureByDate(event,myDate,leagueMidsizeName,leagueList)
            fixturesByDate.append(tmpFixture)
        else:
            print("no eventId",event)
    filename = fileDir + myDate + '.txt'
    with open(filename, 'w') as file:
        json.dump(fixturesByDate, file)
    file.close()
    print("save_fixture", filename)
    return(fixturesByDate)

def readFixtureByDate(event,myDate,leagueMidsizeName,leagueList):
    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    dateFilename = myDate + '.txt'
    fixtureTmp = {}
    #print (event)
    try:
        eventId = event['id']
    except:
        err = -1
        print('error!')
        print(event)
        fixtureTmp['err'] = err
        return (fixtureTmp)
    if 'uid' in event.keys():
        uidStr = event['uid']
        uid = readUid(uidStr)
    else:
        err = -1
    if 'l' in uid:
        leagueId = uid['l']
        (league, err) = findLeague(leagueId, leagueList)
    else:
        err = -1
        leagueId = '9999'
        fixtureTmp['err'] = err
    if err == 0:
        importedLeagueName = league['name']
        importedLeagueMidsizeName = league['midsizeName']
    else:
        importedLeagueName = 'none'
        importedLeagueMidsizeName = 'none'
    if leagueMidsizeName == 'all' or leagueMidsizeName == importedLeagueMidsizeName:
        eventDate = event['date']
        eventName = event['name']
        if 'shortName' in event.keys():
            shortName = event['shortName']
        else:
            shortName = "none"
        season = event['season']['year']
        if 'type' in event['season']:
            type = event['season']['type']
        status = event['status']['type']['name']
        competitions = event['competitions'][0]
        # print(competitions.keys())
        competitionId = competitions['id']
        if 'attendance' in competitions.keys():
            attendance = competitions['attendance']
        else:
            attendance = ''
        if 'venue' in competitions:
            venue = competitions['venue']['fullName']
        else:
            venue = 'none'
        #notes = competitions['notes']
        #        print (notes)
        #situation = competitions['situation']
        #        print (situation)
        competitors = competitions['competitors']
        if 'odds' in competitions:
            hasOdds = True
            odds = competitions['odds']
        #            print (odds)
        else:
            hasOdds = False
        # print('competitor',competitors[0].keys())
        # print('competitor',competitors[1].keys())
        for competitor in competitors:
            homeAway = competitor['homeAway']
            if homeAway == 'home':
                hometeamId = competitor['team']['id']
                hometeam = competitor['team']['name']
                homegoal = score(competitor['score'], status)
                if 'form' in competitor:
                    homeform = competitor['form']
                else:
                    homeform = 'none'
                if 'records' in competitor:
                    homerecords = competitor['records'][0]['summary']
                if (status == 'STATUS_FULL_TIME'):
                    homestatistics = competitions['competitors'][0]['statistics']
                if 'shootoutScore' in competitor:
                    homeShootoutScore = score(competitor['shootoutScore'],status)
                else:
                    homeShootoutScore = 'none'
            #                    for item in homestatistics:
            #                        print(item['name'], item['displayValue'])

            if homeAway == 'away':
                awayteamId = competitor['team']['id']
                awayteam = competitor['team']['name']
                awaygoal = score(competitor['score'], status)
                if 'form' in competitor:
                    awayform = competitor['form']
                else:
                    awayform = 'none'
                if 'records' in competitor:
                    awayrecords = competitor['records'][0]['summary']
                if (status == 'STATUS_FULL_TIME'):
                    awaystatistics = competitions['competitors'][0]['statistics']
                if 'shootoutScore' in competitor:
                    awayShootoutScore = score(competitor['shootoutScore'], status)
                else:
                    awayShootoutScore = 'none'
        #                    for item in awaystatistics:
        #                        print(item['name'], item['displayValue'])
        if 'details' in competitions.keys():
            #print('home',hometeamId)
            #print('away',awayteamId)
            nYellowCard = {}
            nYellowCard[hometeamId] = 0
            nYellowCard[awayteamId] = 0
            nRedCard = {}
            nRedCard[hometeamId] = 0
            nRedCard[awayteamId] = 0
            tmpDetails = competitions['details']
            for tmpDetail in tmpDetails:
                tmpDetailDict = {}
                tmpDetailDict['eventId'] = eventId
                for tmpKey in tmpDetail:
                    if tmpKey == 'type':
                        tmpDetailType = tmpDetail['type']
                        tmpDetailDict['typeId'] = tmpDetailType['id']
                        tmpDetailDict['typeText'] = tmpDetailType['text']
                        #if tmpDetailType['id'] not in detailType.keys():
                        #    detailType[tmpDetailType['id']] = tmpDetailType['text']
                    elif tmpKey == 'clock':
                        tmpDetailDict['clockValue'] = tmpDetail['clock']['value']
                        tmpDetailDict['clockDisplayValue'] = tmpDetail['clock']['displayValue']
                    elif tmpKey == 'team':
                        tmpDetailDict['teamId'] = tmpDetail['team']['id']
                    elif tmpKey == 'athletesInvolved':
                        if tmpDetail['athletesInvolved']:
                            tmpDetailDict['athletesInvolved'] = tmpDetail['athletesInvolved'][0]['id']
                        else:
                            tmpDetailDict['athletesInvolved'] = ''
                    else:
                        tmpDetailDict[tmpKey] = tmpDetail[tmpKey]
                if tmpDetailDict['yellowCard'] == True:
                    #print(tmpDetailDict['teamId'],tmpDetailDict['yellowCard'])
                    if tmpDetailDict['teamId'] not in nYellowCard.keys():
                        #print(eventId)
                        #print(nYellowCard)
                        #tmpFilename = 'D:/soccer/tmp/' + str(eventId) + '.txt'
                        #with open(tmpFilename, 'w') as file:
                        #    json.dump(event, file)
                        #file.close()
                        #print(tmpFilename)
                        nYellowCard[tmpDetailDict['teamId']] = 1
                    else:
                        nYellowCard[tmpDetailDict['teamId']] += 1
                if tmpDetailDict['redCard'] == True:
                    if tmpDetailDict['teamId'] not in nRedCard.keys():
                        nRedCard[tmpDetailDict['teamId']] = 1
                    else:
                        nRedCard[tmpDetailDict['teamId']] += 1
                #details.append(tmpDetailDict)
            homeYellowCard = 0
            awayYellowCard = 0
            homeRedCard = 0
            awayRedCard = 0
            for teamId in nYellowCard.keys():
                if teamId == hometeamId:
                    homeYellowCard = nYellowCard[hometeamId]
                    homeRedCard = nRedCard[hometeamId]
                if teamId == awayteamId:
                    awayYellowCard = nYellowCard[awayteamId]
                    awayRedCard = nRedCard[awayteamId]

        #filename = fileDir + str(eventId) + '.txt'
        #with open(filename, 'w') as file:
        #    json.dump(event, file)
        #file.close()
        #print(filename)

        fixtureTmp['id'] = eventId
        fixtureTmp['uid'] = uidStr
        fixtureTmp['leagueId'] = leagueId
        fixtureTmp['league'] = importedLeagueMidsizeName
        fixtureTmp['season'] = season
        fixtureTmp['season'] = season
        fixtureTmp['status'] = status
        fixtureTmp['date'] = eventDate
        fixtureTmp['name'] = eventName
        fixtureTmp['venue'] = venue
        fixtureTmp['hometeam'] = hometeam
        fixtureTmp['hometeamId'] = hometeamId
        fixtureTmp['awayteam'] = awayteam
        fixtureTmp['awayteamId'] = awayteamId
        fixtureTmp['homegoal'] = homegoal
        fixtureTmp['awaygoal'] = awaygoal
        fixtureTmp['homeShootoutScore'] = homeShootoutScore
        fixtureTmp['awayShootoutScore'] = awayShootoutScore
        fixtureTmp['homeform'] = homeform
        fixtureTmp['awayform'] = awayform
        fixtureTmp['hasOdds'] = hasOdds
        fixtureTmp['homeYellowCard'] = homeYellowCard
        fixtureTmp['homeRedCard'] = homeRedCard
        fixtureTmp['awayYellowCard'] = awayYellowCard
        fixtureTmp['awayRedCard'] = awayRedCard
        fixtureTmp['fileName'] = dateFilename
        fixtureTmp['updateTime'] = currentTime
        # print(hometeam,homerecords,homeform)
        # print(awayteam,awayrecords, awayform)
        # print (fixtureTmp)
    else:
        err = -1
    fixtureTmp['err'] = err
    return (fixtureTmp)

def import_fixture(myDate, leagueMidsizeName, fileDir, leagueList, nLimit):
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=chn.1&calendartype=whitelist&limit=100&dates=20200912-20210101'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=eng.1&dates=20200919-20201010'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?league=ger.1&dates=20200919-20201010'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?league=ger.1&dates=20200919'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?limit=900&dates=20200927'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?limit=100&dates=20210201'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?league=eng.1&limit=900&dates=20200101-20200831'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?&limit=900&dates=20200101'
    # uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard?&limit=900&dates=20201023'
    uri = 'http://site.api.espn.com/apis/site/v2/sports/soccer/' + leagueMidsizeName + '/scoreboard?&limit='+str(nLimit)+'&dates=' + myDate
    fixture = []
    print('uri=', uri)
    try:
        Response = requests.get(uri)
    except requests.exceptions.Timeout:
        err_code = 990
        print(myDate, err_code)
        return (fixture, err_code)
    except requests.exceptions.ConnectionError:
        err_code = 980
        print(myDate, err_code)
        return (fixture, err_code)
    # print('uri', Response.json().keys())

    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    dateFilename = myDate + '.txt'

    if 'code' in Response.json().keys():
        err_code = Response.json()['code']
        print(myDate, err_code)
        return (fixture, err_code)

    if 'events' not in Response.json().keys():
        err_code = 999
        print(myDate, err_code)
        return (fixture, err_code)

    events = Response.json()['events']

    if not bool(events):
        print(myDate, "empty")
        return (fixture, -1)

    if not (os.path.isdir(fileDir)):
        os.mkdir(fileDir)
    # print(events)
    leagues = Response.json()['leagues']
    # print (events)
    # print('events', events[0].keys())
    # print('competitions', events[0]['competitions'][0].keys())
    # print('competitors', events[0]['competitions'][0]['competitors'][0].keys())

    i = 0
    for event in events:
        fixtureTmp = {}
        i += 1
        #        print (event)
        eventId = event['id']
        uidStr = event['uid']
        uid = readUid(uidStr)
        if 'l' in uid:
            leagueId = uid['l']
            (league, err) = findLeague(leagueId, leagueList)
        else:
            err = -1
        if err == 0:
            importedLeagueName = league['name']
            importedLeagueMidsizeName = league['midsizeName']
        else:
            importedLeagueName = 'none'
            importedLeagueMidsizeName = 'none'
        if leagueMidsizeName == 'all' or leagueMidsizeName == importedLeagueMidsizeName:
            eventDate = event['date']
            eventName = event['name']
            if 'shortName' in event.keys():
                shortName = event['shortName']
            else:
                shortName = "none"
            season = event['season']['year']
            if 'type' in event['season']:
                type = event['season']['type']
            status = event['status']['type']['name']
            competitions = event['competitions'][0]
            # print(competitions.keys())
            competitionId = competitions['id']
            if 'attendance' in competitions.keys():
                attendance = competitions['attendance']
            else:
                attendance = ''
            if 'venue' in competitions:
                venue = competitions['venue']['fullName']
            else:
                venue = 'none'
            #notes = competitions['notes']
            #        print (notes)
            #situation = competitions['situation']
            #        print (situation)
            competitors = competitions['competitors']
            if 'odds' in competitions:
                hasOdds = True
                odds = competitions['odds']
            #            print (odds)
            else:
                hasOdds = False
            # print('competitor',competitors[0].keys())
            # print('competitor',competitors[1].keys())
            for competitor in competitors:
                homeAway = competitor['homeAway']
                if homeAway == 'home':
                    hometeamId = competitor['team']['id']
                    hometeam = competitor['team']['name']
                    homegoal = score(competitor['score'], status)
                    if 'form' in competitor:
                        homeform = competitor['form']
                    else:
                        homeform = 'none'
                    if 'records' in competitor:
                        homerecords = competitor['records'][0]['summary']
                    if (status == 'STATUS_FULL_TIME'):
                        homestatistics = competitions['competitors'][0]['statistics']
                    if 'shootoutScore' in competitor:
                        homeShootoutScore = score(competitor['shootoutScore'],status)
                    else:
                        homeShootoutScore = 'none'
                #                    for item in homestatistics:
                #                        print(item['name'], item['displayValue'])

                if homeAway == 'away':
                    awayteamId = competitor['team']['id']
                    awayteam = competitor['team']['name']
                    awaygoal = score(competitor['score'], status)
                    if 'form' in competitor:
                        awayform = competitor['form']
                    else:
                        awayform = 'none'
                    if 'records' in competitor:
                        awayrecords = competitor['records'][0]['summary']
                    if (status == 'STATUS_FULL_TIME'):
                        awaystatistics = competitions['competitors'][0]['statistics']
                    if 'shootoutScore' in competitor:
                        awayShootoutScore = score(competitor['shootoutScore'], status)
                    else:
                        awayShootoutScore = 'none'
            #                    for item in awaystatistics:
            #                        print(item['name'], item['displayValue'])
            if 'details' in competitions.keys():
                #print('home',hometeamId)
                #print('away',awayteamId)
                nYellowCard = {}
                nYellowCard[hometeamId] = 0
                nYellowCard[awayteamId] = 0
                nRedCard = {}
                nRedCard[hometeamId] = 0
                nRedCard[awayteamId] = 0
                tmpDetails = competitions['details']
                for tmpDetail in tmpDetails:
                    tmpDetailDict = {}
                    tmpDetailDict['eventId'] = eventId
                    for tmpKey in tmpDetail:
                        if tmpKey == 'type':
                            tmpDetailType = tmpDetail['type']
                            tmpDetailDict['typeId'] = tmpDetailType['id']
                            tmpDetailDict['typeText'] = tmpDetailType['text']
                            #if tmpDetailType['id'] not in detailType.keys():
                            #    detailType[tmpDetailType['id']] = tmpDetailType['text']
                        elif tmpKey == 'clock':
                            tmpDetailDict['clockValue'] = tmpDetail['clock']['value']
                            tmpDetailDict['clockDisplayValue'] = tmpDetail['clock']['displayValue']
                        elif tmpKey == 'team':
                            tmpDetailDict['teamId'] = tmpDetail['team']['id']
                        elif tmpKey == 'athletesInvolved':
                            if tmpDetail['athletesInvolved']:
                                tmpDetailDict['athletesInvolved'] = tmpDetail['athletesInvolved'][0]['id']
                            else:
                                tmpDetailDict['athletesInvolved'] = ''
                        else:
                            tmpDetailDict[tmpKey] = tmpDetail[tmpKey]
                    if tmpDetailDict['yellowCard'] == True:
                        #print(tmpDetailDict['teamId'],tmpDetailDict['yellowCard'])
                        if tmpDetailDict['teamId'] not in nYellowCard.keys():
                            #print(eventId)
                            #print(nYellowCard)
                            #tmpFilename = 'D:/soccer/tmp/' + str(eventId) + '.txt'
                            #with open(tmpFilename, 'w') as file:
                            #    json.dump(event, file)
                            #file.close()
                            #print(tmpFilename)
                            nYellowCard[tmpDetailDict['teamId']] = 1
                        else:
                            nYellowCard[tmpDetailDict['teamId']] += 1
                    if tmpDetailDict['redCard'] == True:
                        if tmpDetailDict['teamId'] not in nRedCard.keys():
                            nRedCard[tmpDetailDict['teamId']] = 1
                        else:
                            nRedCard[tmpDetailDict['teamId']] += 1
                    #details.append(tmpDetailDict)
                homeYellowCard = 0
                awayYellowCard = 0
                homeRedCard = 0
                awayRedCard = 0
                for teamId in nYellowCard.keys():
                    if teamId == hometeamId:
                        homeYellowCard = nYellowCard[hometeamId]
                        homeRedCard = nRedCard[hometeamId]
                    if teamId == awayteamId:
                        awayYellowCard = nYellowCard[awayteamId]
                        awayRedCard = nRedCard[awayteamId]

            filename = fileDir + str(eventId) + '.txt'
            with open(filename, 'w') as file:
                json.dump(event, file)
            file.close()
            print(filename)

            fixtureTmp['id'] = eventId
            fixtureTmp['uid'] = uidStr
            fixtureTmp['league'] = importedLeagueMidsizeName
            fixtureTmp['season'] = season
            fixtureTmp['season'] = season
            fixtureTmp['status'] = status
            fixtureTmp['date'] = eventDate
            fixtureTmp['name'] = eventName
            fixtureTmp['venue'] = venue
            fixtureTmp['hometeam'] = hometeam
            fixtureTmp['hometeamId'] = hometeamId
            fixtureTmp['awayteam'] = awayteam
            fixtureTmp['awayteamId'] = awayteamId
            fixtureTmp['homegoal'] = homegoal
            fixtureTmp['awaygoal'] = awaygoal
            fixtureTmp['homeShootoutScore'] = homeShootoutScore
            fixtureTmp['awayShootoutScore'] = awayShootoutScore
            fixtureTmp['homeform'] = homeform
            fixtureTmp['awayform'] = awayform
            fixtureTmp['hasOdds'] = hasOdds
            fixtureTmp['homeYellowCard'] = homeYellowCard
            fixtureTmp['homeRedCard'] = homeRedCard
            fixtureTmp['awayYellowCard'] = awayYellowCard
            fixtureTmp['awayRedCard'] = awayRedCard
            fixtureTmp['fileName'] = dateFilename
            fixtureTmp['updateTime'] = currentTime
            # print(hometeam,homerecords,homeform)
            # print(awayteam,awayrecords, awayform)
            # print (fixtureTmp)
        else:
            err = -1
        fixtureTmp['err'] = err
        fixture.append(fixtureTmp)

    return (fixture, 0)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)+1):
        yield start_date + timedelta(n)


def score(goalScored, status):
    goalStatusList = ['STATUS_FULL_TIME', 'STATUS_FINAL_PEN', 'STATUS_FINAL_AET', 'STATUS_FINAL_AGT',
                      'STATUS_IN_PROGRESS', 'STATUS_FIRST_HALF', 'STATUS_SECOND_HALF', 'STATUS_HALFTIME']
    noGoalStatusList = ['STATUS_ABANDONED', 'STATUS_CANCELED', 'STATUS_POSTPONED', 'STATUS_SCHEDULED']
    if status in goalStatusList:
        return (goalScored)
    elif status in noGoalStatusList:
        return ('none')
    else:
        return ('StatusUnknown')


def removeDuplicateFixtures(fixtures, rootDir):
    fixtureId = []
    dupFixtureId = []
    dupFixtures = {}
    i = 0
    for fixture in fixtures:
        id = fixture['id']
        if id not in fixtureId:
            fixtureId.append(id)
            continue
        if id in fixtureId and id not in dupFixtureId:
            i += 1
            dupFixtureId.append(id)
            dupFixtures[id] = []
    nDup = i
    nDup1 = len(dupFixtures)
    nDup3 = len(dupFixtures.keys())
    nDup2 = len(dupFixtureId)

    for fixture in fixtures:
        id = fixture['id']
        if id in dupFixtureId:
            dupFixtures[id].append(fixture)
    k = 0
    for dupId in dupFixtures:
        k += 1
        league = []
        date = []
        status = []
        name = []
        hasOdds = []
        fileName = []
        bDelete = []
        n = len(dupFixtures[dupId])
        for fixture in dupFixtures[dupId]:
            id = fixture['id']
            league.append(fixture['league'])
            date.append(datetime.strptime(fixture['date'], '%Y-%m-%dT%H:%MZ'))
            status.append(fixture['status'])
            name.append(fixture['name'])
            hasOdds.append(fixture['hasOdds'])
            fileName.append(fixture['fileName'])
            bDelete.append(True)
        i = 0
        for fixture in dupFixtures[dupId]:
            if i == 0:
                pastDate = date[0]
                iKeep = 0
            else:
                if date[i] > pastDate:
                    pastDate = date[i]
                    iKeep = i
            if i == n - 1:
                bDelete[iKeep] = False
            if status[i] == 'STATUS_FULL_TIME':
                bDelete[i] = False
            i += 1
        i = 0
        for fixture in dupFixtures[dupId]:
            fixture['delete'] = bDelete[i]
            if n >= 3:
                print(k, i, id, league[i], date[i], status[i], name[i], hasOdds[i], fileName[i], bDelete[i])
            i += 1
        if n >= 3:
            print()
    print('number of duplicate event id:', nDup, nDup1, nDup3, nDup2)

    dupFileName = rootDir + 'duplicate_fixtures.txt'
    with open(dupFileName, 'w') as file:
        Response = json.dump(dupFixtures, file)
    file.close()

    newFixtures = []
    dupId = []
    fileDir = rootDir + 'fixture/all/'
    for fixture in fixtures:
        id = fixture['id']
        if id in dupFixtures.keys():
            if id not in dupId:
                for tmpFixture in dupFixtures[id]:
                    if tmpFixture['delete'] == False:
                        newFixtures.append(fixture)
                        dupId.append(id)
                    else:
                        # Delete date.txt file
                        delFilename = fileDir + tmpFixture['fileName'][0:7] + '/' + str(id) + '.txt'
                        print(id, delFilename)
                        if os.path.isfile(delFilename):
                            os.remove(delFilename)
        else:
            newFixtures.append(fixture)
    return (newFixtures)
def compareFixtures(oldFixtures, newFixtures):
    tmpOldFixtures = []
    tmpNewFixtures = []
    sortedOldFixtures = sorted(oldFixtures, key=lambda i: i['id'])
    sortedNewFixtures = sorted(newFixtures, key=lambda i: i['id'])
    for i in range(len(sortedOldFixtures)):
        oldFixture = sortedOldFixtures[i]
        if 'updateTime' in oldFixture:
            del oldFixture['updateTime']
        tmpOldFixtures.append(oldFixture)
    for i in range(len(sortedNewFixtures)):
        newFixture = sortedNewFixtures[i]
        if 'updateTime' in newFixture:
            del newFixture['updateTime']
        tmpNewFixtures.append(newFixture)
    #    for tmpFixture in tmpOldFixtures:
    #        print(tmpFixture)
    #    print()
    #    for tmpFixture in tmpNewFixtures:
    #        print(tmpFixture)
    print('length old', len(tmpOldFixtures))
    print('length new', len(tmpNewFixtures))
    idListOld = [fixture['id'] for fixture in tmpOldFixtures]
    kTotal = len(tmpNewFixtures)
    diffId = []
    diffFixture = {}
    compare = {}

    k = 0
    #printProgressBar(k, kTotal, prefix='find diff', suffix='complete.')
    i = 0
    for tmpNewFixture in tmpNewFixtures:
        id = tmpNewFixture['id']
        date = tmpNewFixture['date']
        if id in idListOld:
            tmpOldFixture = tmpOldFixtures[idListOld.index(id)]
            if tmpNewFixture != tmpOldFixture:
                diffId.append(id)
                i += 1
                tmpDiff = compareDict(tmpNewFixture, tmpOldFixture)
                tmpDiff['eventDate'] = [date, date]
                diffFixture[id] = tmpDiff
        else:
            diffId.append(id)
            i += 1
            tmpDiff = compareDict(tmpNewFixture, {})
            tmpDiff['eventDate'] = [date, date]
            diffFixture[id] = tmpDiff
        k += 1
        ##printProgressBar(k, kTotal, prefix='find diff', suffix='complete.')
    print(len(diffId))
    return (diffId, diffFixture)

def compareFixtures2(oldFixture, newFixture,start_date):
    id = newFixture["id"]
    matchDate = newFixture["date"]
    matchStatus = newFixture["status"]
    bDiff = False
    oldMatchDate = oldFixture["matchDateTime"]
    oldMatchStatus = oldFixture["matchStatus"]
    matchDateObj = datetime.strptime(matchDate, '%Y-%m-%dT%H:%MZ')
    oldMatchDateObj = oldMatchDate
    if matchDateObj == oldMatchDateObj and matchStatus == oldMatchStatus:
        bDiff = False
    else:
        if matchDateObj >= start_date:
            bDiff = True
    # print(i,id,bDiff,matchDate,oldMatchDate,matchStatus,oldMatchStatus)
    # print (fixture[filter1],fixture[filter2])
    # print(fixtureTmp)
    # print("bDiff=",bDiff)
    # print(matchStatus,oldMatchStatus)
    # print(matchDateObj,oldMatchDateObj)
    return bDiff
def compareFixtures3(bSlow, oldFixture, newFixture,end_date):
    noRoster = [
        "ARG.3",
        "ARG.4",
        "BOL.1",
        "BRA.3",
        "CHI.COPA_CHI",
        "COL.2",
        "CRC.1",
        "CLUB.FRIENDLY",
        "FIFA.FRIENDLY",
        "GHA.1",
        "GUA.1",
        "HON.1",
        "IDN.1",
        "IRL.1",
        "ISR.1",
        "KEN.1",
        "MEX.2",
        "MLT.1",
        "MYS.1",
        "NED.3",
        "NGA.1",
        "NIR.1",
        "RSA.2",
        "RSA.MTN8",
        "SGP.1",
        "SLV.1",
        "THA.1",
        "UGA.1",
        "URU.1",
        "USA.NCAA.M.1",
        "USA.NCAA.W.1",
        "VEN.1",
        "ZAM.1",
        "ZIM.1"
    ]
    noKeyEvents = [
        "ARG.3",
        "ARG.4",
        "BOL.1",
        "BRA.3",
        "CHI.COPA_CHI",
        "COL.2",
        "CRC.1",
        "FIFA.FRIENDLY",
        "CLUB.FRIENDLY",
        "GHA.1",
        "GUA.1",
        "HON.1",
        "IDN.1",
        "IRL.1",
        "ISR.1",
        "KEN.1",
        "MEX.2",
        "MLT.1",
        "MYS.1",
        "NED.3",
        "NGA.1",
        "NIR.1",
        "RSA.2",
        "RSA.MTN8",
        "SGP.1",
        "SLV.1",
        "THA.1",
        "UGA.1",
        "URU.1",
        "USA.NCAA.M.1",
        "USA.NCAA.W.1",
        "VEN.1",
        "ZAM.1",
        "ZIM.1"
    ]
    noCommentary = [
        "ARG.2",
        "ARG.3",
        "ARG.4",
        "BOL.1",
        "BRA.3",
        "CHI.COPA_CHI",
        "COL.2",
        "CRC.1",
        "ECU.1",
        "ENG.5",
        "GHA.1",
        "GRE.1",
        "GUA.1",
        "HON.1",
        "IDN.1",
        "IRL.1",
        "ISR.1",
        "ITA.2",
        "KEN.1",
        "MEX.2",
        "MLT.1",
        "MYS.1",
        "NED.3",
        "NGA.1",
        "NIR.1",
        "PAR.1",
        "RSA.1",
        "RSA.2",
        "RSA.MTN8",
        "SGP.1",
        "SLV.1",
        "THA.1",
        "UGA.1",
        "URU.1",
        "USA.NCAA.M.1",
        "USA.NCAA.W.1",
        "USA.USL.L1.CUP",
        "VEN.1",
        "WAL.1",
        "ZAM.1",
        "ZIM.1"
    ]
    id = newFixture["id"]
    matchDate = newFixture["date"]
    matchStatus = newFixture["status"]
    midsizeName = oldFixture["midsizeName"]
    bDiff = False
    msg = "no change"
    oldMatchDate = oldFixture["matchDateTime"]
    oldMatchStatus = oldFixture["matchStatus"]
    matchDateObj = datetime.strptime(matchDate, '%Y-%m-%dT%H:%MZ')
    #oldMatchDateObj = datetime.strptime(oldMatchDate, '%Y-%m-%dT%H:%MZ')
    oldMatchDateObj = oldMatchDate
    if matchDateObj != oldMatchDateObj:
        msg = "date changed"
        # print(matchDateObj,oldMatchDateObj)
        return msg, True
    if matchStatus != oldMatchStatus:
        msg = "status changed"
        return msg, True
    if matchDateObj > end_date:
        # print(matchDateObj,end_date)
        msg = "match date after end date"
        return msg, True
    if matchStatus == "STATUS_FULL_TIME":
        if bSlow:
            msg = "download all events"
            return msg, True
        if oldFixture['hasRosters'] == False and midsizeName not in noRoster:
            msg = "no roster"
            return msg, True
        if oldFixture['keyEvents'] <= 0 and midsizeName not in noKeyEvents:
            msg = "no keyEvents"
            return msg, True
        if oldFixture['commentary'] <= 0 and midsizeName not in noCommentary:
            msg = "no commentary"
            return msg, True
    # print(i,id,bDiff,matchDate,oldMatchDate,matchStatus,oldMatchStatus)
    # print (fixture[filter1],fixture[filter2])
    # print(fixtureTmp)
    # print("bDiff=",bDiff)
    # print(matchStatus,oldMatchStatus)
    # print(matchDateObj,oldMatchDateObj)
    return msg, bDiff
def open_event(id,directory):
    filename=directory + '/events/'+str(id)+'.txt'
    code=0
    try:
        with open (filename,'r') as file:
            Response = json.load(file)
    except json.decoder.JSONDecodeError:
        print("There was a problem accessing the equipment data.")
        print(id,'import again')
        file.close()
        code = 999
    file.close()
    if code == 999:
        (event, code) = import_event(id, directory)
    else:
        if 'code' in Response:
            code=Response['code']
        event=Response
    return(event,code)
def eventSnapshot(event,tmpEventId):
    snapShot = {}
    snapShot['eventId'] = tmpEventId
    snapShot['eventErr'] = 0
    header = {}
    competitions = {}
    status = {}
    competitors = []
    details = []
    keyEvents = []
    commentary = []
    standings = []
    homeTeamRecord = []
    awayTeamRecord = []
    if 'updateTime' not in event:
        currentTime = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        snapShot['snapshotTime'] = currentTime
    else:
        snapShot['snapshotTime'] = event['updateTime']
    if 'header' in event:
        header = event['header']
        snapShot['hasHeader'] = True
        eventId = int(header['id'])
        if eventId != tmpEventId:
            snapShot['eventErr'] = -1
        if 'season' in header:
            season = header['season']
            if 'year' in season:
                seasonYear = int(season['year'])
            else:
                seasonYear = -1
            if 'type' in season:
                seasonType = int(season['type'])
            else:
                seasonType = -1
            if 'name' in season:
                seasonName = season['name']
            else:
                seasonName = ""
            snapShot['seasonYear'] = seasonYear
            snapShot['seasonType'] = seasonType
            snapShot['seasonName'] = seasonName
        else:
            snapShot['seasonYear'] = -1
            snapShot['seasonType'] = -1
            snapShot['seasonName'] = ""
        if 'competitions' in header:
            snapShot['hasCompetitions'] = True
            competitions = header['competitions'][0]
            matchDate = competitions['date']
            snapShot['matchDate'] = matchDate
            if 'status' in competitions:
                status = competitions['status']['type']
                snapShot['hasStatus'] = True
                snapShot['statusId'] = status['id']
                snapShot['statusName'] = status['name']
            else:
                snapShot['hasStatus'] = False
                snapShot['matchDate'] = ""
            if 'competitors' in competitions:
                competitors = competitions['competitors']
                if len(competitors) == 2:
                    snapShot['competitors'] = 2
                    for competitor in competitors:
                        team = competitor['team']
                        if 'score' in competitor and 'homeAway' in competitor and 'name' in team and 'id' in team:
                            if competitor['homeAway'] == "home":
                                homeTeamId = int(team['id'])
                                homeTeamName= team['name']
                                homeTeamScore = competitor['score']
                                snapShot['homeTeamId'] = homeTeamId
                                snapShot['homeTeamName'] = homeTeamName
                                snapShot['homeTeamScore'] = homeTeamScore
                                if 'record' in competitor:
                                    homeTeamRecord = competitor['record']
                                    snapShot['homeTeamRecord'] = len(homeTeamRecord)
                                else:
                                    snapShot['homeTeamRecord'] = -1
                            else:
                                awayTeamId = int(team['id'])
                                awayTeamName= team['name']
                                awayTeamScore = competitor['score']
                                snapShot['awayTeamId'] = awayTeamId
                                snapShot['awayTeamName'] = awayTeamName
                                snapShot['awayTeamScore'] = awayTeamScore
                                if 'record' in competitor:
                                    awayTeamRecord = competitor['record']
                                    snapShot['awayTeamRecord'] = len(awayTeamRecord)
                                else:
                                    snapShot['awayTeamRecord'] = -1
                        else:
                            snapShot['homeTeamId'] = -1
                            snapShot['homeTeamName'] = ""
                            snapShot['homeTeamScore'] = -1
                            snapShot['awayTeamId'] = -1
                            snapShot['awayTeamName'] = ""
                            snapShot['awayTeamScore'] = -1
                else:
                    snapShot['competitors'] = -1
            else:
                snapShot['competitors'] = -1
            if 'details' in competitions:
                details = competitions['details']
                snapShot['details'] = len(details)
            else:
                snapShot['details'] = -1
        else:
            snapShot['hasCompetitions'] = False
        snapShot['leagueId'] = -1
        snapShot['leagueName'] = ""
        snapShot['midsizeName'] = ""
        if 'league' in header:
            league = header['league']
            if 'id' in league:
                leagueId = int(league['id'])
                snapShot['leagueId'] = leagueId
            if 'name' in league:
                leagueName = league['name']
                snapShot['leagueName'] = leagueName
            if 'midsizeName' in league:
                midsizeName=league['midsizeName']
                snapShot['midsizeName'] = midsizeName
    else:
        snapShot['leagueId'] = -1
        snapShot['leagueName'] = ""
        snapShot['midsizeName'] = ""
        snapShot['hasHeader'] = False
    if 'boxscore' in event:
        snapShot['hasBoxscore'] = True
        boxscore = event['boxscore']
        if 'teams' in boxscore:
            teams = boxscore['teams']
            iTeam = 0
            for team in teams:
                #snapShot['nHomeStats'] = -1
                #snapShot['nAwayStats'] = -1
                if 'homeAway' in team:
                    if team['homeAway'] == 'home':
                        if 'statistics' in team:
                            homeStats = team['statistics']
                            snapShot['nHomeStats'] = len(homeStats)
                        else:
                            snapShot['nHomeStats'] = -1
                    if team['homeAway'] == 'away':
                        if 'statistics' in team:
                            awayStats = team['statistics']
                            snapShot['nAwayStats'] = len(awayStats)
                        else:
                            snapShot['nAwayStats'] = -1
                else:
                    if iTeam == 0:
                        if 'statistics' in team:
                            homeStats = team['statistics']
                            snapShot['nHomeStats'] = len(homeStats)
                        else:
                            snapShot['nHomeStats'] = -1
                    if iTeam == 1:
                        if 'statistics' in team:
                            awayStats = team['statistics']
                            snapShot['nAwayStats'] = len(awayStats)
                        else:
                            snapShot['nAwayStats'] = -1
                iTeam += 1
        else:
            snapShot['nHomeStats'] = -1
            snapShot['nAwayStats'] = -1
    else:
        snapShot['hasBoxscore'] = False
        snapShot['nHomeStats'] = -1
        snapShot['nAwayStats'] = -1
    if 'gameInfo' in event:
        gameInfo = event['gameInfo']
        snapShot['hasGameInfo'] = True
    else:
        snapShot['hasGameInfo'] = False
    if 'odds' in event:
        snapShot['hasOdds'] = True
        odds = event['odds']
    else:
        snapShot['hasOdds'] = False
    if 'rosters' in event:
        rosters = event['rosters']
        if len(rosters) == 2:
            snapShot['hasRosters'] = True
            for roster in rosters:
                homeAway = roster['homeAway']
                if 'roster' in roster:
                    if homeAway == 'home':
                        homeRoster = roster['roster']
                        nHomePlayers = len(homeRoster)
                        snapShot['nHomePlayers'] = nHomePlayers
                    else:
                        awayRoster = roster['roster']
                        nAwayPlayers = len(awayRoster)
                        snapShot['nAwayPlayers'] = nAwayPlayers
                else:
                    snapShot['nHomePlayers'] = -1
                    snapShot['nAwayPlayers'] = -1
                    snapShot['hasRosters'] = False
        else:
            snapShot['nHomePlayers'] = -1
            snapShot['nAwayPlayers'] = -1
            snapShot['hasRosters'] = False
    else:
        snapShot['hasRosters'] = False
        snapShot['nHomePlayers'] = -1
        snapShot['nAwayPlayers'] = -1
    if 'keyEvents' in event:
        keyEvents =event['keyEvents']
        snapShot['keyEvents'] = len(keyEvents)
    else:
        snapShot['keyEvents'] = -1
    if 'commentary' in event:
        commentary =event['commentary']
        snapShot['commentary'] = len(commentary)
    else:
        snapShot['commentary'] = -1
    snapShot['standings'] = -1
    if 'standings' in event:
        if 'groups' in event['standings']:
            if len(event['standings']['groups']) == 1:
                standings =event['standings']['groups'][0]['standings']['entries']
                snapShot['standings'] = len(standings)
    return(snapShot)
def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd=" "):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        printEnd    - Optional  : use ' ' for Pycharm
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def compareDict(Dict1, Dict2):
    # Dict1 and Dict2 has the same keys
    diffDict = {}
    for key in Dict1.keys():
        if key in Dict2.keys():
            if Dict1[key] != Dict2[key]:
                diffDict[key] = [Dict1[key], Dict2[key]]
        else:
            diffDict[key] = [Dict1[key], '']
    return (diffDict)


def import_event_from_hd(id, dir):
    event = {}
    filelist = [f for f in os.listdir(dir)]
    filename = str(id) + '.txt'
    if filename in filelist:
        with open(filename, 'r') as file:
            Response = json.load(file)
        file.close()
        event = Response
    else:
        event['code'] = 404
        event['detail'] = 'file not found'
    # print(Response.keys())
    return (event)


def tzConvert(strTime, fromZone, fromFormat, toZone, toFormat):
    fromZone = tz.gettz(fromZone)
    toZone = tz.gettz(toZone)

    tmpFromTime = datetime.strptime(strTime, fromFormat)
    tmpFromTime = tmpFromTime.replace(tzinfo=fromZone)
    tmpToTime = tmpFromTime.astimezone(toZone)
    return tmpToTime.strftime(toFormat)
def import_team(team_id,midSizeName):
    #uri="http://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/"+str(team_id)+"/roster"
    uri="http://site.api.espn.com/apis/site/v2/sports/soccer/"+midSizeName+"/teams/"+str(team_id)+"/roster"
    try:
        Response=requests.get(uri)
        #print(Response.json().keys())
        roster=Response.json()
        code=0
    except requests.exceptions.Timeout:
        code = 990
        roster = {'code': code}
        print (roster['code'],team_id,midSizeName)
        return (roster, code)
    except requests.exceptions.ConnectionError:
        code = 980
        roster = {'code': code}
        print (roster['code'],team_id,midSizeName)
        return (roster, code)
    except requests.exceptions.JSONDecodeError:
        code = 970
        roster = {'code': code}
        return (roster, code)
    if 'code' in roster:
        print (roster['code'],team_id,midSizeName)
        return(roster,code)
    return(roster,code)
def saveAthleteDB(roster,k,nLeague,n,nTeams,iAthlete,athletesDB,leagueMidsizeName,teamId,
                  teamName,importSeasonType,statNames):
    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    if 'timestamp' in roster:
        timestamp = roster['timestamp']
    else:
        timestamp = currentTime
    if 'status' in roster:
        status = roster['status']
    else:
        status = 'none'
    if 'season' in roster:
        season = roster['season']
        if 'year' in season:
            seasonYear = season['year']
        else:
            seasonYear = 'none'
        if 'type' in season:
            seasonType = season['type']
        else:
            seasonType = 'none'
        if seasonType != importSeasonType:
            print("seasonType from AthleteDB does not match seasonType from Fixture")
            print("seasonType from fixture is",importSeasonType)
            print("seasonType from AthleteDB is", seasonType)
            print("revert back to seasonType from fixture")
            seasonType = importSeasonType
        if 'name' in season:
            seasonName = season['name']
        else:
            seasonName = 'none'
    else:
        seasonName = 'none'
        seasonType = 'none'
        seasonYear = 'none'
    nPlayers = 0
    if 'athletes' in roster:
        athletes = roster['athletes']
        nPlayers = len(athletes)
        for athlete in athletes:
            athlete['timestamp'] = timestamp
            athlete['status'] = status
            athlete['seasonYear'] = seasonYear
            athlete['seasonType'] = seasonType
            athlete['seasonName'] = seasonName
            athlete['index'] = iAthlete
            athlete['league'] = leagueMidsizeName
            athlete['teamId'] = teamId
            athlete['teamName'] = teamName
            if 'statistics' in athlete:
                statistics=athlete['statistics']['splits']['categories']
                stats={}
                for category in statistics:
                    for stat in category['stats']:
                        stats[stat['name']]={'category':category['name'],'value':stat['value'],
                                             'displayValue':stat['displayValue']}
                        if stat['name'] not in statNames:
                            statNames[stat['name']]={'category':category['name'],
                                                    'name':stat['name'],
                                                    'displayName':stat['displayName'],
                                                    'shortDisplayName': stat['shortDisplayName'],
                                                    'description': stat['description']}
                athlete['statistics']=stats
            athletesDB.append(athlete)
            iAthlete += 1
        print("league", k, ' of ', nLeague, leagueMidsizeName,
              'team ', n, 'of ', nTeams, 'no of athletes ',nPlayers,
              'teamId= ', teamId,
              'total athletes',iAthlete, teamName,
              seasonType, seasonName, seasonYear)
    return(athletesDB,iAthlete,statNames)
def scanFixture(directory, scanFileName, start_date, end_date, Progress):
    #
    #  Append event id to updatedId.  These are the event id's to be imported.
    #  Append event json to fixtures for all events between db_start_date to db_end_date
    #
    dir1 = directory + "fixture/all/"
    dir2 = directory
    fileList = []
    dirList = []
    for f in os.listdir(dir1):
        myDate = date(int(f[0:4]), int(f[4:6]), int(f[6:8]))
        if start_date <= myDate <= end_date:
            if isfile(join(dir1, f)):
                fileList.append(f)
            else:
                dirList.append(f)
    nTotal = len(fileList)
    print(nTotal)
    n = 0
    fixtures = []
    currentTime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    if Progress == 'Yes':
        printProgressBar(0, nTotal, prefix='open fixture', suffix='complete.')
    for f in fileList:
        filename = dir1 + f
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                Response = json.load(file)
            file.close()
            events = Response
            for event in events:
                event['fileName'] = f
                event['updateTime'] = currentTime
                fixtures.append(event)
        n += 1
        if Progress == 'Yes':
            printProgressBar(n, nTotal, prefix='open fixture', suffix='complete.')
    i = 0
    fixture = {}
    duplicateIndex = []
    for fixtureTmp in fixtures:
        id = fixtureTmp['id']
        # print (fixture[filter1],fixture[filter2])
        if id not in fixture.keys():
            fixture[int(id)] = fixtureTmp
        else:
            i += 1
            print('duplicate', i, fixtureTmp['id'], fixtureTmp['date'], fixture[fixtureTmp['id']]['date'])

    #
    # save fixtures json data to all_fixtures_json.txt'
    #
    allFixtureJsonFileName = dir2 + scanFileName
    with open(allFixtureJsonFileName, 'w') as file:
        json.dump(fixtures, file)
    file.close()
    return (fixture)
