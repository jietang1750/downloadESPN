import json
import operator
import os
import csv
import time
import datetime
from dateutil import tz
from datetime import timedelta, date
import sqlConn

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def import_event_from_hd(id,dir):
    event={}
    filelist = [f for f in os.listdir(dir)]
    filename=str(id)+'.txt'
    if filename in filelist:
        with open (filename,'r') as file:
            Response=json.load(file)
        file.close()
        event=Response
    else:
        event['code']=404
        event['detail']='file not found'
    #print(Response.keys())
    return (event)

def score(goalScored, status):
    goalStatusList = ['STATUS_FULL_TIME', 'STATUS_FINAL_PEN', 'STATUS_FINAL_AET', 'STATUS_FINAL_AGT',
                      'STATUS_IN_PROGRESS']
    noGoalStatusList = ['STATUS_ABANDONED', 'STATUS_CANCELED', 'STATUS_POSTPONED', 'STATUS_SCHEDULED']
    if status in goalStatusList:
        return (goalScored)
    elif status in noGoalStatusList:
        return ('none')
    else:
        return ('StatusUnknown')

def homeAwayScore(homeGoal,awayGoal, status):
    goalStatusList = ['STATUS_FULL_TIME', 'STATUS_FINAL_PEN', 'STATUS_FINAL_AET', 'STATUS_FINAL_AGT']
    noGoalStatusList = ['STATUS_ABANDONED', 'STATUS_CANCELED', 'STATUS_POSTPONED', 'STATUS_SCHEDULED',
                        'STATUS_IN_PROGRESS']
    homeCleanSheet = 0
    awayCleanSheet = 0
    if status in goalStatusList:
        tmpScore=homeGoal +':' + awayGoal
        if int(homeGoal) == 0:
            homeCleanSheet = 0
            awayCleanSheet = 1
        if int(awayGoal) == 0:
            homeCleanSheet = 1
            awayCleanSheet = 0
        return (tmpScore,homeCleanSheet,awayCleanSheet)
    elif status in noGoalStatusList:
        return ('none',homeCleanSheet,awayCleanSheet)
    else:
        return ('StatusUnknown',homeCleanSheet,awayCleanSheet)

def utc2est(strTimeUTC):

    from_zone=tz.gettz('UTC')
    to_zone=tz.gettz('America/New_York')

    utc=datetime.datetime.strptime(strTimeUTC, '%Y-%m-%d %H:%M:%S')
    utc=utc.replace(tzinfo=from_zone)
    est=utc.astimezone(to_zone)
    return est.strftime('%Y-%m-%d %H:%M:%S')

def utc2uk(strTimeUTC):

    from_zone=tz.gettz('UTC')
    to_zone=tz.gettz('Europe/London')

    utc=datetime.datetime.strptime(strTimeUTC, '%Y-%m-%d %H:%M:%S')
    utc=utc.replace(tzinfo=from_zone)
    est=utc.astimezone(to_zone)
    return est.strftime('%Y-%m-%d %H:%M:%S')

def utc2cet(strTimeUTC):

    from_zone=tz.gettz('UTC')
    to_zone=tz.gettz('Europe/Paris')

    utc=datetime.datetime.strptime(strTimeUTC, '%Y-%m-%d %H:%M:%S')
    utc=utc.replace(tzinfo=from_zone)
    cet=utc.astimezone(to_zone)
    return cet.strftime('%Y-%m-%d %H:%M:%S')

def utc2eet(strTimeUTC):

    from_zone=tz.gettz('UTC')
    to_zone=tz.gettz('Europe/Bucharest')

    utc=datetime.datetime.strptime(strTimeUTC, '%Y-%m-%d %H:%M:%S')
    utc=utc.replace(tzinfo=from_zone)
    eet=utc.astimezone(to_zone)
    return eet.strftime('%Y-%m-%d %H:%M:%S')

def uk2utc(strTimeUK):

    from_zone=tz.gettz('Europe/London')
    to_zone=tz.gettz('UTC')

    uk=datetime.datetime.strptime(strTimeUK, '%Y-%m-%d %H:%M:%S')
    uk=uk.replace(tzinfo=from_zone)
    utc=uk.astimezone(to_zone)
    return utc.strftime('%Y-%m-%d %H:%M:%S')

def cet2utc(strTimeUK):

    from_zone=tz.gettz('Europe/Paris')
    to_zone=tz.gettz('UTC')

    cet=datetime.datetime.strptime(strTimeUK, '%Y-%m-%d %H:%M:%S')
    cet=cet.replace(tzinfo=from_zone)
    utc=cet.astimezone(to_zone)
    return utc.strftime('%Y-%m-%d %H:%M:%S')

def eet2utc(strTimeUK):

    from_zone=tz.gettz('Europe/Bucharest')
    to_zone=tz.gettz('UTC')

    eet=datetime.datetime.strptime(strTimeUK, '%Y-%m-%d %H:%M:%S')
    eet=eet.replace(tzinfo=from_zone)
    utc=eet.astimezone(to_zone)
    return utc.strftime('%Y-%m-%d %H:%M:%S')

def uk2est(strTimeUK):

    from_zone=tz.gettz('Europe/London')
    to_zone=tz.gettz('America/New_York')

    uk=datetime.datetime.strptime(strTimeUK, '%Y-%m-%d %H:%M:%S')
    uk=uk.replace(tzinfo=from_zone)
    est=uk.astimezone(to_zone)
    return est.strftime('%Y-%m-%d %H:%M:%S')

def est2UK(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('Europe/London')
    from_zone=tz.gettz('America/New_York')

    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    uk=est.astimezone(to_zone)
    return uk.strftime(strTimeFormatOut)

def est2cet(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('Europe/Paris')
    from_zone=tz.gettz('America/New_York')

    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    cet=est.astimezone(to_zone)
    return cet.strftime(strTimeFormatOut)

def est2eet(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('Europe/Bucharest')
    from_zone=tz.gettz('America/New_York')

    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    eet=est.astimezone(to_zone)
    return eet.strftime(strTimeFormatOut)

def est2msk(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('Europe/Moscow')
    from_zone=tz.gettz('America/New_York')

    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    msk=est.astimezone(to_zone)
    return msk.strftime(strTimeFormatOut)

def est2cst(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('America/Chicago')
    from_zone=tz.gettz('America/New_York')
    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    uk=est.astimezone(to_zone)
    return uk.strftime(strTimeFormatOut)

def est2mst(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('America/Denver')
    from_zone=tz.gettz('America/New_York')
    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    uk=est.astimezone(to_zone)
    return uk.strftime(strTimeFormatOut)

def est2pst(strTimeEST,strTimeFormatIn,strTimeFormatOut):
    #strTimeFormatIn='%Y/%m/%d %I:%M:%S %p'
    #strTimeFormatOut='%Y/%m/%d %I:%M:%S %p'

    to_zone=tz.gettz('America/Los_Angeles')
    from_zone=tz.gettz('America/New_York')
    est=datetime.datetime.strptime(strTimeEST, strTimeFormatIn)
    est=est.replace(tzinfo=from_zone)
    uk=est.astimezone(to_zone)
    return uk.strftime(strTimeFormatOut)

def getEventSummaryFromRoster(id,roster):
    eventSummary={}
    eventSummary['id'] = id
    homeAway = roster['homeAway']
    if homeAway == 'home':
        eventSummary['teamName'] = homeTeam
        eventSummary['teamId'] = homeTeamId
    if homeAway == 'away':
        eventSummary['teamName'] = awayTeam
        eventSummary['teamId'] = awayTeamId

    eventSummary['homeAway'] = homeAway
    if 'formation' in roster:
        formation = roster['formation']
    else:
        formation = 'none'
    # print('Formation:',formation)
    eventSummary['formation'] = formation

    winner = roster['winner']
    # print(roster.keys())
    # print("Line-Up", homeAway, winner)
    if 'uniform' in roster:
        uniform = roster['uniform']
        if 'alternateColor' in uniform:
            # print('Jersey:',uniform['type'],'color:',uniform['color'],
            #  'alternate color:',uniform['alternateColor'])
            eventSummary['jerseyType'] = uniform['type']
            eventSummary['jerseyColor'] = uniform['alternateColor']
        if 'color' in uniform:
            # print('Jersey:', uniform['type'], 'color:', uniform['color'])
            eventSummary['jerseyType'] = uniform['type']
            eventSummary['jerseyColor'] = uniform['color']
    else:
        eventSummary['jerseyType'] = 'unknown'
        eventSummary['jerseyColor'] = 'ffffff'

    return(eventSummary)

def leagueTable(fixtures):
    table = {}
    tableHome = {}
    tableAway = {}
    for tmpFixture in fixtures:
        #print(tmpFixtures)
        id = tmpFixture['id']
        hometeamId = tmpFixture['hometeamId']
        awayteamId = tmpFixture['awayteamId']
        strHomeGoal = tmpFixture['homegoal']
        strAwayGoal = tmpFixture['awaygoal']
        hometeam = tmpFixture['hometeam']
        awayteam = tmpFixture['awayteam']
        status = tmpFixture['status']
        updateTime = tmpFixture['updateTime']
        if hometeamId not in table.keys():
            table[hometeamId] ={
                'team':hometeam,
                'rank': 0,
                'mp': 0,
                'pts':0,
                'win':0,
                'draw':0,
                'loss':0,
                'gf':0,
                'ga':0,
                'gd':0,
                'cleansheet':0,
                'updateTime': updateTime}
        if hometeamId not in tableHome.keys():
            tableHome[hometeamId] = {
                'team':hometeam,
                'rank': 0,
                'mp': 0,
                'pts': 0,
                'win': 0,
                'draw': 0,
                'loss': 0,
                'gf': 0,
                'ga': 0,
                'gd': 0,
                'cleansheet': 0,
                'updateTime': updateTime}
        if awayteamId not in table.keys():
            table[awayteamId] ={
                'team':awayteam,
                'rank': 0,
                'mp': 0,
                'pts':0,
                'win':0,
                'draw':0,
                'loss':0,
                'gf':0,
                'ga':0,
                'gd':0,
                'cleansheet':0,
                'updateTime': updateTime}
        if awayteamId not in tableAway.keys():
            tableAway[awayteamId] = {
                'team':awayteam,
                'rank': 0,
                'mp': 0,
                'pts': 0,
                'win': 0,
                'draw': 0,
                'loss': 0,
                'gf': 0,
                'ga': 0,
                'gd': 0,
                'cleansheet': 0,
                'updateTime': updateTime}
        if strHomeGoal != 'none' and strAwayGoal != 'none':
            homegoal = int(strHomeGoal)
            awaygoal = int(strAwayGoal)
            table[hometeamId]['mp'] = table[hometeamId]['mp'] + 1
            table[awayteamId]['mp'] = table[awayteamId]['mp'] + 1
            tableHome[hometeamId]['mp'] = tableHome[hometeamId]['mp'] + 1
            tableAway[awayteamId]['mp'] = tableAway[awayteamId]['mp'] + 1
            tableHome[hometeamId]['gf'] = tableHome[hometeamId]['gf'] + homegoal
            tableHome[hometeamId]['ga'] = tableHome[hometeamId]['ga'] + awaygoal
            table[hometeamId]['gf'] = table[hometeamId]['gf'] + homegoal
            table[awayteamId]['ga'] = table[awayteamId]['ga'] + homegoal
            tableAway[awayteamId]['gf'] = tableAway[awayteamId]['gf'] + awaygoal
            tableAway[awayteamId]['ga'] = tableAway[awayteamId]['ga'] + homegoal
            table[awayteamId]['gf'] = table[awayteamId]['gf'] + awaygoal
            table[hometeamId]['ga'] = table[hometeamId]['ga'] + awaygoal
            table[hometeamId]['gd'] = table[hometeamId]['gf'] - table[hometeamId]['ga']
            table[awayteamId]['gd'] = table[awayteamId]['gf'] - table[awayteamId]['ga']
            tableHome[hometeamId]['gd'] = tableHome[hometeamId]['gf'] - tableHome[hometeamId]['ga']
            tableAway[awayteamId]['gd'] = tableAway[awayteamId]['gf'] - tableAway[awayteamId]['ga']
            if homegoal > awaygoal:
                table[hometeamId]['win'] = table[hometeamId]['win'] + 1
                table[awayteamId]['loss'] = table[awayteamId]['loss'] + 1
                table[hometeamId]['pts'] = table[hometeamId]['pts'] + 3
                tableHome[hometeamId]['win'] = tableHome[hometeamId]['win'] + 1
                tableAway[awayteamId]['loss'] = tableAway[awayteamId]['loss'] + 1
                tableHome[hometeamId]['pts'] = tableHome[hometeamId]['pts'] + 3
            elif homegoal < awaygoal:
                table[hometeamId]['loss'] = table[hometeamId]['loss'] + 1
                table[awayteamId]['win'] = table[awayteamId]['win'] + 1
                table[awayteamId]['pts'] = table[awayteamId]['pts'] + 3
                tableHome[hometeamId]['loss'] = tableHome[hometeamId]['loss'] + 1
                tableAway[awayteamId]['win'] = tableAway[awayteamId]['win'] + 1
                tableAway[awayteamId]['pts'] = tableAway[awayteamId]['pts'] + 3
            else:
                table[hometeamId]['draw'] = table[hometeamId]['draw'] + 1
                table[awayteamId]['draw'] = table[awayteamId]['draw'] + 1
                table[hometeamId]['pts'] = table[hometeamId]['pts'] + 1
                table[awayteamId]['pts'] = table[awayteamId]['pts'] + 1
                tableHome[hometeamId]['draw'] = tableHome[hometeamId]['draw'] + 1
                tableAway[awayteamId]['draw'] = tableAway[awayteamId]['draw'] + 1
                tableHome[hometeamId]['pts'] = tableHome[hometeamId]['pts'] + 1
                tableAway[awayteamId]['pts'] = tableAway[awayteamId]['pts'] + 1
            if awaygoal == 0:
                table[hometeamId]['cleansheet'] = table[hometeamId]['cleansheet'] + 1
                tableHome[hometeamId]['cleansheet'] = tableHome[hometeamId]['cleansheet'] + 1
            if homegoal == 0:
                table[awayteamId]['cleansheet'] = table[awayteamId]['cleansheet'] + 1
                tableAway[awayteamId]['cleansheet'] = tableAway[awayteamId]['cleansheet'] + 1
    return(sortLeagueTable(table),sortLeagueTable(tableHome),sortLeagueTable(tableAway))

def sortLeagueTable(table):
    tableSortedTuple = sorted(table.items(),key = lambda k:
                            (k[1]['team']))
    tableSortedTuple = sorted(tableSortedTuple,key = lambda k:
                            (k[1]['pts'], k[1]['gd'], k[1]['gf']), reverse=True)
    i=0
    tableSorted = {}
    for key, value in tableSortedTuple:
        value['rank'] = i + 1
        tableSorted[key] = value
        i += 1
        #print(key,value)
    #print()
    return(tableSorted)

def importOddsBet365(item,eventId,homeTeam,homeTeamId,awayTeam,awayTeamId):
    oddsProvider='Bet365'
    oddsProviderId=2000
    awayTeamOdds = {}
    if ('awayTeamOdds') in item:
        awayTeamOddsSummary=item['awayTeamOdds']['odds']['summary']
        awayTeamOddsValue=item['awayTeamOdds']['odds']['value']
        awayTeamOddsHandicap=item['awayTeamOdds']['odds']['handicap']
        awayTeamOdds['awayTeam']=awayTeam
        awayTeamOdds['awayTeamId']=awayTeamId
        awayTeamOdds['summary']=awayTeamOddsSummary
        awayTeamOdds['value']=awayTeamOddsValue
        awayTeamOdds['handicap']=awayTeamOddsHandicap

    homeTeamOdds = {}
    if ('homeTeamOdds') in item:
        homeTeamOddsSummary=item['homeTeamOdds']['odds']['summary']
        homeTeamOddsValue=item['homeTeamOdds']['odds']['value']
        homeTeamOddsHandicap=item['homeTeamOdds']['odds']['handicap']
        awayTeamOdds['homeTeam']=homeTeam
        awayTeamOdds['homeTeamId']=homeTeamId
        homeTeamOdds['summary']=homeTeamOddsSummary
        homeTeamOdds['value']=homeTeamOddsValue
        homeTeamOdds['handicap']=homeTeamOddsHandicap

    if 'bettingOdds' in item:
        bettingOdds = item['bettingOdds']
        teamOdds = bettingOdds['teamOdds']
        playerOdds = bettingOdds['playerOdds']
    else:
        teamOdds={}
        playerOdds={}
    for key1 in teamOdds:
        teamOddsItemOddsId=teamOdds[key1]['oddId']
        teamOddsItemOddsValue=teamOdds[key1]['value']
        teamOddsItemBetSlipUrl=teamOdds[key1]['betSlipUrl']
        #print (teamOddsItemOddsId,key1,teamOddsItemOddsValue,teamOddsItemBetSlipUrl)
    #print()
    if 'preMatchFirstGoalScorer' in playerOdds:
        preMatchFirstGoalScorer = playerOdds['preMatchFirstGoalScorer']
        for player in preMatchFirstGoalScorer:
            playerName=player['player']
            playerOddId = player['oddId']
            if 'value' in player:
                playerOddsValue=player['value']
                #print ('preMatchFirstGoalScorer',playerOddId,playerName,playerOddsValue)
       #     else:
                #print ('preMatchFirstGoalScorer',playerOddId,playerName,'none')
       # print()

    if 'preMatchAnyTimeGoalScorer' in playerOdds:
        preMatchAnyTimeGoalScorer = playerOdds['preMatchAnyTimeGoalScorer']
        for player in preMatchAnyTimeGoalScorer:
            playerName=player['player']
            playerOddId = player['oddId']
            if 'value' in player:
                playerOddsValue=player['value']
                #print ('preMatchAnyTimetGoalScorer',playerOddId,playerName,playerOddsValue)
            #else:
                #print ('preMatchAnyTimetGoalScorer',playerOddId,playerName,'none')
        #print()
    if 'preMatchLastGoalScorer' in playerOdds:
        preMatchLastGoalScorer = playerOdds['preMatchLastGoalScorer']
        for player in preMatchLastGoalScorer:
            playerName=player['player']
            playerOddId = player['oddId']
            if 'value' in player:
                playerOddsValue=player['value']
                #print ('preMatchLastGoalScorer',playerOddId,playerName,playerOddsValue)
            #else:
                #print('preMatchLastGoalScorer', playerOddId, playerName, 'none')

    combinedOddsSummary=[]
    combinedTeamOdds=[]
    combinedPlayerOdds=[]
    tmpOdds={'id':eventId,'oddsProvider':oddsProvider,'oddsProviderId':oddsProviderId,
             'teamOdds':{'homeTeamOdds':homeTeamOdds,'awayTeamOdds':awayTeamOdds}}
    combinedOddsSummary.append(tmpOdds)
    tmpOdds={'id':eventId,'oddsProvider':oddsProvider,'oddsProviderId':oddsProviderId,
             'teamOdds':teamOdds}
    combinedTeamOdds.append(tmpOdds)
    tmpOdds={'id':eventId,'oddsProvider':oddsProvider,'oddsProviderId':oddsProviderId,
             'playerOdds':playerOdds}
    combinedPlayerOdds.append(tmpOdds)
    return(combinedOddsSummary,combinedTeamOdds,combinedPlayerOdds)

def clockDisplayValueToMinutes(strClockDisplayValue):
    if '+' in strClockDisplayValue:
        regPlayTime = int(strClockDisplayValue.split('+')[0].strip("'"))
        injPlayTime = int(strClockDisplayValue.split('+')[1].strip("'"))
    else:
        regPlayTime = int(strClockDisplayValue.strip("'"))
        injPlayTime = 0
    # print(tmpPlay['clock']['displayValue'],regPlayTime,injPlayTime)
    return(regPlayTime,injPlayTime)


#value1='ENG.1'
#value1='ENG.2'
#value1='ENG.3'
#value1='ENG.4'
#value1='ENG.5'
#value1='GER.1'
#value1='UEFA.CHAMPIONS'
#value1='UEFA.EUROPA'
#value1='ENG.FA'
#value1='ITA.1'
#value1='ESP.1'
#value1='FRA.1'
#value1='ENG.LEAGUE_CUP'
#value1='CHN.1'
#value1='CHN.1.PROMOTION.RELEGATION'
#value1='USA.1'

#leagueList = ['ENG.1']
#leagueList = ['ENG.1','ENG.2','ESP.1','GER.1','FRA.1','ITA.1','FIFA.WORLD']
#leagueList = ['FIFA.WORLD']
#leagueList = ['FIFA.WWC']

#leagueList=['ENG.1',
# 'ENG.2',
# 'ENG.3',
# 'ENG.4',
# 'ENG.5',
# 'GER.1',
# 'UEFA.CHAMPIONS',
# 'UEFA.EUROPA',
# 'ENG.FA',
# 'ITA.1',
# 'ESP.1',
# 'FRA.1',
# 'ENG.LEAGUE_CUP',
# 'CHN.1',
# 'CHN.1.PROMOTION.RELEGATION',
# 'USA.1','UEFA.EURO']


#with open('configESPN.json','r') as file:
with open('config_db.json','r') as file:
    Response = json.load(file)
file.close()
print(Response)
rootDir=Response['rootDir']
Progress=Response['Progress']
year = Response['year']
osStr = Response['osStr']

#leagueList = ['ENG.1','ENG.2','ENG.3','ESP.1','GER.1','FRA.1','ITA.1']
#leagueList = ['ENG.1','ENG.2','ENG.3','ESP.1','GER.1','FRA.1','ITA.1','KSA.1','TUR.1',
#              'UEFA.CHAMPIONS','UEFA.EUROPA','UEFA.EUROPA.CONF']
leagueList = ['ENG.1','ENG.2','ENG.3','ESP.1','GER.1','FRA.1','ITA.1','KSA.1','TUR.1']
#leagueList = ['UEFA.EURO']
#leagueList = ['TUR.1','KSA.1']

if leagueList == ['FIFA.WWC']:
    year = '2023'

if leagueList == ['UEFA.EURO']:
    year = '2024'

filter1='league'
filter2='season'
value2= year

start_date = date(int(year), 7, 1)
end_date = date(int(year)+1, 8, 1)

directory = rootDir + value2 + '/'
dataDirEvents = directory + "events/"

importLeagues = {12654:"ENG.1"}

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

hostName = mysqlDict['hostName']
userId = mysqlDict['userId']
pwd = mysqlDict['pwd']
dbName = mysqlDict['dbName']
odbcDriver = mysqlDict['odbcDriver']

#for value1 in leagueList:
for tmpSeasonType in importLeagues.keys():
    value1 = importLeagues[tmpSeasonType]
    print(value1)

    if osStr == "Windows":
        (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
    # print(conn)
    #directory = "D:/Users/Jie/Documents/Python Scripts/espn_soccer/"
    saveDir = directory + "output/export_data/" + value1 +"/"
    teamDir = directory + 'teams/' + value1 +'/'
    dataDirFixture = directory + "fixture/" + value1 +"/"
    fileNamePlayerId = saveDir + 'playerId.txt'

    sql1 = f"""SET @rownum:=0;
        SELECT 
            @rownum:=@rownum + 1 No, x.*
        FROM
            (SELECT 
                f1.seasonType,
                    f1.eventId,
                    f1.date,
                    CONCAT(t1.displayName, ' vs ', t2.displayName) AS Title,
                    v.fullName AS Venue,
                    f1.attendance,
                    t1.displayName AS homeTeam,
                    t2.displayName AS awayTeam,
                    f1.homeTeamScore,
                    f1.awayTeamScore,
                    t3.clockDisplayValue,
                    st.name
            FROM
                Fixtures f1
            INNER JOIN Leagues l ON f1.leagueId = l.id
            INNER JOIN StatusType st ON f1.statusId = st.id
            INNER JOIN Venues v ON f1.venueId = v.id
            INNER JOIN Teams t1 ON f1.homeTeamId = t1.teamId
            INNER JOIN Teams t2 ON f1.awayTeamId = t2.teamId
            LEFT JOIN (SELECT 
                ke.eventId, ke.typeId, ke.clockValue, ke.clockDisplayValue
            FROM
                KeyEvents ke
            INNER JOIN (SELECT 
                f2.eventId
            FROM
                Fixtures f2) a ON ke.eventId = a.eventid
            INNER JOIN Fixtures f3 ON f3.eventId = ke.eventId
            WHERE
                ke.typeId = 83) t3 ON t3.eventId = f1.eventId
            WHERE
                f1.seasonType = 12654) x
        ORDER BY date ASC;
        """

    i=0
    delim=','
    event1={}
    event2={}
    tmpTable={}
    val = (tmpSeasonType,)
    cursor.execute(sql1, val)
    nTotFixtures = cursor.rowcount
    rs = cursor.fetchall()
    fixture = {}
    for tmpFixture in rs:
        tmpEventId = rs[2]
        tmpMatchDate = rs[3]
        tmpName = rs[4]
        tmpVenue = rs[5]
        tmpAttendance = rs[6]
        tmpHomeTeam = rs[7]
        tmpAwayTeam = rs[8]
        tmpHomeTeamId = rs[9]
        tmpAwayTeamId = rs[10]
        tmpHomeScore = rs[11]
        tmpAwayScore = rs[12]
        tmpClockDisplay = rs[13]
        tmpStatus = rs[13]
        fixture[tmpEventId] = {'eventId':tmpEventId,
                               'date':tmpMatchDate,
                               'name':tmpName,
                               'venue':tmpVenue,
                               'homeTeam':tmpHomeTeam,
                               'awayTeam':tmpAwayTeam,
                               'homeTeamId':tmpHomeTeamId,
                               'awayTeamId':tmpAwayTeamId,
                               'homegoal':tmpHomeScore,
                               'awaygoal':tmpAwayScore,
                               'displayClock': tmpClockDisplay,
                               'status':tmpStatus
                               }
    conn.close()
    for id in fixture.keys():
        i += 1
        """
        filename1=dataDirFixture+fixture[id]['directory']+'/'+id+'.txt'
        filename2=dataDirEvents+id+".txt"
        if os.path.exists(filename1):
            with open(filename1,'r') as file:
                Response = json.load(file)
            file.close()
            hasEvents1=True
            event1[id] = Response
        #    print(id, event[id].keys())
        else:
            hasEvents1=False

        if os.path.exists(filename2):
            with open(filename2,'r') as file:
                Response = json.load(file)
            file.close()
            hasEvents2=True
            event2[id] = Response
        #    print(id, event[id].keys())
        else:
            hasEvents2=False
        """
        #print(id, fixture[id].keys())
        name=fixture[id]['name']
        date=fixture[id]['date']
        status=fixture[id]['status']
        if status == 'STATUS_FINAL_PEN':
            intRegGameTime = 90
            intExtraGameTime = 120
        else:
            intRegGameTime = 90
            intExtraGameTime = 90
        if 'venue' in fixture[id]:
            venue=fixture[id]['venue']
        else:
            venue='none'
        hometeam=fixture[id]['hometeam']
        awayteam=fixture[id]['awayteam']
        homegoal=str(fixture[id]['homegoal'])
        awaygoal=str(fixture[id]['awaygoal'])
        hometeamId=fixture[id]['hometeamId']
        awayteamId=fixture[id]['awayteamId']
        if hometeamId not in tmpTable.keys():
            tmpTable[hometeamId]=[0,0,0,0,0,0]
        if awayteamId not in tmpTable.keys():
            tmpTable[awayteamId]=[0,0,0,0,0,0]
        (tmpScore,homeCleanSheet,awayCleanSheet) = homeAwayScore(homegoal,awaygoal,status)
        if tmpScore != 'none' and tmpScore != 'statusUnknown':
            tmpTable[hometeamId][1] = tmpTable[hometeamId][1] + homeCleanSheet
            tmpTable[awayteamId][2] = tmpTable[awayteamId][2] + awayCleanSheet
            tmpTable[hometeamId][0] = tmpTable[hometeamId][0] + homeCleanSheet
            tmpTable[awayteamId][0] = tmpTable[awayteamId][0] + awayCleanSheet
            tmpTable[hometeamId][3] = tmpTable[hometeamId][3] + 1
            tmpTable[awayteamId][3] = tmpTable[awayteamId][3] + 1
            tmpTable[hometeamId][4] = tmpTable[hometeamId][4] + 1
            tmpTable[awayteamId][5] = tmpTable[awayteamId][5] + 1
        line=str(i)+delim+id+delim+date+delim+name+delim+venue+delim+status+delim+tmpScore+delim+hometeamId+delim+awayteamId
        #if hasEvents1:
        #    line=line+delim+"Has Events1"
        #else:
        #    line = line + delim + "No Events1"
        #if hasEvents2:
        #    line=line+delim+"Has Events2"
        #else:
        #    line = line + delim + "No Events2"
        print(line)

    cleanSheets=[]
    tmpDict={}
    print (tmpTable.keys())
    for teamId in tmpTable.keys():
        tmpDict = {}
        tmpDict['teamId']=teamId
        tmpDict['MP']=tmpTable[teamId][3]
        tmpDict['MPHome']=tmpTable[teamId][4]
        tmpDict['MPAway']=tmpTable[teamId][5]
        tmpDict['cleanSheets']=tmpTable[teamId][0]
        tmpDict['cleanSheetsHome']=tmpTable[teamId][1]
        tmpDict['cleanSheetsAway']=tmpTable[teamId][2]
        cleanSheets.append(tmpDict)

    bPrintStanding=False
    bPrintOdds=True
    bPrintForm=False
    bPrintRoster=True
    bPrintHeadToHeadGames=False
    bPrintStats=True

    if bPrintRoster:
        if os.path.isfile(fileNamePlayerId):
            with open(fileNamePlayerId,'r') as file:
                playerCounter=json.load(file)
            file.close()
            iPlayer=max(playerCounter.items(),key=operator.itemgetter(1))[1]
        else:
            iPlayer = 0
            playerCounter = {}
        totPlayTime = {}
        filename2=teamDir + value1 +".txt"
        with open (filename2,'r') as file:
           athletes=json.load(file)
        file.close()
        for athlete in athletes:
            athleteId=athlete['id']
            if athleteId not in playerCounter.keys():
                iPlayer += 1
                playerCounter[athleteId]=iPlayer
        print('iPlayer',iPlayer)

    if bPrintStats:
        teamStats = []

    if bPrintOdds:
        combinedOddsSummaryBet365=[]
        combinedTeamOddsBet365=[]
        combinedPlayerOddsBet365=[]

    if bPrintRoster:
        gameLineup = []
        gameLineup2 = []
        playerStats = []
        gamePlays = []

    for id in fixture:
        #print(fixture[id].keys())
        #print(event1[id].keys())
        #print(event2[id].keys())
        venue = fixture[id]['venue']
        homeTeam = fixture[id]['homeTeam']
        homeTeamId = fixture[id]['homeTeamId']
        awayTeam = fixture[id]['awayTeam']
        awayTeamId = fixture[id]['awayTeamId']
        homeTeamScore = fixture[id] ['homegoal']
        awayTeamScore = fixture[id] ['awaygoal']
        displayClock = fixture[id]['displayClock']
        status = fixture[id]['status']
        if displayClock != None:
            (intClock1, intClock2) = clockDisplayValueToMinutes(displayClock)
            intRegGameTime = intClock1 + intClock2
            intExtraGameTime = intClock1 + intClock2
        else:
            intRegGameTime = 90
            intExtGameTime = 120



        if id in event1:
            competitions=event1[id]['competitions'][0]
            statusDict = event1[id]['status']
            if 'type' in statusDict.keys():
                status=statusDict['type']['name']
            else:
                status='STATUS_UNKNOWN'
            if 'clock' in statusDict:
                clockTmp=statusDict['clock']
                displayClockTmp = statusDict['displayClock']
                #print(statusDict)
                if clockTmp > 0:
                    (intClock1, intClock2) = clockDisplayValueToMinutes(displayClockTmp)
                    intRegGameTime = intClock1 + intClock2
                    intExtraGameTime = intClock1 + intClock2
                else:
                    intRegGameTime = 90
                    intExtGameTime = 120
            else:
                intRegGameTime = 90
                intExtGameTime = 120
            eventId=competitions['id']
            uid=competitions['uid']
            date=competitions['date']
            attendance=competitions['attendance']
            if 'venue' in competitions:
                venue=competitions['venue']['fullName']
                venueId=competitions['venue']['id']
                if 'capacity' in competitions['venue']:
                    venueCapacity = competitions['venue']['capacity']
                if 'address' in competitions['venue']:
                    if 'country' in competitions['venue']['address']:
                        venueCountry = competitions['venue']['address']['country']
                    else:
                        venueCountry = 'none'
                else:
                    venueCountry = 'none'
            else:
                venue='none'
                venueCountry = 'none'
            competitors=competitions['competitors']
            homeAwayKey={}
            for competitor in competitors:
                homeAway=competitor['homeAway']
                if homeAway == 'home':
                    homeAwayKey['home']=0
                if homeAway == 'away':
                    homeAwayKey['away']=1
            winner=competitors[homeAwayKey['home']]['winner']
            if winner:
                result="Win"
            else:
                result="Loss"
            homeTeam=competitors[homeAwayKey['home']]['team']['name']
            homeTeamId=competitors[homeAwayKey['home']]['team']['id']
            awayTeam=competitors[homeAwayKey['away']]['team']['name']
            awayTeamId=competitors[homeAwayKey['away']]['team']['id']
            homeTeamScore=competitors[homeAwayKey['home']]['score']
            awayTeamScore=competitors[homeAwayKey['away']]['score']
            if 'statistics' in competitors[homeAwayKey['home']]:
                hasStat=True
                homeTeamStats=competitors[homeAwayKey['home']]['statistics']
            if 'statistics' in competitors[homeAwayKey['away']]:
                hasStat=True
                awayTeamStats=competitors[homeAwayKey['away']]['statistics']
            if 'form' in competitors[homeAwayKey['home']]:
                homeTeamForm=competitors[homeAwayKey['home']]['form']
            else:
                homeTeamForm=''
            if 'form' in competitors[homeAwayKey['away']]:
                awayTeamForm=competitors[homeAwayKey['away']]['form']
            else:
                awayTeamForm=''
            if 'records' in competitors[homeAwayKey['home']]:
                homeTeamRecord=competitors[homeAwayKey['home']]['records'][0]
                homeTeamRecordSummary = homeTeamRecord['summary']
            else:
                homeTeamRecord={}
                homeTeamRecordSummary = ''
            if 'records' in competitors[homeAwayKey['away']]:
                awayTeamRecord=competitors[homeAwayKey['away']]['records'][0]
                awayTeamRecordSummary = awayTeamRecord['summary']
            else:
                awayTeamRecord={}
                awayTeamRecordSummary = ''

            gameScore=str(homeTeamScore) + ":" + str(awayTeamScore)
            #print(id,uid,date,homeTeam,awayTeam,venue,gameScore)
        else:
            print(id,'not in event1')
        if id in event2:
            if 'boxscore' in event2[id]:
                boxscore = event2[id]['boxscore']
            if 'gameInfo' in event2[id]:
                gameInfo = event2[id]['gameInfo']
            if 'rosters' in event2[id]:
                rosters = event2[id]['rosters']
            if 'headToHeadGames' in event2[id]:
                headToHeadGames = event2[id]['headToHeadGames']
            if 'broadcasts' in event2[id]:
                broadcasts = event2[id]['broadcasts']
            if 'hasOdds' in event2[id]:
                hasOdds = event2[id]['hasOdds']
            if 'odds' in event2[id]:
                odds = event2[id]['odds']
            if 'standings' in event2[id]:
                standings = event2[id]['standings']
        else:
            print(id,'not in event1')

        teams=boxscore['teams']
        for team in teams:
            teamName=team['team']['name']
            if teamName == fixture[id]['hometeam']:
                if 'statistics' in team:
                    hasHomeTeamStat2 = True
                    hasStat = True
                    homeTeamStats2=team['statistics']
                else:
                    hasHomeStat = False
                    hasHomeTeamStat2 = False
                    hasStat = False
            if teamName == fixture[id]['awayteam']:
                if 'statistics' in team:
                    hasAwayTeamStat2 = True
                    hasStat = True
                    awayTeamStats2=team['statistics']
                else:
                    hasAwayStat = False
                    hasAwayTeamStat2 = False
                    hasStat = False
        strEndRegularTime = 'End Regular Time'
        strEndExtraTime = 'End Extra Time'
        if 'keyEvents' in event2[id]:
            keyEvents=event2[id]['keyEvents']
            for keyEvent in keyEvents:
                keyEventType=keyEvent['type']['text']
                if keyEventType == strEndRegularTime:
                    keyEventClock = keyEvent['clock']['displayValue']
                    (intClock1,intClock2) = clockDisplayValueToMinutes(keyEventClock)
                    intRegGameTime=intClock1+intClock2
                if keyEventType == strEndExtraTime:
                    keyEventClock = keyEvent['clock']['displayValue']
                    (intClock1,intClock2) = clockDisplayValueToMinutes(keyEventClock)
                    intExtraGameTime=intClock1+intClock2

            if 'commentary' in event2[id]:
                commentary=event2[id]['commentary']


        #print('boxscore', boxscore.keys())
        #print('boxscore-form 0', boxscore['form'][0].keys())
        #print('boxscore-form 1', boxscore['form'][1].keys())
        #print('boxscore-form-team 0', boxscore['form'][0]['team'].keys())
        #print('boxscore-form-team 1', boxscore['form'][1]['team'].keys())
        #print('boxscore-form-events 0', boxscore['form'][0]['events'][0].keys())
        #print('boxscore-form-events 1', boxscore['form'][1]['events'][0].keys())
        #print('boxscore-teams 0', boxscore['teams'][0].keys())
        #print('boxscore-teams 1', boxscore['teams'][1].keys())
        #print('boxscore-teams-team 0', boxscore['teams'][0]['team'].keys())
        #print('boxscore-teams-team 1', boxscore['teams'][1]['team'].keys())
        gameInfo['id'] = id
        #print('gameInfo', gameInfo.keys())
        # print('venue',gameInfo['venue'].keys())
        #print('headToHeadGames 0', headToHeadGames[0].keys())
        #print('broadcasts', broadcasts)

        if bPrintOdds:
            #print('hasOdds', hasOdds)
            if hasOdds:
                #print('odds', odds)
                for item in odds:
                    if 'provider' in item:
                        providerId=item['provider']['id']
                        providerName=item['provider']['name']
                        #print(providerId,providerName)
                    if providerId=='2000':
                        (tmpCombinedOddsSummaryBet365,tmpCombinedTeamOddsBet365,tmpCombinedPlayerOddsBet365)=\
                            importOddsBet365(item,id,homeTeam,homeTeamId,awayTeam,awayTeamId)
                        combinedOddsSummaryBet365.append(tmpCombinedOddsSummaryBet365)
                        combinedTeamOddsBet365.append(tmpCombinedTeamOddsBet365)
                        combinedPlayerOddsBet365.append(tmpCombinedPlayerOddsBet365)
            else:
                odds = []
        #print('rosters', rosters[0].keys())
        #print('rosters', rosters[1].keys())
        # print('standings',standings.keys())
        #event = {'id': id, 'boxscore': boxscore, 'gameInfo': gameInfo, 'headToHeadGames': headToHeadGames,
        #         'broadcasts': broadcasts, 'odds': odds, 'rosters': rosters, 'standings': standings}

        #event=event2[id]
        #print(event['boxscore'])
        # print(event)
        if bPrintForm:
            for form in boxscore['form']:
                team = form['team']
                #print(team['id'], team['displayName'])
                for game in form['events']:
                    score = game['homeTeamScore'] + " : " + game['awayTeamScore']
                    leagueName = game['leagueName']
                    #print(score, game['gameResult'], game['homeTeamId'], game['awayTeamId'], game['opponent']['displayName'],
                    #      leagueName)
            for team in boxscore['teams']:
                name = team['team']['displayName']
                print(name)
                if 'statistics' in team:
                    statistics = team['statistics']
                    #for statistic in statistics:
                    #    print(statistic['name'],statistic['label'], statistic['displayValue'])

        if bPrintRoster:
            substitute={}
            substitute['name']='Substitute'
            substitute['displayName']='Substitute'
            substitute['abbreviation']='SUB'
            tbd_pos={}
            tbd_pos['name']='TBD'
            tbd_pos['displayName']='TBD'
            tbd_pos['abbreviation']='TBD'
            for roster in rosters:
                eventSummary=getEventSummaryFromRoster(id,roster)
                if eventSummary['homeAway'] == 'home':
                    homeEventSummary=eventSummary.copy()
                elif eventSummary['homeAway'] == 'away':
                    awayEventSummary = eventSummary.copy()
                if 'roster' in roster:
                    players=roster['roster']
                    n=0
                    for player in players:
                        tmpPlayer2={}
                        intRedCardTime=0
                        #print(player.keys())
                        #print(player['athlete'].keys())
                        starter = player['starter']
                        playerId = player['athlete']['id']
                        playerDisplayName = player['athlete']['displayName']
                        if playerId not in totPlayTime.keys():
                            totPlayTime[playerId]={}
                            totPlayTime[playerId]['totPlayTime']=0
                            totPlayTime[playerId]['totAppearance'] = 0
                            totPlayTime[playerId]['avePlayTime'] = 0
                        if playerId not in playerCounter.keys():
                            iPlayer += 1
                            playerCounter[playerId]=iPlayer
                        if starter:
                            playerPosition = player['position']
                            if status == 'STATUS_FULL_TIME':
                                tmpPlayer2['playtime'] = intRegGameTime
                                #print(status,intRegGameTime,intExtraGameTime)
                            else:
                                tmpPlayer2['playtime'] = intExtraGameTime
                                #print(status, intRegGameTime, intExtraGameTime)
                            tmpPlayer2['starter'] = True
                        else:
                            player['position']=substitute
                            playerPosition = substitute
                            tmpPlayer2['playtime'] = 0
                            tmpPlayer2['starter'] = False
                        if 'jersey' in player:
                            playerJersey = player['jersey']
                        else:
                            print(id, n)
                            playerJersey = ''
                            for tmpAthlete in athletes:
                                if playerId == tmpAthlete['id'] and value1 == tmpAthlete['league']:
                                    if 'jersey' in tmpAthlete:
                                        playerJersey = tmpAthlete['jersey']
                                    else:
                                        playerJersey = ''
                                    break
                        if 'plays' in player:
                            for play in player['plays']:
                                if play['redCard']:
                                    redCardClock=play['clock']['displayValue']
                                    (intClock1, intClock2) = clockDisplayValueToMinutes(redCardClock)
                                    intRedCardTime = intClock1 + intClock2
                                    tmpPlayer2['playtime'] = intRedCardTime

                        if 'subbedOut' in player:
                            subbedOut=player['subbedOut']
                            if type(subbedOut) is dict:
                                if 'didSub' in subbedOut.keys():
                                    player['subbedOut'] = subbedOut['didSub']
                                else:
                                    if not(isinstance(subbedOut,bool)):
                                        player['subbedOut']=False
                        else:
                            player['subbedOut'] = False
                        if 'subbedIn' in player:
                            subbedIn=player['subbedIn']
                            if type(subbedIn) is dict:
                                if 'didSub' in subbedIn.keys():
                                    player['subbedIn'] = subbedIn['didSub']
                                else:
                                    if not(isinstance(subbedIn,bool)):
                                        player['subbedIn']=False
                        else:
                            player['subbedIn'] = False
                        if 'stats' in player:
                            stats=player['stats']
                        #print('stats', player['stats'])
                        tmpPlayer2['eventId']=id
                        tmpPlayer2['athleteId']=playerId
                        tmpPlayer2['displayName']=playerDisplayName
                        tmpPlayer2['jersey']=playerJersey
                        tmpPlayer2['position']=playerPosition

                        if subbedIn and subbedOut:
                            subClock=[]
                            if 'plays' in player:
                                for play in player['plays']:
                                    if play['substitution']:
                                       subClock.append(play['clock']['displayValue'])
                            tmpPlayer2['subbedIn'] = subbedIn
                            tmpPlayer2['subbedOut'] = subbedOut
                            if 'subbedOutFor' in player:
                                tmpSubbedOutFor={}
                                if 'jersey' in player['subbedOutFor']:
                                    subPlayerJersey = player['subbedOutFor']['jersey']
                                else:
                                    print(id, n)
                                    subPlayerJersey = ''
                                    for tmpAthlete in athletes:
                                        if playerId == tmpAthlete['id'] and value1 == tmpAthlete['league']:
                                            if 'jersey' in tmpAthlete:
                                                sbuPlayerJersey = tmpAthlete['jersey']
                                            else:
                                                subPlayerJersey = ''
                                            break
                                subPlayer = player['subbedOutFor']['athlete']
                                subPlayerId = subPlayer['id']
                                subPlayerName = subPlayer['displayName']
                                subPlayerClock = subClock[1]
                                (regPlayTime,injPlayTime) = clockDisplayValueToMinutes(subPlayerClock)
                                clockValueOut=regPlayTime+injPlayTime
                                tmpSubbedOutFor['jersey'] = subPlayerJersey
                                tmpSubbedOutFor['athleteId'] = subPlayerId
                                tmpSubbedOutFor['displayName'] = subPlayerName
                                tmpSubbedOutFor['clock'] = subPlayerClock
                                tmpSubbedOutFor['clockValue']=clockValueOut
                                tmpPlayer2['subbedOutFor']=tmpSubbedOutFor
                            if 'subbedInFor' in player:
                                tmpSubbedInFor={}
                                if 'jersey' in player['subbedInFor']:
                                    subPlayerJersey = player['subbedInFor']['jersey']
                                else:
                                    print(id, n)
                                    subPlayerJersey = ''
                                    for tmpAthlete in athletes:
                                        if playerId == tmpAthlete['id'] and value1 == tmpAthlete['league']:
                                            if 'jersey' in tmpAthlete:
                                                sbuPlayerJersey = tmpAthlete['jersey']
                                            else:
                                                subPlayerJersey = ''
                                            break
                                subPlayer = player['subbedInFor']['athlete']
                                subPlayerId = subPlayer['id']
                                subPlayerName = subPlayer['displayName']
                                subPlayerClock = subClock[0]
                                (regPlayTime,injPlayTime) = clockDisplayValueToMinutes(subPlayerClock)
                                clockValueIn=regPlayTime+injPlayTime
                                tmpSubbedInFor['jersey'] = subPlayerJersey
                                tmpSubbedInFor['athleteId'] = subPlayerId
                                tmpSubbedInFor['displayName'] = subPlayerName
                                tmpSubbedInFor['clock'] = subPlayerClock
                                tmpSubbedInFor['clockValue']=clockValueIn
                                tmpPlayer2['subbedInFor']=tmpSubbedInFor
                            tmpPlayer2['playtime']= clockValueOut - clockValueIn
                            #print(status,clockValueOut,clockValueIn)
                        elif subbedOut:
                            subClock=[]
                            if 'plays' in player:
                                for play in player['plays']:
                                    if play['substitution']:
                                        subClock.append(play['clock']['displayValue'])
                            tmpPlayer2['subbedOut'] = subbedOut
                            if 'subbedOutFor' in player:
                                tmpSubbedOutFor={}
                                if 'jersey' in player['subbedOutFor']:
                                    subPlayerJersey=player['subbedOutFor']['jersey']
                                else:
                                    print(id, n)
                                    subPlayerJersey = ''
                                    for tmpAthlete in athletes:
                                        if playerId == tmpAthlete['id'] and value1 == tmpAthlete['league']:
                                            if 'jersey' in tmpAthlete:
                                                sbuPlayerJersey = tmpAthlete['jersey']
                                            else:
                                                subPlayerJersey = ''
                                            break
                                subPlayer=player['subbedOutFor']['athlete']
                                subPlayerId=subPlayer['id']
                                subPlayerName=subPlayer['displayName']
                                subPlayerClock = subClock[0]
                                (regPlayTime,injPlayTime) = clockDisplayValueToMinutes(subPlayerClock)
                                clockValueOut=regPlayTime+injPlayTime
                                tmpSubbedOutFor['jersey'] = subPlayerJersey
                                tmpSubbedOutFor['athleteId'] = subPlayerId
                                tmpSubbedOutFor['displayName'] = subPlayerName
                                tmpSubbedOutFor['clock'] = subPlayerClock
                                tmpSubbedOutFor['clockValue']=clockValueOut
                                tmpPlayer2['subbedOutFor'] = tmpSubbedOutFor
                                tmpPlayer2['playtime']= clockValueOut
                                #print('subbedOut',status,clockValueOut)
                            #print('plays',player['plays'][0])

                            #print('Start',player['starter'],player['jersey'],player['athlete']['fullName'],player['position']['name'],
                            #   'Subbed Out by',subPlayerJersey,subPlayerName, 'at', subPlayerClock)
                        elif subbedIn:
                            subClock=[]
                            if 'plays' in player:
                                for play in player['plays']:
                                    if play['substitution']:
                                        subClock.append(play['clock']['displayValue'])
                            tmpPlayer2['subbedIn'] = subbedIn
                            if 'subbedInFor' in player:
                                tmpSubbedInFor={}
                                if 'jersey' in player['subbedInFor']:
                                    subPlayerJersey=player['subbedInFor']['jersey']
                                else:
                                    print(id, n)
                                    subPlayerJersey = ''
                                    for tmpAthlete in athletes:
                                        if playerId == tmpAthlete['id'] and value1 == tmpAthlete['league']:
                                            if 'jersey' in tmpAthlete:
                                                sbuPlayerJersey = tmpAthlete['jersey']
                                            else:
                                                subPlayerJersey = ''
                                            break
                                subPlayer=player['subbedInFor']['athlete']
                                subPlayerId=subPlayer['id']
                                subPlayerName=subPlayer['displayName']
                                subPlayerClock = subClock[0]
                                (regPlayTime,injPlayTime) = clockDisplayValueToMinutes(subPlayerClock)
                                clockValueIn=regPlayTime+injPlayTime
                                tmpSubbedInFor['jersey'] = subPlayerJersey
                                tmpSubbedInFor['athleteId'] = subPlayerId
                                tmpSubbedInFor['displayName'] = subPlayerName
                                tmpSubbedInFor['clock'] = subPlayerClock
                                tmpSubbedInFor['clockValue']=clockValueIn
                                tmpPlayer2['subbedInFor']=tmpSubbedInFor
                                if intRedCardTime > 0:
                                    tmpPlayer2['playtime'] = intRedCardTime-clockValueIn
                                elif status == 'STATUS_FULL_TIME':
                                    tmpPlayer2['playtime'] = intRegGameTime-clockValueIn
                                    #print('subbedIn', status, intRegGameTime,clockValueIn)
                                else:
                                    tmpPlayer2['playtime'] = intExtraGameTime-clockValueIn
                                    #print('subbedIn', status,intExtraGameTime, clockValueIn)
                                    #print(tmpPlayer2['playtime'], intRedCardTime,intRegGameTime, intExtraGameTime,clockValueIn,status)
                        if tmpPlayer2['playtime'] >0:
                            totPlayTime[playerId]['totPlayTime'] =totPlayTime[playerId]['totPlayTime']+tmpPlayer2['playtime']
                            totPlayTime[playerId]['totAppearance'] =totPlayTime[playerId]['totAppearance']+1
                            totPlayTime[playerId]['avePlayTime'] =totPlayTime[playerId]['totPlayTime']/totPlayTime[playerId]['totAppearance']+1

                            #if clockValueIn >90:
                                #    tmpPlayer2['playtime'] = 1
                                #else:
                                #    tmpPlayer2['playtime'] = 90-clockValueIn

                                #print('plays',player['plays'][0])
                                #print('Start',player['starter'],player['jersey'],player['athlete']['fullName'],player['position']['name'],
                                #      'Subbed In for', subPlayerJersey, subPlayerName,'at', subPlayerClock)
                        #else:
                            #print('Start',player['starter'],player['jersey'],player['athlete']['fullName'],player['position']['name'])
                        tmpPlayer={}
                        tmpPlays={}
                        tmpStats={}
                        for key in eventSummary.keys():
                            tmpPlayer[key]=eventSummary[key]
                            tmpPlayer2[key]=eventSummary[key]
                            tmpPlays[key] = eventSummary[key]
                            tmpStats[key] = eventSummary[key]
                        for key in player.keys():
                            if key != 'stats' and key != 'plays':
                                tmpPlayer[key]=player[key]
                            if key == 'athlete':
                                tmpPlays[key] = player[key]
                                tmpStats[key] = player[key]
                            if key == 'jersey':
                                tmpPlays[key] = player[key]
                                tmpStats[key] = player[key]
                            if key == 'plays':
                                i=0
                                for tmpPlay in player[key]:
                                    if 'clock' in tmpPlay:
                                        strClock=tmpPlay['clock']['displayValue']
                                        if '+' in strClock:
                                            regPlayTime=int(strClock.split('+')[0].strip("'"))
                                            injPlayTime=int(strClock.split('+')[1].strip("'"))
                                        else:
                                            if "'" in strClock:
                                                regPlayTime = int(strClock.strip("'"))
                                            else:
                                                regPlayTime=90
                                            injPlayTime = 0
                                        #print(tmpPlay['clock']['displayValue'],regPlayTime,injPlayTime)
                                        player[key][i]['regularPlayTime'] = regPlayTime
                                        player[key][i]['injuryPlayTime'] = injPlayTime
                                    i +=1
                                tmpPlays[key] = player[key]
                            if key == 'stats':
                                for stat in stats:
                                    tmpStatName=stat['name']
                                    tmpStatValue=stat['value']
                                    if tmpStatName not in statisticsNames:
                                        tmpStatDisplayName=stat['displayName']
                                        tmpStatShortDisplayName=stat['shortDisplayName']
                                        tmpStatDescription=stat['description']
                                        tmpStatAbbreviation=stat['abbreviation']
                                        statisticsNames[tmpStatName]=[tmpStatShortDisplayName,tmpStatDisplayName,tmpStatDescription,
                                                                      tmpStatAbbreviation]
                                    tmpStats[tmpStatName]=tmpStatValue
                        if 'subbedIn' in tmpPlayer and 'subbedOut' in tmpPlayer:
                            bSubbedIn=tmpPlayer['subbedIn']
                            bSubbedOut = tmpPlayer['subbedOut']
                            if bSubbedIn==True and bSubbedOut==True:
                                tmpPlayerSub = {}
                                tmpPlayerSub=tmpPlayer.copy()
                                tmpPlayerSub['subbedIn']=False
                                tmpPlayerSub['subbedInFor'] ={}
                                tmpPlayer['subbedOut'] = False
                                tmpPlayer['subbedOutFor'] ={}
                                gameLineup.append(tmpPlayer)
                                gameLineup.append(tmpPlayerSub)
                                subClock={}
                                for tmpPlay in tmpPlays['plays']:
                                   if tmpPlay['substitution']:
                                       tmpClock=tmpPlay['clock']
                                       tmpRegularPlayTime=tmpPlay['regularPlayTime']
                                       tmpInjuryPlayTime=tmpPlay['injuryPlayTime']
                                       tmpClockTime=int(tmpRegularPlayTime)+int(tmpInjuryPlayTime)
                                       subClock[tmpClockTime]={'clock':tmpClock,'regularPlayTime':tmpRegularPlayTime,
                                                               'injuryPlayTime':tmpInjuryPlayTime}
                                subInTime=sorted (subClock.keys())[0]
                                subOutTime=sorted (subClock.keys())[1]
                                tmpPlayer['clock']=subClock[subInTime]['clock']
                                tmpPlayer['regularPlayTime']=subClock[subInTime]['regularPlayTime']
                                tmpPlayer['injuryPlayTime']=subClock[subInTime]['injuryPlayTime']
                                tmpPlayerSub['clock']=subClock[subOutTime]['clock']
                                tmpPlayerSub['regularPlayTime']=subClock[subOutTime]['regularPlayTime']
                                tmpPlayerSub['injuryPlayTime']=subClock[subOutTime]['injuryPlayTime']
                            else:
                                if 'plays' in tmpPlays:
                                    subClock={}
                                    for tmpPlay in tmpPlays['plays']:
                                        if tmpPlay['substitution']:
                                            tmpClock=tmpPlay['clock']
                                            tmpRegularPlayTime=tmpPlay['regularPlayTime']
                                            tmpInjuryPlayTime=tmpPlay['injuryPlayTime']
                                            tmpPlayer['clock']=tmpClock
                                            tmpPlayer['regularPlayTime']=tmpRegularPlayTime
                                            tmpPlayer['injuryPlayTime']=tmpInjuryPlayTime
                                gameLineup.append(tmpPlayer)
                        else:
                            subClock = {}
                            for tmpPlay in tmpPlays['plays']:
                                if tmpPlay['substitution']:
                                    tmpClock = tmpPlay['clock']
                                    tmpRegularPlayTime = tmpPlay['regularPlayTime']
                                    tmpInjuryPlayTime = tmpPlay['injuryPlayTime']
                                    tmpClockTime = int(tmpRegularPlayTime) + int(tmpInjuryPlayTime)
                                    subClock[tmpClockTime] = {'clock': tmpClock, 'regularPlayTime': tmpRegularPlayTime,
                                                              'injuryPlayTime': tmpInjuryPlayTime}
                            subTime = sorted(subClock.keys())[0]
                            tmpPlayer['clock'] = subClock[subTime]['clock']
                            tmpPlayer['regularPlayTime'] = subClock[subTime]['regularPlayTime']
                            tmpPlayer['injuryPlayTime'] = subClock[subTime]['injuryPlayTime']
                            gameLineup.append(tmpPlayer)
                        #gameLineup.append(tmpPlayer)
                        gamePlays.append(tmpPlays)
                        playerStats.append(tmpStats)
                        gameLineup2.append(tmpPlayer2)
                        n += 1

                fileName=saveDir+'statsNames.txt'
                tmpStatName=[]
                for name in statisticsNames:
                    name1 = {}
                    name1['name']=name
                    name1['shortDisplayName']=statisticsNames[name][0]
                    name1['displayName']=statisticsNames[name][1]
                    name1['description']=statisticsNames[name][2]
                    name1['abbreviation']=statisticsNames[name][3]
                    tmpStatName.append(name1)
                with open(fileName,'w') as file:
                    json.dump(tmpStatName,file)
                file.close()


        #print('gameInfo', gameInfo)
        if 'venue' in gameInfo:
            venue = gameInfo['venue']['fullName']
            if 'city' in gameInfo['venue']['address']:
                city = gameInfo['venue']['address']['city']
            else:
                city = 'none'
        else:
            venue = 'none'
            city = 'none'
        id = gameInfo['id']
        if 'attendance' in gameInfo:
            attendance = gameInfo['attendance']
            #print('gameInfo', venue, city, attendance, id)
        #else:
            #print('gameInfo', venue, city, 'tbd', id)
        if bPrintHeadToHeadGames:
            for headToHeadGame in headToHeadGames[0]['events']:
                date = headToHeadGame['gameDate']
                score = headToHeadGame['score']
                gameResult = headToHeadGame['gameResult']
                leagueName = headToHeadGame['leagueName']
                atVs = headToHeadGame['atVs']
                #print('headToHeadGame', date, gameResult, score, atVs, leagueName)

        if bPrintStats:
            # print()
            # print(homeTeam,"Game Stats:")
            if hasStat or hasHomeTeamStat2:
                homeStats={}
                homeStats['id']=id
                homeStats['team']=fixture[id]['hometeam']
                homeStats['teamId']=fixture[id]['hometeamId']
                homeStats['jerseyType']=homeEventSummary['jerseyType']
                homeStats['jerseyColor']=homeEventSummary['jerseyColor']
                homeStats['form'] = homeTeamForm
                homeStats['record'] = homeTeamRecordSummary
                homeStats['homeAway'] = 'home'
                teamStats.append(homeStats)
                for stat in homeTeamStats2:
                    name=stat['name']
                    label=stat['label']
                    displayValue=stat['displayValue']
                    # print(name,label,displayValue)
                    homeStats[name]=displayValue
                    if name not in teamStatsNames:
                        teamStatsNames[name]=stat['label']
            # print()
            # print(homeTeam,'Form',homeTeamForm)
            # print (homeTeam,'Record',homeTeamRecordSummary)
            # print()
            #print(awayTeam,"Game Stats:")
            if hasStat or hasAwayTeamStat2:
                awayStats = {}
                awayStats['id']=id
                awayStats['team']=fixture[id]['awayteam']
                awayStats['teamId']=fixture[id]['awayteamId']
                awayStats['jerseyType']=awayEventSummary['jerseyType']
                awayStats['jerseyColor']=awayEventSummary['jerseyColor']
                awayStats['form'] = awayTeamForm
                awayStats['record'] = awayTeamRecordSummary
                awayStats['homeAway'] = 'away'
                teamStats.append(awayStats)
                for stat in awayTeamStats2:
                    name=stat['name']
                    label = stat['label']
                    displayValue=stat['displayValue']
                    # print(name,label,displayValue)
                    awayStats[name] = displayValue
                    if name not in teamStatsNames:
                        teamStatsNames[name] = stat['label']
            # print()
            # print(awayTeam,'Form',awayTeamForm)
            # print (awayTeam,'Record',awayTeamRecordSummary)

        if bPrintStanding:
            standings = event2['standings']
            for group in standings['groups']:
            #    print(group)
                for entry in group['standings']['entries']:
                    team = entry['team']
                    teamId = entry['id']
                    teamStats = entry['stats']
                    for teamStat in teamStats:
                        if teamStat['name'] == 'gamesPlayed':
                            mp = teamStat['displayValue']
                        if teamStat['name'] == 'points':
                            pts = teamStat['displayValue']
                        if teamStat['name'] == 'rank':
                            rank = teamStat['displayValue']
                        if teamStat['name'] == 'pointDifferential':
                            gd = teamStat['displayValue']
                        if teamStat['name'] == 'All Splits' and teamStat['displayName'] == 'Overall':
                            summary = teamStat['displayValue']
                    #print(rank, team, mp, summary, pts, gd)
        #print (event2[id])


    fileName = saveDir + 'lineup.txt'
    with open(fileName, 'w') as file:
        json.dump(gameLineup, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'totPlayTime.txt'
    with open(fileName, 'w') as file:
        json.dump(totPlayTime, file)
    file.close()
    print(fileName)

    fileName = fileNamePlayerId
    with open(fileName, 'w') as file:
        json.dump(playerCounter, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'playerStats.txt'
    with open(fileName, 'w') as file:
        json.dump(playerStats, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'teamStats.txt'
    with open(fileName, 'w') as file:
        json.dump(teamStats, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'teamStatsNames.txt'
    with open(fileName, 'w') as file:
        json.dump(teamStatsNames, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'oddsSummaryBet365.txt'
    with open(fileName, 'w') as file:
        json.dump(combinedOddsSummaryBet365, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'oddsTeamBet365.txt'
    with open(fileName, 'w') as file:
        json.dump(combinedTeamOddsBet365, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'oddsPlayerBet365.txt'
    with open(fileName, 'w') as file:
        json.dump(combinedPlayerOddsBet365, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'cleanSheets.txt'
    with open(fileName, 'w') as file:
        json.dump(cleanSheets, file)
    file.close()
    print(fileName)
    

    #process gamelineup2
    newLineup = []
    for tmpPlayer in gameLineup2:
        #print(tmpPlayer)
        eventId = tmpPlayer['id']
        playerId = tmpPlayer['athleteId']
        playerName = tmpPlayer['displayName']
        playerJersey = tmpPlayer['jersey']
        homeAway = tmpPlayer['homeAway']
        teamName = tmpPlayer['teamName']
        playtime = tmpPlayer['playtime']
        position = tmpPlayer['position']['displayName']
        starter = tmpPlayer['starter']
        formation = tmpPlayer['formation']
        tmpNewPlayer = {'id': eventId,
                        'athleteId': playerId,
                        'playerName': playerName,
                        'teamName':teamName,
                        'jersey': playerJersey,
                        'starter':starter,
                        'position':position,
                        'homeAway': homeAway,
                        'formation':formation,
                        'playTime': playtime}
        if 'subbedIn' in tmpPlayer.keys():
            if isinstance(tmpPlayer['subbedIn'],dict):
                didSub = tmpPlayer['subbedIn']['didSub']
            elif tmpPlayer['subbedIn']:
                subbedInForPlayer= tmpPlayer['subbedInFor']
                subbedInForPlayerName = subbedInForPlayer['displayName']
                subbedInForPlayerJersey = subbedInForPlayer['jersey']
                subbedInClock = subbedInForPlayer['clock']
                tmpNewPlayer['subbedIn'] = 'Subbed In For'
                tmpNewPlayer['subbedInForName'] = subbedInForPlayerName
                tmpNewPlayer['subbedInForJersey'] = subbedInForPlayerJersey
                tmpNewPlayer['subbedInClock'] = subbedInClock
        if 'subbedOut' in tmpPlayer.keys():
            if isinstance(tmpPlayer['subbedOut'],dict):
                didSub = tmpPlayer['subbedOut']['didSub']
            elif tmpPlayer['subbedOut']:
                subbedOutForPlayer = tmpPlayer['subbedOutFor']
                subbedOutForPlayerName = subbedOutForPlayer['displayName']
                subbedOutForPlayerJersey = subbedOutForPlayer['jersey']
                subbedOutClock = subbedOutForPlayer['clock']
                tmpNewPlayer['subbedOut'] = 'Subbed Out By'
                tmpNewPlayer['subbedOutForName'] = subbedOutForPlayerName
                tmpNewPlayer['subbedOutForJersey'] = subbedOutForPlayerJersey
                tmpNewPlayer['subbedOutClock'] = subbedOutClock
        #print(tmpNewPlayer)
        newLineup.append(tmpNewPlayer)

    fileName = saveDir + 'lineup2.txt'
    with open(fileName, 'w') as file:
        json.dump(newLineup, file)
    file.close()
    print(fileName)


    #process plays
    newPlays = []
    for tmpPlay in gamePlays:
        #print(tmpPlay)
        eventId = tmpPlay['id']
        team = tmpPlay ['teamName']
        athlete = tmpPlay['athlete']
        playerId = athlete['id']
        if 'jersey' in tmpPlay:
            jersey = tmpPlay['jersey']
        else:
            jersey = 'none'
        homeAway = tmpPlay['homeAway']
        playerName = athlete['displayName']
        if 'plays' in tmpPlay.keys():
            playsByPlayer = tmpPlay['plays']
            #print(eventId, playerName, playsByPlayer)
            for playByPlayer in playsByPlayer:
                tmpPlayByPlayer = {'id': eventId,
                                   'teamName': team,
                                   'athleteId': playerId,
                                   'athleteName': playerName,
                                   'jersey': jersey,
                                   'homeAway': homeAway}
                for key in playByPlayer:
                    if key == 'clock':
                        clock = playByPlayer [key]['displayValue']
                        tmpPlayByPlayer['clock'] = clock
                        clockValue = playByPlayer ['regularPlayTime'] + playByPlayer['injuryPlayTime']
                        tmpPlayByPlayer['clockValue'] = clockValue
                    elif key == 'regularPlayTime':
                        regularPlayTime=playByPlayer[key]
                        tmpPlayByPlayer['regularPlayTime'] = regularPlayTime
                    elif key == 'injuryPlayTime':
                        injuryPlayTime=playByPlayer[key]
                        tmpPlayByPlayer['injuryPlayTime'] = injuryPlayTime
                    elif playByPlayer[key]:
                        tmpPlayByPlayer[key] = key
                #print(tmpPlayByPlayer)
                newPlays.append(tmpPlayByPlayer)

    fileName = saveDir + 'plays.txt'
    with open(fileName, 'w') as file:
        json.dump(newPlays, file)
    file.close()
    print(fileName)

    fileName = directory + value1 + '_fixtures_json.txt'
    with open(fileName, 'r') as file:
        tmpFixtures = json.load(file)
    file.close()
    print(fileName)

    (table,tableHome,tableAway) = leagueTable(tmpFixtures)
    #print(table)
    #print(tableHome)
    #print(tableAway)

    fileName = saveDir + 'leagueTable.txt'
    with open(fileName, 'w') as file:
        json.dump(table, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'leagueTableHome.txt'
    with open(fileName, 'w') as file:
        json.dump(tableHome, file)
    file.close()
    print(fileName)

    fileName = saveDir + 'leagueTableAway.txt'
    with open(fileName, 'w') as file:
        json.dump(tableAway, file)
    file.close()
    print(fileName)

