import json
import pandas as pd
import sqlConn
from datetime import datetime
from dateutil import tz
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import os
import sql_insert_all

def matchLists(list1,list2):
    matchedList = []
    n = 0
    min_score = 100
    tmpList2 = list2.copy()
    tmpList1 = list1.copy()
    tmpList = list1.copy()
    print(len(list1), len(list2))
    i = 0
    while len(tmpList1) > 0:
        j = 1
        for name1 in tmpList:
            if len(tmpList2)>0:
                highest = process.extractOne(name1, tmpList2)
                name2 = highest[0]
                score = highest[1]
                if score >= min_score or len(tmpList1) == 1 or len(tmpList2) == 0:
                    if score < 60:
                        n += 1
                    matchedList.append([name1,name2,score])
                    tmpList1.remove(name1)
                    tmpList2.remove(name2)
                    i += 1
                    #print(i, j, score, league1, ':', name1, '->', name2, ',', league2, min_score, len(tmpList1),len(tmpList2))
                #print(i, j, score, league1, ':', name1, '->', name2, ',', league2, min_score, len(tmpList1),len(tmpList2))
            if len(tmpList1) == 0:
                break
            j += 1
        if len(tmpList2) == 0:
            break
        tmpList = tmpList1.copy()
        min_score = min_score - 5
    if len(tmpList1)==0 and len(tmpList2)>0:
        for name2 in tmpList2:
            matchedList.append(['',name2,0])
            n +=1
    if len(tmpList2)==0 and len(tmpList1)>0:
        for name1 in tmpList1:
            matchedList.append([name1,'',0])
            n +=1
    return(n,matchedList)

def importPlayerDB(mysqlDict,tablename):
    userId = mysqlDict['userId']
    pwd = mysqlDict['pwd']
    hostName = mysqlDict['hostName']
    odbcDriver =mysqlDict['odbcDriver']
    dbName = mysqlDict['dbName']
    osStr = mysqlDict['osStr']
    if osStr == "Windows":
        (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    print(conn)
    playerDB_df = pd.DataFrame()
    sql1 = ("SELECT id,"
            "    fullName,"
            "    dateOfBirth,"
            "    citizenship"
            " FROM " + tablename + ";")
    try:
        cursor.execute(sql1)
        rs = cursor.fetchall()
        conn.close()  # close the connection
        playerList = []
        for row in rs:
            idESPN = int(row[0])
            fullNameESPN = row[1]
            dobESPNStr = row[2]
            citizenshipESPN = row[3]
            if dobESPNStr == '' or dobESPNStr == None:
                dobESPN = None
            else:
                dobESPN = datetime.strptime(dobESPNStr, "%Y-%m-%dT%H:%MZ").date()
            playerList.append({'playerIdESPN': idESPN,
                                   'fullNameESPN': fullNameESPN,
                                   'DoBESPN': dobESPN,
                                   'citizenshipESPN': citizenshipESPN})
        playerList.sort(key=lambda x: x['fullNameESPN'])
    except Exception as e:
        conn.close()
        print(str(e))
    return(playerList)

def importUnmatchedTM(osStr,hostName,userId,pwd,dbName,odbcDriver):
    if osStr == "Windows":
        (conn,cursor) = sqlConn.connectDB_ODBC(hostName,userId,pwd,dbName,odbcDriver)
    elif osStr == "Linux":
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    else:
        (conn, cursor) = sqlConn.connectDB(hostName,userId,pwd,dbName)
    print(conn)
    player_id_list = []
    sql1 = ("SELECT "
    " t1.player_id FROM PlayerDBTM t1"
    " left join PlayerIdTM t2"
    " on t1.player_id = t2.player_id_TM"
    " where t2.updateId is NULL"
    " order by t1.market_value_in_eur DESC;")
    try:
        cursor.execute(sql1)
        # print(rs)
        rs = cursor.fetchall()
        #print(rs)
        for row in rs:
            player_id_tmp = int(row[0])
            player_id_list.append(player_id_tmp)
        conn.close()  # close the connection
    except Exception as e:
        conn.close()
        print(str(e))
    return(player_id_list)
def saveMatchedList(idDictESPN, matchedPlayersFilename):
    matchedIdList = []
    for idESPN in idDictESPN.keys():
        idTM = idDictESPN[idESPN]["playerIdTM"]
        #playerFullNameTM = idDictESPN[idESPN]["fullNameTM"]
        #playerDoBTM = idDictESPN[idESPN]["DoBTM"]
        #ratio1 = idDictESPN[idESPN]["fuzzyRatio1"]
        #ratio2 = idDictESPN[idESPN]["fuzzyRatio2"]
        #DoBScore = idDictESPN[idESPN]["DoBScore"]
        score = idDictESPN[idESPN]["combinedScore"]
        tmpId = {"playerId": idESPN,
                 "player_id_TM": idTM,
                 "fuzzyScore": score}
        matchedIdList.append(tmpId)

    print(matchedPlayersFilename)
    with open(matchedPlayersFilename, 'w') as file:
        json.dump(matchedIdList, file)
    file.close()
    return ("complete")

def retrieveTMData(playersFilename):
    players_df = sqlConn.importCsvToDf(playersFilename)
    players_df['player_id'] = players_df['player_id'].astype("int")
    players_df['name'] = players_df['name'].fillna("").astype(object)
    players_df['country_of_citizenship'] = players_df['country_of_citizenship'].fillna("").astype(object)
    players_df['date_of_birth'] = players_df['date_of_birth'].fillna("").astype(object)
    print("players_df Info")
    print(players_df.info())

    #if bUnmatchedList:
    #    unmatched_player_list = importUnmatchedTM(osStr,hostName,userId,pwd,dbName,odbcDriver)
    #else:
    #    unmatched_player_list = players_df['player_id'].tolist()

    unmatched_player_list = players_df['player_id'].tolist()

    playerListTM = []
    for index, row in players_df.iterrows():
        idTM = int(row["player_id"])
        if idTM in unmatched_player_list:
            fullNameTM = row['name']
            citizenshipTM = row['country_of_citizenship']
            dobTMStr = row["date_of_birth"]
            # print(row)
            if dobTMStr == '' or dobTMStr == None:
                dobTM = None
            else:
                # print(dobTMStr)
                dobTM = datetime.strptime(dobTMStr, "%Y-%m-%d %H:%M:%S").date()
            # print(index, idTM, fullNameTM, dobTM)
            playerListTM.append({'playerIdTM': idTM,
                                 'fullNameTM': fullNameTM,
                                 'DoBTM': dobTM,
                                 'citizenshipTM': citizenshipTM})
    playerListTM.sort(key=lambda x: x['fullNameTM'])
    return(playerListTM)

def matchPlayers(playerListESPN,playerListTM,matchedPlayersFilename):

    nTotalESPN = len(playerListESPN)
    nTotalTM = len(playerListTM)

    nCompare = 0
    nInterval = 10000
    ratio1 = 0
    ratio2 = 0
    n = 0
    m = 0
    k = 0
    i = 0
    idDictESPN = {}
    matchedId = {}
    #ileft = len(playerListESPN)
    ileft = nTotalESPN
    matchScore_lc = 115
    matchScore_mc = 145
    matchScore_hc = 175
    # matchScore_hc = 100     #try 100
    matchScore_country = 50
    for tmpPlayer in playerListESPN:
        i += 1
        playerId = tmpPlayer['playerIdESPN']
        playerFullName = tmpPlayer['fullNameESPN']
        playerDoB = tmpPlayer['DoBESPN']
        playerCitizenship = tmpPlayer['citizenshipESPN']
        bMatch = False
        for tmpPlayerTM in playerListTM:
            playerIdTM = tmpPlayerTM['playerIdTM']
            playerFullNameTM = tmpPlayerTM['fullNameTM']
            playerDoBTM = tmpPlayerTM['DoBTM']
            playerCitizenshipTM = tmpPlayerTM['citizenshipTM']
            if playerDoB != None and playerDoBTM != None:
                DoB_delta = (playerDoB - playerDoBTM).days
                # if DoB_delta >- 21 and DoB_delta < 21:
                # if DoB_delta >- 7 and DoB_delta < 7:
                if DoB_delta == 0:
                    DoBScore = 30
                elif DoB_delta > - 7 and DoB_delta < 7:
                    DoBScore = 10
                elif DoB_delta > - 14 and DoB_delta < 14:
                    DoBScore = 5
                # elif DoB_delta > - 21 and DoB_delta < 21:
                #    DoBScore = 5
                # elif DoB_delta > - 365 and DoB_delta < 365:
                #    DoBScore = 5
                else:
                    DoBScore = 0
                if DoBScore > 0:
                    # matchScore = matchScore_mc
                    # ratio = fuzz.ratio(playerFullName, playerFullNameTM)
                    # ratio = fuzz.token_set_ratio(playerFullName, playerFullNameTM)
                    if playerCitizenship != "" and playerCitizenshipTM != "":
                        ratio2 = fuzz.token_sort_ratio(playerCitizenship, playerCitizenshipTM) / 2
                    else:
                        ratio2 = 25
                    ratio1 = fuzz.token_sort_ratio(playerFullName, playerFullNameTM)
                    ratio = ratio1 + ratio2 + DoBScore
                    nCompare += 1
                    # if int(nCompare/nInterval)*nInterval == nCompare:
                    #    print('number of compares', nCompare, i, 'equal DoB')
                    if playerId in idDictESPN.keys():
                        old_ratio = idDictESPN[playerId]["combinedScore"]
                    else:
                        old_ratio = 0
                    if ratio > old_ratio:
                        matchedPlayerTM = tmpPlayerTM.copy()
                        idDictESPN[playerId] = {"playerIdTM": playerIdTM,
                                                "fullNameTM": playerFullNameTM,
                                                "citizenshipTM": playerCitizenshipTM,
                                                "DoBTM": playerDoBTM,
                                                "fuzzyRatio1": ratio1,
                                                "fuzzyRatio2": ratio2,
                                                "DoBScore": DoBScore,
                                                "combinedScore": ratio}
                    if ratio >= 180:
                        break
                    else:
                        m += 1
                        # bMatch = True
                        # print(i,n,'DoB match, name not match!',ratio, playerFullName, playerFullNameTM, playerDoB,playerDoBTM)
        if playerId in idDictESPN.keys():
            combinedScore = idDictESPN[playerId]["combinedScore"]
            playerIdTM = idDictESPN[playerId]["playerIdTM"]
            playerFullNameTM = idDictESPN[playerId]["fullNameTM"]
            playerCitizenshipTM = idDictESPN[playerId]["citizenshipTM"]
            playerDoBTM = idDictESPN[playerId]["DoBTM"]
            ratio1 = idDictESPN[playerId]["fuzzyRatio1"]
            ratio2 = idDictESPN[playerId]["fuzzyRatio2"]
            DoBScore = idDictESPN[playerId]["DoBScore"]
            if combinedScore >= matchScore_mc:
                n += 1
                bMatch = True
                playerListTM.remove(matchedPlayerTM)
                matchedId[playerId] = idDictESPN[playerId]
                if combinedScore <= 150:
                    print(i, 'of', ileft, n, 'marginal match.', ratio1, ratio2, DoBScore, combinedScore,
                          playerId,
                          playerIdTM, playerFullName, "-",
                          playerFullNameTM, playerDoB, "-", playerDoBTM,
                          playerCitizenship, "-", playerCitizenshipTM, nCompare)
                # else:
                #    print(i, 'of', ileft, n, '    good match.', ratio1, ratio2, DoBScore, combinedScore,
                #          playerId,
                #          playerIdTM, playerFullName,
                #          playerFullNameTM, playerDoB, playerDoBTM,
                #          playerCitizenship, playerCitizenshipTM, nCompare)
            else:
                k += 1
                # print(i, 'of', ileft, n, 'no match found.', ratio1, ratio2, DoBScore, combinedScore,
                # playerId,
                # playerIdTM, playerFullName,
                # playerFullNameTM, playerDoB, playerDoBTM,
                # playerCitizenship, playerCitizenshipTM, nCompare)
        else:
            # print(i, 'of', ileft, n, 'no match found.', playerId)
            k += 1
            # print(i, k, 'no match!', playerFullName, playerDoB)

    ileft = i - n
    print('number of compares', nCompare)
    print('matched players', n)
    print('not matched players', k)
    print('total players in ESPN', nTotalESPN)
    print('total players in TM', nTotalTM)
    print('not matched players in TM  ', len(playerListTM))
    print('not matched players in ESPN', ileft)
    print('number of matched id', len(matchedId.keys()))

    msg = saveMatchedList(matchedId, matchedPlayersFilename)
    print(matchedPlayersFilename, msg)
    m = 0
    for tmpPlayer in playerListESPN:
        playerId = tmpPlayer['playerIdESPN']
        if playerId not in matchedId.keys():
            m += 1
            playerFullName = tmpPlayer['fullNameESPN']
            playerDoB = tmpPlayer['DoBESPN']
            playerCitizenship = tmpPlayer['citizenshipESPN']
            bMatch = False
            for tmpPlayerTM in playerListTM:
                playerIdTM = tmpPlayerTM['playerIdTM']
                playerFullNameTM = tmpPlayerTM['fullNameTM']
                playerDoBTM = tmpPlayerTM['DoBTM']
                playerCitizenshipTM = tmpPlayerTM['citizenshipTM']
                if playerCitizenship != "" and playerCitizenshipTM != "":
                    #ratio2 = fuzz.token_sort_ratio(playerCitizenship, playerCitizenshipTM) / 2
                    ratio2 = fuzz.partial_ratio(playerCitizenship, playerCitizenshipTM) / 2
                else:
                    ratio2 = 25
                if ratio2 >=25:
                    ratio1 = fuzz.token_sort_ratio(playerFullName, playerFullNameTM)
                    ratio = ratio1 + ratio2 + 30  # assume DoB is always matches
                    nCompare += 1
                    # if int(nCompare/nInterval)*nInterval == nCompare:
                    #    print('number of compares',nCompare, i, 'last scan')
                    if ratio >= matchScore_hc:
                        bMatch = True
                        matchedPlayerTM = tmpPlayerTM.copy()
                        n += 1
                        k -= 1
                        matchedId[playerId] = {"playerIdTM": playerIdTM,
                                           "fullNameTM": playerFullNameTM,
                                           "citizenshipTM": playerCitizenshipTM,
                                           "DoBTM": playerDoBTM,
                                           "fuzzyRatio1": ratio1,
                                           "fuzzyRatio2": ratio2,
                                           "DoBScore": 30,
                                           "combinedScore": ratio}
                        print(m, 'of', ileft, len(playerListTM), n, 'matched!', ratio, playerFullName, "-",
                            playerFullNameTM,
                            playerDoB, "-", playerDoBTM,
                            playerCitizenship, "-", playerCitizenshipTM, nCompare)
                        break
            if bMatch:
                playerListTM.remove(matchedPlayerTM)

    print('number of compares', nCompare)
    print('matched players', n)
    print('not matched players', k)
    print('total players in ESPN', nTotalESPN)
    print('total players in TM', nTotalTM)
    print('not matched players in TM  ', len(playerListTM))
    print('not matched players in ESPN', i - n)
    print('number of matched id', len(idDictESPN.keys()))

    msg = saveMatchedList(matchedId, matchedPlayersFilename)
    print(matchedPlayersFilename, msg)
def importTMDataSet(dirTM):
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    # dirTM = 'Z:/Soccer/espn_DB/tables/transfer market'
    # Download all files of a dataset
    # https://technowhisp.com/kaggle-api-python-documentation/
    # Signature: dataset_download_files(dataset, path=None, force=False, quiet=True, unzip=False)
    api.dataset_download_files('davidcariboo/player-scores',path=dirTM,unzip=True)
    return("import TM dataset success")

with open('config_db_lx.json','r') as file:
    Response = json.load(file)
file.close()

print(Response)

rootDir=Response['rootDir']
rootDir2=Response['rootDir2']
importLeagueList=Response['leagues']
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

# dataSet = Response['dataSet']
# directory2 = rootDir2 + 'tables/transfer market/'
directory = rootDir2 + 'tables/'
filename = directory + 'dataSet.json'
with open(filename,'r') as file:
    Response= json.load(file)
file.close()

startDate=Response['startDate']
endDate=Response['endDate']
extractionDate=Response['extractionDate']
statusStr=Response['status']

dataSet = startDate + "-" + endDate + "-" + extractionDate

playersFilename = "players.csv"
directoryTM = rootDir2 + 'tables/transfer market'
matchedPlayersFilename = directoryTM + "/" + "matched_players.json"
playersFilename = directoryTM + "/" + playersFilename

bImportTMDataSet = True

if bImportTMDataSet:
    #import Transfer Market data set from Kaggle
    msg = importTMDataSet(directoryTM)
    print(msg)
    playerListTM = retrieveTMData(playersFilename)

    #import playerDB from excel4soccer DB
    tablename = "PlayerDB"
    playerListESPN = importPlayerDB(mysqlDict,tablename).copy()
    #matchPlayers
    msg=matchPlayers(playerListESPN,playerListTM,matchedPlayersFilename)
    print(msg)

#insert player TM data into excel4soccer DB
msg = sql_insert_all.Insert_PlayerDBTransferMarket(rootDir, rootDir2, dataSet, mysqlDict)
print(msg)

print("End")
