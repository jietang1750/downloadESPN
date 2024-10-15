import sqlConn
import pyodbc
import json
import sys
def createTableUpdateIdSQL(dbcur,task):
    tablename = "UpdateId"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table UpdateId already exists"
        print(msg)
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        if task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + tablename + "cannot be completed"
            else:
                dbcur.commit()
                msg = task + tablename + "is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
            except Exception as e:
                print(e)
                msg = task + tablename + "cannot be completed"
            else:
                dbcur.commit()
                msg = task + tablename + "is complete"
        else:
            msg = task + tablename + "cannot be completed"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            updateId int NOT NULL,
            updateContent VARCHAR(40),
            updateDateTime TIMESTAMP,
            dataSet VARCHAR(40),
            PRIMARY KEY (updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + tablename + "cannot be completed"
    return(msg)
def seasonTypeCreateTableSQL(dbcur,tablename,task):
    # tablename = "SeasonType"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_SeasonType_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Seasontype_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
                    try:
                        dbcur.execute(sql2)
                    except Exception as e:
                        print(e)
                        msg = task + " " + tablename + " cannot be completed"
                    else:
                        dbcur.commit()
                        msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " +  tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            year VARCHAR(20),
            typeId int NOT NULL,
            name VARCHAR(200),
            slug VARCHAR(100),
            updateId int NOT NULL,
            PRIMARY KEY(typeId),
            CONSTRAINT fk_SeasonType_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def statusTypeCreateTableSQL(dbcur,tablename,task):
    # tablename = "statusType"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_StatusType_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_StatusType_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " +  tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            name VARCHAR(50),
            state VARCHAR(20),
            completed BIT,
            description VARCHAR(100),
            detail VARCHAR(100),
            shortDetail VARCHAR(50),
            updateId int NOT NULL, 
            PRIMARY KEY(id),
            CONSTRAINT fk_StatusType_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return (bExist, msg)
def leaguesCreateTableSQL(dbcur,tablename,task):
    # tablename = "Leagues"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Leagues_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Leagues_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " +  tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL ,
            alternateId INT,
            name VARCHAR(200),
            abbreviation VARCHAR(100),
            shortName VARCHAR(100),
            midsizeName VARCHAR(40),
            slug VARCHAR(100),
            seasonTypeId INT,
            seasonHasStandings BIT ,
            logoUrl1 VARCHAR(255),
            logoUrl1LastUpdated DATETIME,
            logoUrl2 VARCHAR(255),
            logoUrl2LastUpdated DATETIME,
            hasStandings BIT,
            updateTime TIMESTAMP,
            isTournament BIT,
            updateId INT, 
            PRIMARY KEY(id),
            CONSTRAINT fk_Leagues_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def venuesCreateTableSQL(dbcur,tablename,task):
    # tablename = "Venues"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Venues_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Venues_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " +  tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            fullName VARCHAR(255),
            shortName VARCHAR(128),
            capacity INT,
            city VARCHAR(255),
            country VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            CONSTRAINT fk_Venues_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def positionTypeCreateTableSQL(dbcur,tablename,task):
    # tablename = "PositionType"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PositionType_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PositionType_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            name VARCHAR(128) NOT NULL,
            displayName VARCHAR(255),
            abbreviation VARCHAR(128),
            updateId INT,
            PRIMARY KEY(name),
            CONSTRAINT fk_PositionType_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def statTypeCreateTableSQL(dbcur,tablename,task):
    # tablename = "StatType"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_StatType_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_StatType_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            name VARCHAR(128) NOT NULL,
            displayName VARCHAR(255),
            shortDisplayName VARCHAR(255),
            description VARCHAR(255),
            abbreviation VARCHAR(128),
            updateId INT,
            PRIMARY KEY(name),
            CONSTRAINT fk_StatType_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamsCreateTableSQL(dbcur,tablename,task):
    # tablename = "Teams"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Teams_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Teams_venueId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Teams_ibfk_2;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Teams_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_StatType_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            teamId INT NOT NULL,
            uid VARCHAR(255),
            location VARCHAR(255),
            name VARCHAR(255),
            abbreviation VARCHAR(50),
            displayName VARCHAR(255),
            shortDisplayName VARCHAR(128),
            color VARCHAR(10),
            alternateColor VARCHAR(10),
            isActive BIT,
            logoURL VARCHAR(255),
            venueId INT,
            updateTime TIMESTAMP,
            slug VARCHAR(255),
            updateId INT,
            PRIMARY KEY(teamId),
            CONSTRAINT fk_Teams_venueId FOREIGN KEY(venueId) REFERENCES Venues(id),
            CONSTRAINT fk_Teams_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamStatNameCreateTableSQL(dbcur,tablename,task):
    # tablename = "TeamStatName"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY TeamStatName_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamStatName_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamStatName_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            stat VARCHAR(255),
            name VARCHAR(255),
            updateId INT,
            PRIMARY KEY(id),
            CONSTRAINT fk_TeamStatName_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamStatName2CreateTableSQL(dbcur,tablename,task):
    # tablename = "TeamStatName2"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY TeamStatName2_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamStatName2_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (updateId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamStatName2_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            stat VARCHAR(255),
            statAbbreviation VARCHAR(255),
            updateId INT,
            PRIMARY KEY(id),
            CONSTRAINT fk_TeamStatName2_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def fixturesCreateTableSQL(dbcur,tablename,task):
    # tablename = "Fixtures"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Fixtures_homeTeamId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Fixtures_awayTeamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Fixtures_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Fixtures_homeTeamId FOREIGN KEY (homeTeamId) REFERENCES Teams(teamId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Fixtures_awayTeamId FOREIGN KEY (awayTeamId) REFERENCES Teams(teamId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Fixtures_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                # dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " +  tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            leagueId INT,
            uid VARCHAR(100),
            attendance INT,
            date DATETIME,
            startDate DATETIME,
            neutralSite BIT,
            conferenceCompetition BIT,
            boxscoreAvailable BIT,
            commentaryAvailable BIT,
            recent BIT,
            boxscoreSource VARCHAR(10),
            playByPlaySource VARCHAR(10),
            seasonType INT,
            statusId INT,
            clock INT,
            displayClock VARCHAR(20),
            period INT,
            venueId INT,
            homeTeamId INT,
            homeTeamUid VARCHAR(100),
            homeTeamOrder INT,
            homeTeamWinner BIT,
            homeTeamScore INT,
            homeTeamShootoutScore INT,
            homeTeamForm VARCHAR(10),
            awayTeamId INT,
            awayTeamUid VARCHAR(100),
            awayTeamOrder INT,
            awayTeamWinner BIT,
            awayTeamScore INT,
            awayTeamShootoutScore INT,
            awayTeamForm VARCHAR(10),
            hasStats BIT,
            homeYellowCard INT,
            homeRedCard INT,
            awayYellowCard INT,
            awayRedCard INT,
            updateTime TIMESTAMP,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId),
            CONSTRAINT fk_Fixtures_homeTeamId FOREIGN KEY(homeTeamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_Fixtures_awayTeamId FOREIGN KEY(awayTeamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_Fixtures_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamStatsCreateTableSQL(dbcur,tablename,task):
    # tablename = "TeamStats"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY TeamStats_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamStats_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamStats_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamStats_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId,teamId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamStats_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamStats_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamStats_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            teamOrder TINYINT,
            updateTime TIMESTAMP,
            hasStats BIT,
            foulsCommitted INT,
            yellowCards INT,
            redCards INT,
            offsides INT,
            wonCorners INT,
            saves INT,
            possessionPct FLOAT,
            totalShots INT,
            shotsOnTarget INT,
            shotPct FLOAT,
            penaltyKickGoals INT,
            penaltyKickShots INT,
            accuratePasses INT,
            totalPasses INT,
            passPct FLOAT,
            accurateCrosses INT,
            totalCrosses INT,
            crossPct FLOAT,
            totalLongBalls INT,
            accurateLongBalls INT,
            longballPct FLOAT,
            blockedShots INT,
            effectiveTackles INT,
            totalTackles INT,
            tacklePct FLOAT,
            interceptions INT,
            effectiveClearance INT,
            totalClearance INT,
            goalDifference INT,
            totalGoals INT,
            goalAssists INT,
            goalsConceded INT,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,teamId),
            CONSTRAINT fk_TeamStats_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_TeamStats_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_TeamStats_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamUniformCreateTableSQL(dbcur,tablename,task):
    # tablename = "TeamUniform"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY TeamStats_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamUniform_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamUniform_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamUniform_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId,teamId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamUniform_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamUniform_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamUniform_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            teamOrder TINYINT,
            teamColor VARCHAR(10),
            teamAlternateColor VARCHAR(10),
            uniformType VARCHAR(10),
            uniformColor VARCHAR(10),
            uniformAlternateColor VARCHAR(10),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId),
            CONSTRAINT fk_TeamUniform_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_TeamUniform_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_TeamUniform_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def attendanceCreateTableSQL(dbcur,tablename,task):
    # tablename = "Attendance"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Attendance_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Attendance_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Attendance_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Attendance_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Attendance_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            attendance INT,
            updateId INT,
            PRIMARY KEY(eventId),
            CONSTRAINT fk_Attendance_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Attendance_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def officialsCreateTableSQL(dbcur,tablename,task):
    # tablename = "Officials"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Officials_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Officials_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Officials_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Officials_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Officials_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            fullName VARCHAR(128),
            displayName VARCHAR(128),
            refOrder INT NOT NULL,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,refOrder),
            CONSTRAINT fk_Officials_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Officials_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def venueDBCreateTableSQL(dbcur,tablename,taks):
    # tablename = "VenueDB"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY VenueDb_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_VenueDB_venueId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_VenueDB_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_VenueDB_venueId FOREIGN KEY (eventId) REFERENCES Venues(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_VenueDB_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            venueId INT NOT NULL,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,venueId),
            FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_VenueDB_venueId FOREIGN KEY(venueId) REFERENCES Venues(id),
            CONSTRAINT fk_VenueDB_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def keyEventTypeCreateTableSQL(dbcur,tablename,task):
    # tablename = "KeyEventType"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY KeyEventType_ibfk_1;
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEventType_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (keyEventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEventType_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            keyEventId INT NOT NULL,
            keyEventName VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventId),
            CONSTRAINT fk_KeyEventType_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def keyEventSourceCreateTableSQL(dbcur,tablename,task):
    # tablename = "KeyEventSource"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY KeyEventSource_ibfk_1;
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEventSource_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (keyEventSourceId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEventSource_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            keyEventSourceIndex INT NOT NULL,
            keyEventSourceId INT NOT NULL,
            keyEventSourceName VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventSourceId),
            CONSTRAINT fk_KeyEventSource_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playsCreateTableSQL(dbcur,tablename,task):
    # tablename = "Plays"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Plays_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Plays_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Plays_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (playId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Plays_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Plays_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            playOrder INT NOT NULL,
            playId INT,
            typeId INT,
            text VARCHAR(255),
            shortText VARCHAR(255),
            period INT,
            clockValue INT,
            clockDisplayValue VARCHAR(128),
            teamDisplayName VARCHAR(128),
            teamId INT,
            sourceId INT,
            scoringPlay BIT,
            shootout BIT,
            wallclock DATETIME,
            goalPositionX FLOAT,
            goalPositionY FLOAT,
            fieldPositionX FLOAT,
            fieldPositionY FLOAT,
            fieldPosition2X FLOAT,
            fieldPosition2Y FLOAT,
            updateId INT NOT NULL,
            PRIMARY KEY(playId),
            CONSTRAINT fk_Plays_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Plays_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def keyEventsCreateTableSQL(dbcur,tablename,task):
    # tablename = "KeyEvents"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY KeyEvents_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEvents_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEvents_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (keyEventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEvents_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEvents_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            keyEventOrder INT NOT NULL,
            keyEventId INT,
            typeId INT,
            period INT,
            clockValue INT,
            clockDisplayValue VARCHAR(128),
            scoringPlay BIT,
            sourceId INT,
            shootout BIT,
            keyEventText VARCHAR(255),
            keyEventShortText VARCHAR(255),
            teamId INT,
            teamDisplayName VARCHAR(128),
            goalPositionX FLOAT,
            goalPositionY FLOAT,
            fieldPositionX FLOAT,
            fieldPositionY FLOAT,
            fieldPosition2X FLOAT,
            fieldPosition2Y FLOAT,
            updateId INT NOT NULL,
            PRIMARY KEY(keyEventId),
            CONSTRAINT fk_KeyEvents_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_KeyEvents_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def commentaryCreateTableSQL(dbcur,tablename,task):
    # tablename = "Commentary"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Commentary_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Commentary_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Commentary_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId,commentaryOrder);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Commentary_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Commentary_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            commentaryOrder INT NOT NULL,
            clockValue INT,
            clockDisplayValue VARCHAR(10),
            commentaryText VARCHAR(4096),
            id INT,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,commentaryOrder),
            CONSTRAINT fk_Commentary_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Commentary_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def athletesCreateTableSQL(dbcur,tablename,task):
    # tablename = "Atheletes"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Athletes_ibfk_1;
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Athletes_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (id);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Athletes_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            uid VARCHAR(128),
            guid VARCHAR(128),
            lastName VARCHAR(256),
            fullName VARCHAR(256),
            displayName VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            CONSTRAINT fk_Athletes_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerDBCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayerDB"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY PlayerDB_ibfk_1;
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerDB_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (id);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerDB_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id INT NOT NULL,
            uid VARCHAR(128),
            guid VARCHAR(128),
            firstName VARCHAR(128),
            lastName VARCHAR(128),
            fullName VARCHAR(128),
            displayName VARCHAR(128),
            shortName VARCHAR(128),
            weight DOUBLE(10,2),
            displayWeight VARCHAR(128),
            height DOUBLE(10,2),
            displayHeight VARCHAR(128),
            age INT,
            dateOfBirth VARCHAR(100),
            gender  VARCHAR(10),
            citizenship VARCHAR(128),
            slug VARCHAR(128),
            jersey INT,
            status VARCHAR(128),
            profiled BIT,
            timestamp TIMESTAMP,
            birthPlaceCountry VARCHAR(128),
            citizenshipCountryAlternateId INT,
            citizenshipCountryAbbreviation  VARCHAR(128),
            birthCountryAlternateId INT,
            birthCountryAbbreviation VARCHAR(128),
            flag_href VARCHAR(256),
            flag_alt VARCHAR(128),
            positionId INT,
            positionName  VARCHAR(128),
            positionDisplayName VARCHAR(128),
            positionAbbreviation VARCHAR(128),
            middleName VARCHAR(128),
            headshot_href VARCHAR(128),
            headshot_alt  VARCHAR(128),
            birthPlaceCity VARCHAR(128),
            nickname  VARCHAR(128),
            updateId INT NOT NULL,
            PRIMARY KEY(id),
            CONSTRAINT fk_PlayerDB_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playParticipantsCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayParticipants"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY PlayParticipants_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayParticipants_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayParticipants_playId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayParticipants_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId, playId, playOrder);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayParticipants_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayParticipants_playId FOREIGN KEY (playId) REFERENCES Plays(playId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayParticipants_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            playId INT NOT NULL,
            playOrder INT,
            participant VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,playId,playOrder),
            CONSTRAINT fk_PlayParticipants_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_PlayParticipants_playId FOREIGN KEY(playId) REFERENCES Plays(playId),
            CONSTRAINT fk_PlayParticipants_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def keyEventParticipantsCreateTableSQL(dbcur,tablename,task):
    # tablename = "KeyEventParticipants"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY KeyEventParticipants_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEventParticipants_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEventParticipants_keyEventId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_KeyEventParticipants_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId,keyEventId, keyEventOrder);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEventParticipants_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEventParticipants_keyEventId FOREIGN KEY (playId) REFERENCES KeyEvents(keyEventId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_KeyEventParticipants_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            keyEventId INT NOT NULL,
            keyEventOrder INT,
            participant VARCHAR(256),
            updateId INT NOT NULL,
            PRIMARY KEY(eventId,keyEventId,keyEventOrder),
            CONSTRAINT fk_KeyEventParticipants_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_KeyEventParticipants_keyEventId FOREIGN KEY(keyEventId) REFERENCES KeyEvents(keyEventId),
            CONSTRAINT fk_KeyEventParticipants_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerPlaysCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayerPlays"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY PlayerPlays_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerPlays_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerPlays_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerPlays_athleteId;
                """
        sql13 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerPlays_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerPlays_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayPlays_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayPlays_athleteId FOREIGN KEY (athleteId) REFERENCES PlayerDB(id);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerPlays_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
                dbcur.execute(sql13)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            homeAway VARCHAR(10),
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            hasPlays BIT,
            clockDisplayValue VARCHAR(128),
            clockValue INT,
            playOrder INT,
            scoringPlay BIT,
            substitution BIT,
            redCard BIT,
            yellowCard BIT,
            penaltyKick BIT,
            ownGoal BIT,
            didScore BIT,
            updateTime TIMESTAMP,
            didAssist BIT,
            updateId INT NOT NULL,
            PRIMARY KEY (eventID, teamID, playOrder),
            CONSTRAINT fk_PlayerPlays_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_PlayerPlays_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_PlayerPlays_id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_PlayerPlays_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerStatsCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayStats"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY PlayStats_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayStats_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayStats_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayStats_athleteId;
                """
        sql13 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayStats_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayStats_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayStats_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayStats_athleteId FOREIGN KEY (athleteId) REFERENCES PlayerDB(id);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayStats_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
                dbcur.execute(sql13)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            appearances INT,
            foulsCommitted INT,
            foulsSuffered  INT,
            ownGoals INT,
            redCards INT,
            subIns INT,
            yellowCards INT,
            goalAssists INT,
            shotsOnTarget INT,
            totalGoals INT,
            totalShots INT,
            goalsConceded INT,
            saves INT,
            shotsFaced INT,
            hasStats BIT,
            updateTime TIMESTAMP,
            offsides INT, 
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId, athleteId),
            CONSTRAINT fk_PlayStats_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_PlayStats_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_PlayStats_Id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_PlayStats_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def teamRosterCreateTableSQL(dbcur,tablename,task):
    # tablename = "TeamRoster"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY TeamRoster_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamRoster_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamRoster_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamRoster_athleteId;
                """
        sql13 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamRoster_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (eventId,teamId,athleteId);
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamRoster_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamRoster_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamRoster_athleteId FOREIGN KEY (athleteId) REFERENCES PlayerDB(id);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamRoster_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        sql7 = f"""
                ALTER TABLE {tablename}
                MODIFY formationPlace INT AFTER position;
                ADD uniformType VARCHAR(10) AFTER teamId,
                ADD uniformColor VARCHAR(10) AFTER uniformType,
                ADD homeAway VARCHAR(10) AFTER uniformColor,
                ADD winner BIT DEFAULT NULL AFTER homeAway, 
                ADD formation VARCHAR(10) AFTER winner, 
                ADD subbedInForAthleteId INT AFTER subbedIn,
                ADD subbedInForAthleteJersey INT AFTER subbedInForAthleteId,
                ADD subbedInClockValue INT AFTER subbedInForAthleteJersey,
                ADD subbedInDisplayClock VARCHAR(10) AFTER subbedInClockValue,
                ADD subbedOutForAthleteId int AFTER subbedOut, 
                ADD subbedOutForAthleteJersey int AFTER subbedOutForAthleteId, 
                ADD subbedOutClockValue INT AFTER subbedOutForAthleteJersey, 
                ADD subbedOutDisplayClock VARCHAR(10) AFTER subbedOutClockValue;
                """
        sql8 = f"""
                ALTER TABLE {tablename}
                MODIFY formationPlace INT AFTER position;
                """
        sql9 = f"""
                ALTER TABLE {tablename}
                MODIFY winner BIT;
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
                dbcur.execute(sql13)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                #dbcur.execute(sql3)
                #dbcur.execute(sql4)
                #dbcur.execute(sql5)
                #dbcur.execute(sql6)
                #dbcur.execute(sql7)
                #dbcur.execute(sql8)
                dbcur.execute(sql9)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                conn.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            teamId INT NOT NULL,
            active BIT DEFAULT NULL,
            starter BIT DEFAULT NULL,
            jersey INT DEFAULT NULL,
            athleteId INT NOT NULL,
            athleteDisplayName VARCHAR(256),
            position VARCHAR(128),
            subbedIn BIT DEFAULT NULL,
            subbedOut BIT DEFAULT NULL,
            formationPlace INT,
            hasStats BIT DEFAULT NULL,
            hasPlays BIT DEFAULT NULL,
            updateTime TIMESTAMP,
            updateId INT NOT NULL,
            PRIMARY KEY(eventId, teamId, athleteId),
            CONSTRAINT fk_TeamRoster_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_TeamRoster_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
            CONSTRAINT fk_TeamRoster_Id FOREIGN KEY(athleteId) REFERENCES Athletes(id),
            CONSTRAINT fk_TeamRoster_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            conn.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def detailTypesCreateTableSQL(dbcur,tablename,task):
    # tablename = "DetailTypes"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY DetailTypes_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_DetailTypes_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_DetailTypes_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            id bigint DEFAULT NULL,
            typeId int NOT NULL,
            typeText text,
            updateId INT DEFAULT NULL,            
            CONSTRAINT fk_DetailTypes_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def detailsCreateTableSQL(dbcur,tablename,task):
    # tablename = "Details"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY Details_ibfk_1;
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Details_eventId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Details_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Details_eventId FOREIGN KEY (eventId) REFERENCES Fixtures(eventId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Details_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            eventId INT NOT NULL,
            detailOrder INT NOT NULL,
            typeId INT DEFAULT NULL,
            typeText VARCHAR(100),
            clockValue int DEFAULT NULL,
            clockDisplayValue VARCHAR(100),
            scoringPlay BIT DEFAULT NULL,
            scoreValue int DEFAULT NULL,
            teamId int DEFAULT NULL,
            redCard tinyint(1) DEFAULT NULL,
            yellowCard tinyint(1) DEFAULT NULL,
            penaltyKick tinyint(1) DEFAULT NULL,
            ownGoal tinyint(1) DEFAULT NULL,
            shootout tinyint(1) DEFAULT NULL,
            athletesInvolved int DEFAULT NULL,
            updateId INT DEFAULT NULL,
            PRIMARY KEY(eventId,detailOrder),
            CONSTRAINT fk_Details_eventId FOREIGN KEY(eventId) REFERENCES Fixtures(eventId),
            CONSTRAINT fk_Details_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerStatsCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayerStats"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerStats_id;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerStats_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerStats_seasonType;
                """
        sql13 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerStats_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerStats_id FOREIGN KEY (id) REFERENCES PlayerDB(id);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerStats_teamId FOREIGN KEY (teamId) REFERENCES Teams(teamId);
                """
        sql5 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerStats_seasonType FOREIGN KEY (seasonType) REFERENCES SeasonType(typeId);
                """
        sql6 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerStats_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
                dbcur.execute(sql13)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
                dbcur.execute(sql5)
                dbcur.execute(sql6)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
              id int NOT NULL,
              uid text,
              guid text,
              league text,
              teamId int NOT NULL,
              playerIndex int DEFAULT NULL,
              seasonYear VARCHAR(20) DEFAULT NULL,
              seasonType int NOT NULL,
              seasonName text,
              timestamp timestamp NULL DEFAULT NULL,
              foulsCommitted_category text,
              foulsCommitted_value int DEFAULT NULL,
              foulsCommitted_displayValue text,
              foulsSuffered_category text,
              foulsSuffered_value int DEFAULT NULL,
              foulsSuffered_displayValue text,
              redCards_category text,
              redCards_value int DEFAULT NULL,
              redCards_displayValue text,
              yellowCards_category text,
              yellowCards_value int DEFAULT NULL,
              yellowCards_displayValue text,
              ownGoals_category text,
              ownGoals_value int DEFAULT NULL,
              ownGoals_displayValue text,
              appearances_category text,
              appearances_value int DEFAULT NULL,
              appearances_displayValue text,
              subIns_category text,
              subIns_value int DEFAULT NULL,
              subIns_displayValue text,
              goalAssists_category text,
              goalAssists_value int DEFAULT NULL,
              goalAssists_displayValue text,
              offsides_category text,
              offsides_value int DEFAULT NULL,
              offsides_displayValue text,
              shotsOnTarget_category text,
              shotsOnTarget_value int DEFAULT NULL,
              shotsOnTarget_displayValue text,
              totalShots_category text,
              totalShots_value int DEFAULT NULL,
              totalShots_displayValue text,
              totalGoals_category text,
              totalGoals_value int DEFAULT NULL,
              totalGoals_displayValue text,
              saves_category text,
              saves_value int DEFAULT NULL,
              saves_displayValue text,
              shotsFaced_category text,
              shotsFaced_value int DEFAULT NULL,
              shotsFaced_displayValue text,
              goalsConceded_category text,
              goalsConceded_value int DEFAULT NULL,
              goalsConceded_displayValue text,
              updateId int NOT NULL,
              PRIMARY KEY (id,teamId,seasonType),
              CONSTRAINT fk_PlayerStats_id FOREIGN KEY(id) REFERENCES PlayerDB(id),
              CONSTRAINT fk_PlayerStats_teamId FOREIGN KEY(teamId) REFERENCES Teams(teamId),
              CONSTRAINT fk_PlayerStats_seasonType FOREIGN KEY(seasonType) REFERENCES SeasonType(typeId),
              CONSTRAINT fk_PlayerStats_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId))
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)

try:
    conn = pyodbc.connect(driver='{MySQL ODBC 8.0 Unicode Driver}',
                          Server='tang-svr',
                          database='excel4soccer',
                          uid='jtang',
                          pwd='cstagt9903',
                          trusted_connection='yes'
                          )
except Exception as e:
    print(e)
    print('task is terminated')
else:
    cursor=conn.cursor()
def teamsInLeagueCreateTableSQL(dbcur,tablename,task):
    # tablename = "Teams"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamsInLeague_leagueId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamsInLeague_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_TeamsInLeague_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (leagueId, year, teamId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_TeamsInLeague_leagueId FOREIGN KEY(leagueId) REFERENCES Leagues(id),
                ADD CONSTRAINT fk_TeamsInLeague_teamId FOREIGN KEY(teamId) REFERENCES Teams(TeamId),
                ADD CONSTRAINT fk_TeamsInLeague_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                # dbcur.execute(sql3)
                print(sql4)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            year INT,
            leagueId INT,
            midsizeLeagueName VARCHAR(128),
            teamId INT NOT NULL,
            uid VARCHAR(255),
            location VARCHAR(255),
            name VARCHAR(255),
            abbreviation VARCHAR(50),
            displayName VARCHAR(255),
            shortDisplayName VARCHAR(128),
            slug VARCHAR(255),
            color VARCHAR(10),
            alternateColor VARCHAR(10),
            isActive BIT,
            isAllStar BIT,
            hasRecord BIT,
            timeStamp DATETIME,
            updateId INT,
            PRIMARY KEY(leagueId, year, teamId),
            CONSTRAINT fk_TeamsInLeague_leagueId FOREIGN KEY(leagueId) REFERENCES Leagues(id),
            CONSTRAINT fk_TeamsInLeague_teamId FOREIGN KEY(teamId) REFERENCES Teams(TeamId),
            CONSTRAINT fk_TeamsInLeague_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId));
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def standingsCreateTableSQL(dbcur,tablename,task):
    # tablename = "Teams"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql10 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Standings_leagueId;
                """
        sql11 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Standings_teamId;
                """
        sql12 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_Standings_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (leagueId, year, teamId);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_Standings_leagueId FOREIGN KEY(leagueId) REFERENCES Leagues(id),
                ADD CONSTRAINT fk_Standings_teamId FOREIGN KEY(teamId) REFERENCES Teams(TeamId),
                ADD CONSTRAINT fk_Standings_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql10)
                dbcur.execute(sql11)
                dbcur.execute(sql12)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                # dbcur.execute(sql3)
                print(sql4)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
            year INT,
            leagueId INT,
            midsizeLeagueName VARCHAR(128),
            teamId INT NOT NULL,
            gamesPlayed INT,
            losses INT,
            pointDifferential INT,
            points INT,
            pointsAgainst INT,
            pointsFor INT,
            streak INT,
            ties INT,
            wins INT,
            awayGamesPlayed INT,
            awayLosses INT,
            awayPointsAgainst INT,
            awayPointsFor INT,
            awayTies INT,
            awayWins INT,
            deductions INT,
            homeGamesPlayed INT,
            homeLosses INT,
            homePointsAgainst INT,
            homePointsFor INT,
            homeTies INT,
            homeWins INT,
            ppg INT,
            teamRank INT,
            rankChange INT,
            timeStamp DATETIME,
            updateId INT,
            PRIMARY KEY(leagueId, year, teamId),
            CONSTRAINT fk_Standings_leagueId FOREIGN KEY(leagueId) REFERENCES Leagues(id),
            CONSTRAINT fk_Standings_teamId FOREIGN KEY(teamId) REFERENCES Teams(TeamId),
            CONSTRAINT fk_Standings_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId));
        """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerDBTMCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayerDB"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerDBTM_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (id);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerDBTM_updateId FOREIGN KEY (updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
                player_id INT NOT NULL,
                first_name VARCHAR(200),
                last_name VARCHAR(200),
                name VARCHAR(200),
                last_season INT,
                current_club_id INT,
                player_code VARCHAR(200),
                country_of_birth VARCHAR(200),
                city_of_birth VARCHAR(200),
                country_of_citizenship VARCHAR(200),
                date_of_birth VARCHAR(200),
                sub_position VARCHAR(200),
                position VARCHAR(200),
                foot VARCHAR(200),
                height_in_cm INT,
                contract_expiration_date DATETIME,
                agent_name VARCHAR(200),
                image_url VARCHAR(1024),
                url VARCHAR(1024),
                current_club_domestic_competition_id VARCHAR(200),
                current_club_name VARCHAR(1024),
                market_value_in_eur INT,
                highest_market_value_in_eur INT,
                updateId INT,
                PRIMARY KEY(player_id),
                CONSTRAINT fk_PlayerDBTM_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId)); 
                """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)
def playerIdTMCreateTableSQL(dbcur,tablename,task):
    # tablename = "PlayerIdTM"
    bExist = sqlConn.checkTableExists(dbcur, tablename)
    if bExist:
        msg = "table " + tablename +  " already exists"
        print(msg)
        sql0 = f"""SHOW CREATE TABLE {tablename};
                """
        sql1 = f"""ALTER TABLE {tablename}
                DROP FOREIGN KEY fk_PlayerIdTM_player_id_TM 
                DROP FOREIGN fk_PlayerIdTM_updateId;
                """
        sql2 = f"""DROP TABLE {tablename};
                """
        sql3 = f"""ALTER TABLE {tablename}
                ADD PRIMARY KEY (id);
                """
        sql4 = f"""
                ALTER TABLE {tablename}
                ADD CONSTRAINT fk_PlayerIdTM_player_id_TM FOREIGN KEY(player_id_TM) REFERENCES PlayerDBTM(player_id) 
                ADD CONSTRAINT fk_PlayerIdTM_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId);
                """
        # rs = dbcur.execute(sql0).fetchall()
        # dbcur.commit()
        # print(rs)
        if task == "DropForeignKey":
            try:
                dbcur.execute(sql1)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "DropTable":
            try:
                dbcur.execute(sql2)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
                msg = task + " " + tablename + " is complete"
        elif task == "AlterTable":
            try:
                dbcur.execute(sql3)
                dbcur.execute(sql4)
            except Exception as e:
                print(e)
                msg = task + " " + tablename + " cannot be completed"
            else:
                dbcur.commit()
    elif task == "CreateTable":
        sql1 = f"""CREATE TABLE {tablename} (
                playerId INT NOT NULL,
                player_id_TM INT,
                matchedScore INT,
                updateId INT,
                PRIMARY KEY(playerId),
                CONSTRAINT fk_PlayerIdTM_player_id_TM FOREIGN KEY(player_id_TM) REFERENCES PlayerDBTM(player_id),
                CONSTRAINT fk_PlayerIdTM_updateId FOREIGN KEY(updateId) REFERENCES UpdateId(updateId)); 
                """
        try:
            dbcur.execute(sql1)
        except Exception as e:
            print(e)
            print(sql1)
            msg = 'cannot create table '+ tablename
        else:
            dbcur.commit()
            msg = tablename + ' is created!'
    else:
        msg = task + " " + tablename + " cannot be completed"
    return(bExist,msg)

with open('config_db.json','r') as file:
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
osStr = "Windows"

hostName = mysqlDict['hostName']
userId = mysqlDict['userId']
pwd = mysqlDict['pwd']
dbName = mysqlDict['dbName']
odbcDriver = mysqlDict['odbcDriver']

if osStr == "Windows":
    (conn, cursor) = sqlConn.connectDB_ODBC(hostName, userId, pwd, dbName, odbcDriver)
elif osStr == "Linux":
    (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
else:
    (conn, cursor) = sqlConn.connectDB(hostName, userId, pwd, dbName)
#print(conn)

# task = "CreateTable"
# task = "DropForeignKey"
# task = "DropTable"
task = "AlterTable"

tableName27 = 'TeamRoster'
msg = teamRosterCreateTableSQL(cursor,tableName27,task)
print(msg)

"""
tableName1 = 'SeasonType'
msg = seasonTypeCreateTableSQL(cursor,tableName1,task)
print(msg)

tableName2 = 'StatusType'
msg = statusTypeCreateTableSQL(cursor,tableName2,task)
print(msg)


tableName3 = 'Leagues'
msg = leaguesCreateTableSQL(cursor,tableName3,task)
print(msg)


tableName4 = 'Venues'
msg = venuesCreateTableSQL(cursor,tableName4,task)
print(msg)

tableName5 = 'PositionType'
msg = positionTypeCreateTableSQL(cursor,tableName5,task)
print(msg)

tableName6 = 'StatType'
msg = statTypeCreateTableSQL(cursor,tableName6,task)
print(msg)

tableName7 = 'Teams'
msg = teamsCreateTableSQL(cursor,tableName7,task)
print(msg)

tableName8 = 'TeamStatName'
msg = teamStatNameCreateTableSQL(cursor,tableName8,task)
print(msg)

tableName9 = 'TeamStatName2'
msg = teamStatName2CreateTableSQL(cursor,tableName9,task)
print(msg)

tableName10 = 'Fixtures'
msg = fixturesCreateTableSQL(cursor,tableName10,task)
print(msg)

tableName11 = 'TeamStats'
msg = teamStatsCreateTableSQL(cursor,tableName11,task)
print(msg)

tableName12 = 'TeamUniform'
msg = teamUniformCreateTableSQL(cursor,tableName12,task)
print(msg)

tableName13 = 'Attendance'
msg = attendanceCreateTableSQL(cursor,tableName13,task)
print(msg)
tableName14 = 'Officials'
msg = officialsCreateTableSQL(cursor,tableName14,task)
print(msg)
tableName15 = 'VenueDB'
msg = venueDBCreateTableSQL(cursor,tableName15,task)
print(msg)

tableName16 = 'KeyEventType'
msg = keyEventTypeCreateTableSQL(cursor,tableName16,task)
print(msg)

tableName17 = 'KeyEventSource'
msg = keyEventSourceCreateTableSQL(cursor,tableName17,task)
print(msg)

tableName18 = 'Plays'
msg = playsCreateTableSQL(cursor,tableName18,task)
print(msg)

tableName19 = 'KeyEvents'
msg = keyEventsCreateTableSQL(cursor,tableName19,task)
print(msg)

tableName20 = 'Commentary'
msg = commentaryCreateTableSQL(cursor,tableName20,task)
print(msg)

tableName21 = 'PlayerDB'
msg = playerDBCreateTableSQL(cursor,tableName21,task)
print(msg)

tableName22 = 'Athletes'
msg = athletesCreateTableSQL(cursor,tableName22,task)
print(msg)

tableName23 = 'PlayParticipants'
msg = playParticipantsCreateTableSQL(cursor,tableName23,task)
print(msg)

tableName24 = 'KeyEventParticipants'
msg = keyEventParticipantsCreateTableSQL(cursor,tableName24,task)
print(msg)

tableName25 = 'PlayerPlays'
msg = playerPlaysCreateTableSQL(cursor,tableName25,task)
print(msg)

tableName26 = 'PlayerStats'
msg = playerStatsCreateTableSQL(cursor,tableName26,task)
print(msg)

tableName28 = 'DetailTypes'
msg = detailTypesCreateTableSQL(cursor, tableName28, task)
print(msg)

tableName29 = 'Details'
msg = detailsCreateTableSQL(cursor, tableName29, task)
print(msg)

tableName30 = 'PlayerStats'
msg = playerStatsCreateTableSQL(cursor, tableName30, task)
print(msg)

tableName31 = 'TeamsInLeague'
msg = teamsInLeagueCreateTableSQL(cursor, tableName31, task)
print(msg)

tableName32 = 'Standings'
msg = standingsCreateTableSQL(cursor, tableName32, task)
print(msg)

tableName33 = 'PlayerDBTM'
msg = playerDBTMCreateTableSQL(cursor, tableName33, task)
print(msg)
tableName34 = 'PlayerIdTM'
msg = playerIdTMCreateTableSQL(cursor, tableName34, task)
print(msg)
"""

conn.close()
