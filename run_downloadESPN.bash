#!/bin/bash

logfilename="/home/jtang/work/espn.log"
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: ESPN Download All." > $logfilename
echo "[$currenttime]: ESPN Download All."
python3 /home/jtang/python_scripts/soccer/espn/downloadESPN/main.py > /home/jtang/work/downloadESPN.out
#python3 /home/jtang/python_scripts/soccer/espn/downloadESPN/takeSnapshots.py > /home/jtang/work/snapshots.out
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: Backup database.." >> $logfilename
echo "[$currenttime]: Backup database.."
/home/jtang/backup/backup_excel4soccerDB.sh
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: ESPN Download Stops." >> $logfilename
echo "[$currenttime]: ESPN Download Stops."
