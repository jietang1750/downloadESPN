#!/bin/bash

logfilename="/home/jtang/work/tm.log"
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: update TM." > $logfilename
echo "[$currenttime]: update TM."
python3 /home/jtang/projects/downloadESPN/main.py > /home/jtang/work/updateTM.out
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: Backup database.." >> $logfilename
echo "[$currenttime]: Backup database.."
/home/jtang/backup/backup_excel4soccerDB.sh
currenttime=`date +%F-%I-%M-%S`
echo "[$currenttime]: ESPN Download Stops." >> $logfilename
echo "[$currenttime]: ESPN Download Stops."
