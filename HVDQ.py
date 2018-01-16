# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Created: 13 December 2017
# *   Modified: 15 January 2018
# * /

# Read the HV status (from the online DB) and put the status to a 
# DQ Table (e.g. 'HV status') in the production DB

# To run the script from (e.g.) g2be1:/home/daq/glukicov/TrackerDQ 
# 1) python HVDQ.py

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import sys # concatenating string 
import datetime

###========================OPEN CONNECTION===============================##
dbconf = None
with open('dbconnection.json', 'r') as f:
    dbconf = json.load(f)
# create a connection to the database using info from the json file
cnx = psycopg2.connect(user=dbconf['user'],
                                  host=dbconf['host'],
                                  database=dbconf['dbname'], port=dbconf['port'])
# query the database and obtain the result as python objects
cur=cnx.cursor() 

###========================READ HV STATUS: TRACKER SC===============================##

#Switching to tracker SC space to read the HV status: 
cur.execute("set schema 'gm2tracker_sc';")

#Defining Tracker Constants
moduleN = 8 # 8 trackers in 2 stations: 1) 8 in C7 2) 8 in C10
statationN = 2 

#Containers to store a unique relations between module name (e.g.T2_M0) 
# and its HV id (scid) in dictionary
nameID={} 
keys=[] 
values=[]
# psql to get ids and modules names from the DB that correspond to HV data 
cur.execute("select * from slow_control_items where name like '%HV%'")
rows = cur.fetchall()

#Store ids and names 
for item in rows:
	keys.append(int(item[0]))   #scid 
	values.append(str(item[1])) #name

#Add to un-sorted dictionary
for i_value in range(len(keys)):
        nameID[keys[i_value]] = values[i_value]

#Sort dictionary by string name [e.g. first=HVSTATUS_T1_M0, last=HVSTATUS_T2_M7]
nameID = OrderedDict(sorted(nameID.items(), key=lambda x: x[1]))

#Pull HV values for T1_M0 based on its scid,
# and covert ON (255) = 1 or else to 0 
scid=nameID.keys()[0]
timeStamp=[]
HV_statusModules=[]

## Get 10 latest ones XXX
cur.execute("select * from slow_control_data where scid=1151 ORDER BY time ASC limit 10;")
rows = cur.fetchall()
for item in rows:
	timeStamp.append(int(item[3])) #epoch timestamp
	if (int(item[2])==255):
		HV_statusModules.append(1)
	else:
		HV_statusModules.append(0)

print HV_statusModules
print timeStamp

timeStamp=[]
HV_statusModules=[]
HV_statusStation=[]

#For station 1 (T1):
# Check 10 latest entries for all modules
# get first scid from the sorted list

scidFirst=nameID.keys()[0]
scidLast=nameID.keys()[7]
limit=1
curCommand = "select * from slow_control_data where (scid>= " + str(scidFirst).strip() + " AND scid<= " + str(scidLast).strip() + " ) ORDER BY time ASC limit " + str(limit) + " ;"  
print curCommand

for i_lookup in range(0, limit):	
	cur.execute(curCommand)
	rows = cur.fetchall()
	for item in rows:
		if (int(item[2])==255):        
			HV_statusModules.append(1)
		else:
			HV_statusModules.append(0)

	#if all modules reported 1, write 1, if there is 0, write 0 
	if (0 in HV_statusModules):
		HV_statusStation.append(0)
	else:
		HV_statusStation.append(1)

	#print HV_statusModules
	#print timeStamp
	HV_statusModules=[]
	#Timestamps are the same for all modules - grab the last one
	timeStamp.append(int(item[3])) #epoch timestamp

print 'Tracker 1 HV status: ', HV_statusStation[0]
print 'Timestamp', datetime.datetime.fromtimestamp(int(timeStamp[0])).strftime('%Y-%m-%d %H:%M:%S')
   
###========================WRITE HV STATUS: DQC===============================##


#Switching to DQC space to write the HV status: 
cur.execute("set schema 'gm2dq';")

cur.execute("insert into tracker_hv values ('{1,1,1}', 8183, 11);")


###========================CLOSE CONNECTION===============================##

# close communication with the databaes
cur.close()
cnx.close()


# ##### Some psql shortcuts ####
# psql -U gm2_reader -h ifdbprod.fnal.gov -d gm2_online_prod -p 5452
# [connects to the Production DB, which is duplicated from the Online DB]

# \dt  - list all table
# \dn - list all schemas 
# \dt gm2tracker_sc.* - list all tables under the Tracker SC schema 
# set schema 'gm2tracker_sc';
# set schema 'gm2dq'; 

# select * from slow_control_items where name like '%HV%' limit 10;
# select * from slow_control_data where scid=946 limit 10; 

#cur.execute("select * from slow_control_data where scid=946 limit 10;")
# fetch all the rows
# rows = cur.fetchall()
# print rows


#Create a table in gm2dq schema:
# !! array size is not enforced in psql/ no unsigned ints in psql....

# CREATE TABLE tracker_hv (
#     hv_status  integer ARRAY[3],
#     run        integer,
#     subrun     integer
# );


