# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Modified: 26 February 2018
# * /

# Read the tracker HV status from the online DB in gm2tracker_sc schema and put the
# collated status to a DQ Table 'HV_status' in the gm2dq schema, with the 
# associated run and subrun number from the 'subrun_time' table

# To run the script from (e.g.) g2be1:/home/daq/glukicov/TrackerDQ 
# 1) python HVDQ.py

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import datetime  #epoch time -> UTC
import argparse, sys
 
######## HELPER FUNCTIONS AND ARGUMENTS ########

parser = argparse.ArgumentParser(description='psql control')
parser.add_argument('-l', '--limit', help='limit for the returned query')
args = parser.parse_args()
limit = args.limit


###========================OPEN CONNECTION ======================================##

dbconf = None
with open('dbconnection.json', 'r') as f:
	dbconf = json.load(f)
# create a connection to the database using info from the json file
cnx = psycopg2.connect(user=dbconf['user'],
								  host=dbconf['host'],
								  database=dbconf['dbname'], port=dbconf['port'])
# query the database and obtain the result as python objects
cur=cnx.cursor() 	

###========================GET TRACKER MODULE LABELS: ONCE======================================##

#Defining Tracker Constants
moduleN = 8 # 8 trackers in 2 stations: 1) 8 in C7 (Station ID=#1) 2) 8 in C10 (Station ID=#2) 3) 0 in C1 (Station ID=#0)
stationN = 2 

#Containers to store a unique relations between module name (e.g.T2_M0) 
# and its HV id (scid) in a dictionary
nameID={} 
keys=[] 
values=[]

# get ids and modules names from the DB that correspond to HV data 
cur.execute("SELECT scid, name FROM gm2tracker_sc.slow_control_items WHERE name LIKE '%HV%'")
rows = cur.fetchall()
for item in rows:
	keys.append(int(item[0]))   #scid (e.g. 1151)
	values.append(str(item[1])) #name (e.g. HVSTATUS_T1_M0)

#Add to un-sorted dictionary
for i_value in range(len(keys)):
		nameID[keys[i_value]] = values[i_value]

# #Sort dictionary by string name [e.g. first=HVSTATUS_T1_M0/1151, last=HVSTATUS_T2_M7/953]
# #M0-M7 in T1 [1151-1158], M0-M7 in T2 [946-953]
nameID = OrderedDict(sorted(nameID.items(), key=lambda x: x[1]))

print nameID


###======================== GET LATEST SUBRUN TIME ======================================##

#Get Run and Subrun of the latest subrun and its start and end times 
curCommand = "SELECT run, subrun, start_time, end_time from gm2dq.subrun_time ORDER by end_time DESC limit " + str(limit) + " ;"
cur.execute(curCommand)
rows = cur.fetchall()
for row in rows:
	run=row[0]
	subrun=row[1]
	startTime=row[2]  # local time 
	endTime=row[3]    # local time 

print run, subrun, startTime, endTime


###======================== BASED ON THE LATEST SUBRUN: FIND HV RECORDS FOR EACH STATION ======================================##

for i_station in range(0, stationN):

	scidFirst=nameID.keys()[i_station*moduleN]  # get ID of the 1st module in the station 
	scidLast=nameID.keys()[(moduleN-1)+moduleN*i_station] #get ID of the last module in the stations

	#Select 8 HV records for the given run and subrun 
	curCommand = "SELECT scid, value, time FROM gm2tracker_sc.slow_control_data WHERE (" +str(scidFirst) + "<= scid AND scid <= " +str(scidLast) + " AND )  ORDER by scid ASC;"
	cur.execute(curCommand)
	rows = cur.fetchall()
	for row in rows:
		scid=row[0] # Module ID (e.g. 1151 == M0 in T1)
		value=format(int(row[1]), '08b')  # Decimal -> Binary (255 -> 11111111 etc. )    
		time=datetime.datetime.fromtimestamp(row[2]).strftime('%Y-%m-%d %H:%M:%S')  # UTC time timestamp to local time 
		print scid, value, time 







