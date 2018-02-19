# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Modified: 19 February 2018
# * /

# Read the tracker HV status from the online DB in gm2tracker_sc schema and put the
# collated status to a DQ Table 'HV_status' in the gm2dq schema, with the 
# associated run and subrun number from the 'subrun_time' table

# To run the script from (e.g.) g2be1:/home/daq/glukicov/TrackerDQ 
# 1) python HVDQ.py

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import datetime #epoch time -> UTC
import argparse, sys

#TODO:
# 0) Add arguments e.g. limit
# 1) Assemble code into functions 
# 2) Two types: (1) fill-once (to catch-up on old entries) (2) "crone-job" style to run iteratively 

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

###========================READ AND RECORD HV STATUS===============================##

#Defining Tracker Constants
moduleN = 8 # 8 trackers in 2 stations: 1) 8 in C7 (Station ID=#1) 2) 8 in C10 (Station ID=#2) 3) 0 in C1 (Station ID=#0)
statationN = 2 

#Containers to store a unique relations between module name (e.g.T2_M0) 
# and its HV id (scid) in a dictionary
nameID={} 
keys=[] 
values=[]

# get ids and modules names from the DB that correspond to HV data 
cur.execute("SELECT * FROM gm2tracker_sc.slow_control_items WHERE name LIKE '%HV%'")
rows = cur.fetchall()
for item in rows:
	keys.append(int(item[0]))   #scid (e.g. 1151)
	values.append(str(item[1])) #name (e.g. HVSTATUS_T1_M0)

#Add to un-sorted dictionary
for i_value in range(len(keys)):
		nameID[keys[i_value]] = values[i_value]

#Sort dictionary by string name [e.g. first=HVSTATUS_T1_M0/1151, last=HVSTATUS_T2_M7/953]
#M0-M7 in T1 [1151-1158], M0-M7 in T2 [946-953]
nameID = OrderedDict(sorted(nameID.items(), key=lambda x: x[1]))

timeStamp=[0 for i_cord in xrange(statationN) ]  #HV status is recorded every ~5 mins [T1/T2 offset by ~5s]
HV_statusStations=[0 for i_cord in xrange(statationN) ]  #e.g(1111 1111)

tmpStatus="" #tmp HV status holder

#Loop over the stations
for i_station in range(0, statationN):
	scidFirst=nameID.keys()[i_station*moduleN]  # get ID of the 1st module in the station 
	scidLast=nameID.keys()[(moduleN-1)+moduleN*i_station] #get ID of the last module in the stations
	
	# Select entries for modules in this station, ordered in time, limit to xx
	curCommand = "SELECT * FROM gm2tracker_sc.slow_control_data WHERE (scid>= " + str(scidFirst).strip() + " AND scid<= " + str(scidLast).strip() + " ) ORDER BY time ASC LIMIT " + str(limit) + " ;"  
	cur.execute(curCommand)
	rows = cur.fetchall() 
	for i_module in range(0, moduleN):
		#current module ID [the returned list is not guaranteed to be in "module name order"]
		scidModule=nameID.keys()[i_module+i_station*(moduleN)]
		#cycle through returned modules IDs until the right one
		for item in rows:
			if (int(item[1])==scidModule): # getting the right module
				# Decimal -> Binary (255 -> 11111111 etc. )    
				tmpStatus=tmpStatus + format(int(item[2]), '08b')
	
	# assemble full "8 bit" status for the station  
	HV_statusStations[i_station]=tmpStatus
	
	#Timestamps are the same for all modules in that station 
	timeStamp[i_station]=int(item[3]) #epoch timestamp
	tmpStatus="" # reset

	#Get Run and Subrun based on the timestamp
	curCommand = "SELECT run, subrun from gm2dq.subrun_time WHERE start_time >= to_timestamp(%d) ORDER by start_time ASC limit 1;" % timeStamp[i_station]
	cur.execute(curCommand)
	rows = cur.fetchall()
	for row in rows:
		run=row[0]
		subrun=row[1]

	#Write assembled data to the DQ space for that station
	# id [primary key on auto-increment], station #, hv_status, run, subrun
	insCommand = "INSERT INTO gm2dq.tracker_hv (station, hv_status, run, subrun)"
	insCommand = insCommand + "VALUES ( "+ str(i_station+1) +" ,B'"+ str(HV_statusStations[i_station]) +"' , " + str(run) + ", " + str(subrun) + ") ;" 
	cur.execute(str(insCommand))
	cnx.commit()

###========================CLOSE CONNECTION===============================##

# close communication with the databaes
cur.close()
cnx.close()


#### Creating the tracker_hv table ###

'''

CREATE TABLE gm2dq.tracker_hv (
    id  SERIAL PRIMARY KEY,
    station  smallint,
    hv_status  BIT(64),
    run integer,
    subrun integer
);

'''


