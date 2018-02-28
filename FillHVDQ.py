# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Modified: 27 February 2018
# * /

# Read the tracker HV status from the online DB in gm2tracker_sc schema and put the
# collated status to a DQ Table 'HV_status' in the gm2dq schema, with the 
# associated run and subrun number from the 'subrun_time' table

# To run the script from (e.g.) g2be1:/home/daq/glukicov/TrackerDQ 
# 1) python FillHVDQ.py

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import datetime
import time
import argparse, sys
 
######## HELPER FUNCTIONS AND ARGUMENTS ########

# parser = argparse.ArgumentParser(description='psql control')
# parser.add_argument('-l', '--limit', help='limit for the returned query')
# args = parser.parse_args()
# limit = args.limit

###===============================OPEN CONNECTION ============================================##

dbconf = None
with open('dbconnection.json', 'r') as f:
	dbconf = json.load(f)
# create a connection to the database using info from the json file
cnx = psycopg2.connect(user=dbconf['user'],
								  host=dbconf['host'],
								  database=dbconf['dbname'], port=dbconf['port'])
# query the database and obtain the result as python objects
cur=cnx.cursor() 	

###========================GET TRACKER MODULE LABELS, CONSTANTS, DB QUERY: ONCE======================================##

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

#Lists to store all data from the subrun_time table 
run=[]
subrun=[]
startTS=[]  # subrun 
endTS=[]    # subrun 

firstRecord = 1518415200 # 12 Feb 2018 (1518415200) - first subrun record

#Lists to store all data from the slow_control_data table [HV status per module as binary]
timeHV=[]   # UTC timestamp (same for all modules in that record)
valuesHV=[] # Decimal 

###======================== INTERACT WITH THE DB ======================================##

#Get Run and Subrun of the latest subrun and its start and end times 
curCommand = "SELECT run, subrun, start_time, end_time from gm2dq.subrun_time ORDER by start_time ASC;"
cur.execute(curCommand)
rows = cur.fetchall()
for row in rows:
	run.append(row[0])
	subrun.append(row[1])
	startTS.append( time.mktime(row[2].timetuple()) ) # UTC TS
	endTS.append( time.mktime(row[3].timetuple()) )  # UTC TS


#Get all HV records after 12 Feb 2018 (1518415200) - first subrun record 
#Run over 2 stations, and write to DB
for i_station in range(0, 2):
	HVstatus="" # string to accumulate HV status for station
	scidFirst=nameID.keys()[i_station*moduleN]  # get ID of the 1st module in the station 
	scidLast=nameID.keys()[(moduleN-1)+moduleN*i_station] #get ID of the last module in the stations
	#Return time assembled, module assembled ordered query 
	curCommand = "SELECT  time, value from gm2tracker_sc.slow_control_data WHERE (" +str(scidFirst) + "<= scid AND scid <= " +str(scidLast)+ " AND "+str(firstRecord)+" < time  ) ORDER by time ASC, scid ASC;"
	cur.execute(curCommand)
	rows = cur.fetchall()
	moduleCounter=0
	for row in rows:
		timestamp=row[0] # UTC timestamp  [need only 1 per station]
		HVstatus=HVstatus+format(int(row[1]), '08b')  # Decimal -> Binary (255 -> 11111111 etc. )
		moduleCounter=moduleCounter+1 

		#Once we done with record for 8 modules, collate the HV record and start on the next record 
		if (moduleCounter==moduleN):
			timeHV.append(timestamp)
			valuesHV.append(HVstatus)
			HVstatus=""
			moduleCounter=0

	#Correlated storage of HV-subrun records for that station 
	stationDB=[]
	HVstatusDB=[]
	runDB=[]
	subrunDB=[]

	#Loop over subrun records to find the HV record that occurred between subrun stop and start time
	for i_len in range(0, len(run) ):
		for i_hv in range(0, len(timeHV)):
			if( int(timeHV[i_hv])>=int(startTS[i_len]) and int(timeHV[i_hv])<int(endTS[i_len]) ):
				stationDB.append(str(i_station+1)) # label station 1 (C7) and 2 (C10)
				HVstatusDB.append(valuesHV[i_hv])  # decimal 1111....
				runDB.append(run[i_len])           # corresponding run 
				subrunDB.append(subrun[i_len])	   # corresponding subrun 
				continue
		continue

	#Zip all data for that station 
	dataDB=zip(stationDB, HVstatusDB, runDB, subrunDB)

	# Write assembled data to the DQ space for that station
	# id [primary key on auto-increment], station #, hv_status, run, subrun
	for d in dataDB:
	    cur.execute("INSERT INTO gm2dq.tracker_hv (station, hv_status, run, subrun) VALUES (%s, %s, %s, %s)", d)
	    cnx.commit()

	 #Repeat for next station 

#end of station loop 

###========================CLOSE CONNECTION===============================##

# close communication with the databaes
cur.close()
cnx.close()




