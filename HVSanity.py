# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Modified: 26 February 2018
# * /

# Sanity Checks on the tracker_hv table 

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import datetime
import time
import argparse, sys
 
######## HELPER FUNCTIONS AND ARGUMENTS ########

parser = argparse.ArgumentParser(description='psql control')
parser.add_argument('-l', '--limit', help='limit for the returned query')
args = parser.parse_args()
limit = args.limit

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

###========================SANITY CHEKCS===============================##


station=[]
hv_status=[]
run=[]
subrun=[]

cur.execute("SELECT station, hv_status, run, subrun FROM gm2dq.tracker_hv")
rows = cur.fetchall()
for row in rows:
	station.append(row[0])
	hv_status.append(row[1])
	run.append(row[2])
	subrun.append(row[3])


#print station[1:5], hv_status[1:4], run[1:4], subrun[1:4]

allOff = "0000000000000000000000000000000000000000000000000000000000000000"
allOn =  "1111111111111111111111111111111111111111111111111111111111111111"


for i in range (0, len(hv_status)):
	if (hv_status[i] ==  allOff):
		k=0
		#print "All off"
		#print hv_status[i]
	elif (hv_status[i] == allOn):
		k=1
		#print "All on"
		#print hv_status[i]
	else:
		print "Spark", hv_status[i]
		print "Station ", station[i]
		print "In Run", run[i], " subrun ", subrun[i]
		cur.execute("SELECT start_time, end_time FROM gm2dq.subrun_time WHERE run =  "+ str(run[i]) +" AND subrun = "+ str(subrun[i])+ " );"
		rows = cur.fetchall()
		for row in rows:
			startTime = row[0]
			endTime = row[0]

		print "Occurred between ", startTime, " and ", endTime

	

###========================CLOSE CONNECTION===============================##

# close communication with the databaes
cur.close()
cnx.close()




