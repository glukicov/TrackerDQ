# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Created: 13 December 2017
# *   Modified: 16 January 2018
# * /

# Read the HV status (from the online DB) and put the status to a 
# DQ Table ('HV_status') in the online DB

# To run the script from (e.g.) g2be1:/home/daq/glukicov/TrackerDQ 
# 1) python HVDQ.py

import psycopg2  #db query
import json     # dbconnetion.json
from collections import OrderedDict # sort dictionaries
import datetime #for testing [epoch time -> UTC]

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

#Sort dictionary by string name [e.g. first=HVSTATUS_T1_M0/1151, last=HVSTATUS_T2_M7/953]
#M0-M7 in T1 [1151-1158], M0-M7 in T2 [946-953]
nameID = OrderedDict(sorted(nameID.items(), key=lambda x: x[1]))

limit=8  # for testing XXX 

timeStamp=[0 for i_cord in xrange(statationN) ]  #HV status is recorded every ~5 mins [T1/T2 offset by ~5s]
HV_statusStations=[0 for i_cord in xrange(statationN) ]  #e.g(1111 1111)

tmpStatus="" #tmp status holder

for i_station in range(0, statationN):
	print "i_station=", i_station
	scidFirst=nameID.keys()[i_station*moduleN]  # get ID of the 1st module in the station 
	scidLast=nameID.keys()[(moduleN-1)+moduleN*i_station] #get ID of the last module in the stations
	# Select entries for modules in this station, ordered in time, limit to xx
	curCommand = "select * from slow_control_data where (scid>= " + str(scidFirst).strip() + " AND scid<= " + str(scidLast).strip() + " ) ORDER BY time DESC limit " + str(limit) + " ;"  
	cur.execute(curCommand)
	rows = cur.fetchall()
	print curCommand
	for i_module in range(0, moduleN):
		#current module ID [the returned list is not guaranteed to be in order]
		scidModule=nameID.keys()[i_module+i_station*(moduleN)]
		#cycle through returned modules IDs until the right one
		for item in rows:
			if (int(item[1])==scidModule): # getting the right module
				#if on
				if (int(item[2])==255):        
					tmpStatus=tmpStatus + "1"
				#if off
				else:
					tmpStatus=tmpStatus + "0"

	# assemble full "8 bit" status for the station  
	HV_statusStations[i_station]=tmpStatus
	
	#Timestamps are the same for all modules in that station 
	timeStamp[i_station]=int(item[3]) #epoch timestamp
	tmpStatus="" # reset

print 'Tracker 1 HV status: ', HV_statusStations[0]
print 'Tracker 2 HV status: ', HV_statusStations[1]
print 'Timestamp 1', datetime.datetime.fromtimestamp(int(timeStamp[0])).strftime('%Y-%m-%d %H:%M:%S')
print 'Timestamp 2', datetime.datetime.fromtimestamp(int(timeStamp[1])).strftime('%Y-%m-%d %H:%M:%S')
meanTime = int( ( timeStamp[0] + timeStamp[1] ) / 2) 
print 'Mean Timestamp: ', datetime.datetime.fromtimestamp(int(meanTime)).strftime('%Y-%m-%d %H:%M:%S')

#"Write HV status and time to the table in the gm2dq schema  
insCommand = "insert into gm2dq.tracker_hv values (B'"+str(HV_statusStations[0]) + "' , B'" + str(HV_statusStations[1]) + "' , "  +str(meanTime)  + ") ;" 
cur.execute(str(insCommand))
cnx.commit()


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
#     hv_status  BIGINT ARRAY[3],
#     run        integer,
#     subrun     integer
# );

# CREATE TABLE tracker_hv (
#     hv_status  integer ARRAY[3],
#     time integer
# );

# CREATE TABLE tracker_hv (
#     station_1  BIT(8),
#     station_2  BIT(8),
#     time integer
# );


