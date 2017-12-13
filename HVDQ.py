# /*
# *   Gleb Lukicov (g.lukicov@ucl.ac.uk) @ Fermilab
# *   Created: 13 December 2017
# *   Modified: 13 December 2017
# * /

# Read the HV status (from the online DB) and put the status to a 
# DQ Table (e.g. 'HV status') in the production DB

# To run the script from (e.g.) g2db:/home/daq/glukicov/
# 1) useconda 
# 2) python HVDQ.py

import psycopg2
import json

dbconf = None
with open('dbconnection.json', 'r') as f:
    dbconf = json.load(f)
# create a connection to the database using info from the json file
cnx = psycopg2.connect(user=dbconf['user'],
                                  host=dbconf['host'],
                                  database=dbconf['dbname'], port=dbconf['port'])
# query the database and obtain the result as python objects
cur=cnx.cursor() 
cur.execute("set schema 'gm2tracker_sc';")

#Now getting listing data stored in the Tracker HV SC:


#Defining Tracker Constants
moduleN = 16 # 16 trackers in 2 stations: 1) 8 in C7 2) 8 in C10
statationN = 2 


#Store a unique relations between Module name and its id (scid) in dictionary
#nameID=

cur.execute("select * from slow_control_items where name like '%HV%'")

rows = cur.fetchall()
#print rows

i_module = 0
for item in rows:
	print "item0= ", item[0] #scid 
	print "item1= ", item[1] # name (e.g. HVSTATUS_T2_M0)
	i_module += i_module

# print nameID


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

# select * from slow_control_items where name like '%HV%' limit 10;
# select * from slow_control_data where scid=946 limit 10; 

#cur.execute("select * from slow_control_data where scid=946 limit 10;")
# fetch all the rows
#rows = cur.fetchall()
#print rows

