# To run the script from (e.g.) gm2gpvm01:/gm2/app/users/glukicov/DQ_DB/scripts
# useconda 
# python readgm2onlineDB.py 

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

# channel is where you switch to your device/channel of interest
cur.execute("select time, value from g2sc_values where channel='calo1temps' limit 10")

# fetch all the rows
rows = cur.fetchall()

# printout all the data
for row in rows:
    print row[0]," ", row[1][1] # printing time and channel 1 of calo 1

# close communication with the databaes
cur.close()
cnx.close()