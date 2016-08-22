# This script imports the market mapping from EDDB into the TCE-Relay DB

import json
import sqlite3
import timeit
import os

scriptDir = os.path.dirname(os.path.realpath(__file__))

stations = json.load(open('c:/temp/stations.json'))
systems = json.load(open('c:/temp/systems_populated.json'))

total=len(stations)
print("There are",total,"stations")

connTceRelayClient = sqlite3.connect(scriptDir+"/TCE-RelayClient.db")
connTceRelayClient.row_factory = sqlite3.Row

systemNamesCache = {}

def getSystemNameById(id):
    global systems
    global systemNamesCache
    if len(systemNamesCache) == 0:
        for system in systems:
            systemNamesCache[system["id"]] = system["name"]
    try:
        return systemNamesCache[id]
    except KeyError:
        return None
        
t1 = timeit.default_timer()

c = connTceRelayClient.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(c.fetchall())

c.execute("DELETE FROM stationIdMappings")

#id = 0
marketCount = 0
count = 0
for station in stations:
    count += 1
    id=station["id"]
    name=station["name"]
    print (count,"/",total,name,id)
    system_id=station["system_id"]
    has_market=station["has_market"]

    if name != None and has_market:
        systemName = getSystemNameById(system_id)

        name = name.upper()
        systemName = systemName.upper()

#		print (count,"/",marketCount,name,id)
        
        c.execute("INSERT INTO stationIdMappings (stationId, stationName, systemName) VALUES (?, ?, ?)", 
            (id, name, systemName))

        # Add systemNames without ' as well
        systemNameAscii = systemName.replace("'", "")
            
        if (systemNameAscii != systemName):
            c.execute("INSERT INTO stationIdMappings (stationId, stationName, systemName) VALUES (?, ?, ?)", 
                (id, name, systemNameAscii))
            
        marketCount += 1
    
connTceRelayClient.commit()
print ("Imported",marketCount,"markets")

t2 = timeit.default_timer()
print (t2-t1)