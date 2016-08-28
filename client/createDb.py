# This script imports the market mapping from EDDB into the TCE-Relay DB

import json
import sqlite3
import timeit
import os

scriptDir = os.path.dirname(os.path.realpath(__file__))

stations = json.load(open(scriptDir+'/stations.json'))
systems = json.load(open(scriptDir+'/systems_populated.json'))

total=len(stations)
print("There are",total,"stations")

connTceRelayClient = sqlite3.connect(scriptDir+"/TCE-RelayClient.db")
connTceRelayClient.row_factory = sqlite3.Row

systemCache = {}

def getSystemNameById(id):
    system = getSystemById(id)
    if system == None:
        return None
    else:
        return system["name"]

def getSystemById(id):
    global systems
    global systemCache
    if len(systemCache) == 0:
        for system in systems:
            systemCache[system["id"]] = system
    try:
        return systemCache[id]
    except KeyError:
        return None
        
t1 = timeit.default_timer()

c = connTceRelayClient.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(c.fetchall())

c.execute("DROP TABLE stationIdMappings")
c.execute("""CREATE TABLE 'stationIdMappings' (
    'id'            INTEGER,
    'stationId'     INTEGER NOT NULL,
    'systemId'      INTEGER NOT NULL,
    'stationName'   TEXT NOT NULL,
    'systemName'    TEXT NOT NULL,
    'distanceStar'  INTEGER,
    'starX'         DOUBLE,
    'starY'         DOUBLE,
    'starZ'         DOUBLE,
    'main'          INTEGER NOT NULL,
    PRIMARY KEY(id)
    )""")
c.execute("CREATE INDEX idx_distanceStar on stationIdMappings (distanceStar)")
c.execute("CREATE INDEX idx_main on stationIdMappings (main)")
c.execute("CREATE INDEX idx_stationId on stationIdMappings (stationId)")
c.execute("CREATE INDEX idx_stationName_systemName on stationIdMappings (stationName, systemName)")
c.execute("CREATE INDEX idx_systemId on stationIdMappings (systemId)")

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
        system = getSystemById(system_id)

        name = name.upper()
        systemName = getSystemNameById(system_id).upper()
        distanceStar = station["distance_to_star"]
#		print (count,"/",marketCount,name,id)
        
        c.execute("INSERT INTO stationIdMappings (stationId, systemId, stationName, systemName, distanceStar, starX, starY, starZ, main) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
            (id, system_id, name, systemName, distanceStar, system["x"], system["y"], system["z"], 1))

        # Add systemNames without ' as well
        systemNameAscii = systemName.replace("'", "")
            
        if (systemNameAscii != systemName):
            c.execute("INSERT INTO stationIdMappings (stationId, systemId, stationName, systemName, distanceStar, starX, starY, starZ, main) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                (id, system_id, name, systemNameAscii, distanceStar, system["x"], system["y"], system["z"], 0))
            
        marketCount += 1
    
connTceRelayClient.commit()
print ("Imported",marketCount,"markets")

t2 = timeit.default_timer()
print (t2-t1)