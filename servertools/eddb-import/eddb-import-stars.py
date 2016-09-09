import csv
import timeit
import MySQLdb
import config
import os
import sqlite3
import json

# Import EDDB data to server DB
# 
# needs systems.json and bodies.json from EDDB
# needs Resources.db from TCE

scriptDir = os.path.dirname(os.path.realpath(__file__))
# Fixed number of stars in TCE
MAX_SYSTEM_ID = 88421

t1 = timeit.default_timer()

db = MySQLdb.connect(db=config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

# These are global
systems = json.load(open(scriptDir+'/systems.json'))
bodies = json.load(open(scriptDir+'/bodies.json'))

connResources = sqlite3.connect(scriptDir+"/Resources.db")
connResources.row_factory = sqlite3.Row

bodiesCache = {}

def mapStarClass(starClass):
    global connResources
    c = connResources.cursor()
    c.execute("SELECT ID from public_StarTypes WHERE StarClass=?", (starClass,))
    result = c.fetchone()
    if result == None:
        return None
    else:
        return result["ID"]

def getBodiesBySystemId(systemId):
    systemId = int(systemId)
    global bodiesCache
    if len(bodiesCache) == 0:
        t1 = timeit.default_timer()
        for body in bodies:
            if body["group_name"] == "Star":
                bodySystemId = body["system_id"]
                try:
                    bodiesCache[bodySystemId].append(body)
                except KeyError:
                    bodiesCache[bodySystemId] = [body]
        t2 = timeit.default_timer()
        print ("Created bodiesCache with", len(bodiesCache), "entries in", t2-t1, "seconds")
    try:
        return bodiesCache[systemId]
    except KeyError:
        return []

def getMainStarBySystemId(systemId):
    systemId = int(systemId)
    bodies = getBodiesBySystemId(systemId)
    for body in bodies:
        if body["is_main_star"] == True:
            return body
    if len(bodies)>0:
        # Fallback: Use first star
        return bodies[0]
    return None

db.connect()
db.create_tables([Star], safe=True)
db.autocommit(False)
c = db.cursor()

# Read bodies from JSON
getBodiesBySystemId(0)

c.execute("DELETE FROM Star")

for systemId in bodies:
    if systemId <= MAX_SYSTEM_ID:
        mainStar = getMainStarBySystemId(systemId)
        if mainStar != None:
            mainStarClass = mapStarClass(mainStar["spectral_class"])
            c.execute("INSERT INTO Star (id, x, y, z, spectralClass) values (?, ?, ?, ?)", (systemId, mainStar["x"], mainStar["y"], mainStar["z"], mainStarClass)

db.commit()

t2 = timeit.default_timer()
print "Import took {}seconds".format(t2-t1)
print "Updated {} prices".format(countUpdated)
print missingCommodityMappings
