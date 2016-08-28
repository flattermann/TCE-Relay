# Find difference between Stars.db and systems.json

import json
import sqlite3
import timeit
import os

updateStars = True

scriptDir = os.path.dirname(os.path.realpath(__file__))

# You MUST have 64bit python for this or you'll get a MemoryError
systems = json.load(open(scriptDir+'/systems.json', encoding="iso8859-1"))
connStars = sqlite3.connect(scriptDir+"/TCE_Stars.db")
connStars.row_factory = sqlite3.Row

c = connStars.cursor()
c.execute("SELECT * FROM public_Stars")
stars = c.fetchall()

systemsCache = {}

def getSystem(id):
    global systemsCache
    if len(systemsCache) == 0:
        t1 = timeit.default_timer()
        for system in systems:
            systemsCache[int(system["id"])] = system
        t2 = timeit.default_timer()
        print ("Created systemsCache with", len(systemsCache), "entries in", t2-t1, "seconds")
    try:
        return systemsCache[int(id)]
    except KeyError:
        return None

def updateStar(starId, starName, x, y, z):
    global connStars
    c = connStars.cursor()
    c.execute("UPDATE public_Stars SET StarName=?, X=?, Y=?, Z=? WHERE ID=?", (starName, x, y, z, starId))

countNameDiff=0
countMissing=0
countCoordsDiff=0
count=0
for star in stars:
    count += 1
    if count % 10000 == 0:
        print(count,"/",len(stars))
    starId = star["ID"]
    system = getSystem(starId)
    if system == None:
        countMissing += 1
        print ("No matching system found for ", starId, star["StarName"])
        continue
    different=False
    if star["StarName"] != system["name"].upper():
        countNameDiff += 1
        print ("Different name for", starId, star["StarName"], system["name"].upper())
        different=True
    if star["X"] != system["x"] or star["Y"] != system["y"] or star["Z"] != system["z"] :
        countCoordsDiff += 1
        print ("Different coords for", starId, star["X"], system["x"], star["Y"], system["y"], star["Z"], system["z"])
        different=True
    if updateStars and different:
        print("    -> Updating star")
        updateStar(starId, system["name"].upper(), system["x"], system["y"], system["z"])

if updateStars:
    connStars.commit()
    
print("Checked",len(stars),"stars against",len(systems),"systems")
print("Not in systems.json", countMissing)
print("Name difference", countNameDiff)
print("Coords difference", countCoordsDiff)
