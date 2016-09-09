# Find difference between Stars.db and systems.json

import json
import sqlite3
import timeit
import os

updateStars = True

scriptDir = os.path.dirname(os.path.realpath(__file__))

# You MUST have 64bit python for this or you'll get a MemoryError
systems = json.load(open(scriptDir+'/systems.json', encoding="iso8859-1"))
bodies = json.load(open(scriptDir+'/bodies.json', encoding="iso8859-1"))
connStars = sqlite3.connect(scriptDir+"/TCE_Stars.db")
connResources = sqlite3.connect(scriptDir+"/Resources.db")
connStars.row_factory = sqlite3.Row
connResources.row_factory = sqlite3.Row

c = connStars.cursor()
c.execute("SELECT * FROM public_Stars")
stars = c.fetchall()

systemsCache = {}
bodiesCache = {}

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

def updateStar(starId, starName, x, y, z):
    global connStars
    c = connStars.cursor()
    c.execute("UPDATE public_Stars SET StarName=?, X=?, Y=?, Z=? WHERE ID=?", (starName, x, y, z, starId))

def updateStarClass(starId, starClass):
    global connStars
    c = connStars.cursor()
    c.execute("UPDATE public_Stars SET Class=? WHERE ID=?", (starClass, starId))

# def updateStarState(starId):
    # global connStars
    # c = connStars.cursor()
    # c.execute("UPDATE public_Stars SET State=0 WHERE ID=?", (starId, ))

# def updateStarNoteToEmpty(starId):
    # global connStars
    # c = connStars.cursor()
    # c.execute("UPDATE public_Stars SET Note='' WHERE ID=?", (starId, ))

def mapStarClass(starClass):
    global connResources
    c = connResources.cursor()
    c.execute("SELECT ID from public_StarTypes WHERE StarClass=?", (starClass,))
    result = c.fetchone()
    if result == None:
        return None
    else:
        return result["ID"]

countNameDiff=0
countMissing=0
countCoordsDiff=0
countClassDiff=0
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
    bodies = getBodiesBySystemId(starId)
    mainStar = getMainStarBySystemId(starId)
    if mainStar != None:
        mainStarClass = mapStarClass(mainStar["spectral_class"])
    else:
        mainStarClass = None
    different=False
    if star["StarName"] != system["name"].upper():
        countNameDiff += 1
        print ("Different name for", starId, star["StarName"], system["name"].upper())
        different=True
    if star["X"] != system["x"] or star["Y"] != system["y"] or star["Z"] != system["z"] :
        countCoordsDiff += 1
        print ("Different coords for", starId, star["X"], system["x"], star["Y"], system["y"], star["Z"], system["z"])
        different=True
    if mainStarClass != None:
        if mainStarClass != star["Class"]:
            countClassDiff += 1
            print ("Different StarClass for", starId, system["name"], star["Class"], mainStarClass)
            if updateStars:
                updateStarClass(starId, mainStarClass)
        # if star["State"] == None:
            # updateStarState(starId)
        # if star["Note"] == None:
            # updateStarNoteToEmpty(starId)
    if mainStar is None and len(bodies)>0:
        print ("No mainstar found for", starId, system["name"], len(bodies))
    if updateStars and different:
        print("    -> Updating star")
        updateStar(starId, system["name"].upper(), system["x"], system["y"], system["z"])

if updateStars:
    connStars.commit()

connStars.close()
connResources.close()

print("Checked",len(stars),"stars against",len(systems),"systems")
print("Not in systems.json", countMissing)
print("Name difference", countNameDiff)
print("Coords difference", countCoordsDiff)
print("StarClass difference", countClassDiff)