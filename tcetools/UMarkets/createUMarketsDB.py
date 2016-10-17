# This script imports the markets from EDDB into the TCE DB

import json
import sqlite3
import timeit
import os

scriptDir = os.path.dirname(os.path.realpath(__file__))

stations = json.load(open(scriptDir + "/stations.json"))
systems = json.load(open(scriptDir + "/systems_populated.json"))

total=len(stations)
print("There are",total,"stations")

connDefaultMarkets = sqlite3.connect(scriptDir + "/TCE_UMarkets.db")
connDefaultMarkets.row_factory = sqlite3.Row

connResources = sqlite3.connect(scriptDir + "/Resources.db")
connResources.row_factory = sqlite3.Row

typesCache = {}
systemNamesCache = {}
economiesCache = {}
allegianceCache = {}
typeMappingEddbToTce = {1: 7, 2: 8, 3: 1, 4: 6, 5: 5, 6: 6, 7: 3, 8: 11, 9: 9, 11: 0, 12: 0, 13: 14, 14: 13, 15: 0, 17: 15}

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
		
def getTceTypeId(name):
	global connResources
	global typesCache
	if name == None:
		return 0
	try:
		return typesCache[name]
	except KeyError:
		c = connResources.cursor()
		search = name + "%"
		c.execute("SELECT id from public_MarketTypes WHERE TypeName LIKE ?", (search, ))
		result = c.fetchone()
		val = 0
		if (result != None):
			val = result["id"]
		typesCache[name] = val
		return val

def translateTypeIdEddbToTce(id):
	try:
		return typeMappingEddbToTce[id]
	except:
		return -1

def getTceEconomyId(nameArray, idx):
	global connResources
	global economiesCache
	c = connResources.cursor()
	try:
		name = nameArray[idx]
		try:
			return economiesCache[name]
		except:
			c.execute("SELECT id from public_Economy WHERE EconomyType LIKE ?", (name, ))
			result = c.fetchone()
			val = 0
			if (result != None):
				val = result["id"]
			economiesCache[name] = val
			return val
	except IndexError:
		pass
	return 0

def getTceAllegianceId(name):
	global connResources
	global allegianceCache
	if name == None:
		return 0
	try:
		return allegianceCache[name]
	except KeyError:
		c = connResources.cursor()
		c.execute("SELECT id from public_Allegiance WHERE Allegiance LIKE ?", (name, ))
		result = c.fetchone()
		val = 0
		if (result != None):
			val = result["id"]
		allegianceCache[name] = val
		return val

t1 = timeit.default_timer()

c = connDefaultMarkets.cursor()
c.execute("DELETE FROM public_Markets_UR")

typeIds = {}

#id = 0
marketCount = 0
count = 0
noSystemCount = 0
for station in stations:
	count += 1
	id=station["id"]
	name=station["name"]
	print (count,"/",total,name)
	system_id=station["system_id"]
	max_landing_pad_size=station["max_landing_pad_size"]
	distance_to_star=station["distance_to_star"]
	state=station["state"]
	allegiance=station["allegiance"]
	type_id=station["type_id"]
	type=station["type"]	
	has_blackmarket=station["has_blackmarket"]
	has_market=station["has_market"]
	has_refuel=station["has_refuel"]
	has_repair=station["has_repair"]
	has_rearm=station["has_rearm"]
	has_outfitting=station["has_outfitting"]
	has_shipyard=station["has_shipyard"]
	has_docking=station["has_docking"]
	has_commodities=station["has_commodities"]
	is_planetary=station["is_planetary"]
	import_commodities=station["import_commodities"]
	export_commodities=station["export_commodities"]
	prohibited_commodities=station["prohibited_commodities"]
	economies=station["economies"]
	selling_ships=station["selling_ships"]
	selling_modules=station["selling_modules"]

	if name != None and has_market:
		try:
			typeIdNames = typeIds[type_id]
		except KeyError:
			typeIdNames = {}
			typeIds[type_id] = typeIdNames
		
		try:
			typeIdCount = typeIdNames[type]
		except KeyError:
			typeIdCount = 0
		typeIdNames[type] = typeIdCount+1

		if system_id > 88421:
			noSystemCount += 1
		else:
			# Set defaults if data is missing
			if distance_to_star == None:
				distance_to_star = 0
			if has_refuel != 1:
				has_refuel = 0
			if has_rearm != 1:
				has_rearm = 0
			if has_repair != 1:
				has_repair = 0
			if has_outfitting != 1:
				has_outfitting = 0
			if has_shipyard != 1:
				has_shipyard = 0
			if has_blackmarket != 1:
				has_blackmarket = 0

			systemName = getSystemNameById(system_id)

			# Convert to TCE
			tce_type_id = translateTypeIdEddbToTce(type_id)
			if tce_type_id < 0:
				print ("Missing type id mapping, EDDB Type ID", type_id, type)
				tce_type_id = 0
				
			allegianceId = getTceAllegianceId(allegiance)
			eco1 = getTceEconomyId(economies, 0)
			eco2 = getTceEconomyId(economies, 1)
			
			name = name.upper()
			systemName = systemName.upper()
		
			c.execute("INSERT INTO public_Markets_UR VALUES (?"+14*",?"+")", 
				(id, name, system_id, systemName, distance_to_star, allegianceId, eco1, eco2, tce_type_id, has_refuel, has_rearm, has_repair, has_outfitting, has_shipyard, has_blackmarket))
			
			marketCount += 1
	
connDefaultMarkets.commit()
print ("Imported",marketCount,"markets")
print (noSystemCount,"markets have no star in TCE DB")

# Dump caches
# All Types from stations.json
print (typeIds)
print (typeMappingEddbToTce)
# All Types from stations.json (Markets only)
#print (typesCache)
# All economies
print (economiesCache)
# All allegiances
print (allegianceCache)
t2 = timeit.default_timer()
print (t2-t1)