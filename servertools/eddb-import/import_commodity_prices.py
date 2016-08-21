import csv
import timeit
import MySQLdb
import config
import os
import sqlite3
import json

# Import EDDB data to server DB
# 
# needs commodities.json and listings.csv from EDDB
# needs Resources.db from TCE

scriptDir = os.path.dirname(__file__)

t1 = timeit.default_timer()

db = MySQLdb.connect(db=config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

# These are global
commodities = json.load(open(scriptDir+'/commodities.json'))
listingsCsv = csv.DictReader(open(scriptDir+'/listings.csv'))

connResources = sqlite3.connect(scriptDir+"/Resources.db")
connResources.row_factory = sqlite3.Row

commodityIdToTceCache = {}
commodityCache = {}

def getCommodityById(commodityId):
    global commodities
    global commodityCache
    if len(commodityCache) == 0:
        t1 = timeit.default_timer()
        for commodity in commodities:
            commodityCache[commodity["id"]] = commodity
        t2 = timeit.default_timer()
        if not fromTce:
            print ("Finished... Commodity caching took", (t2-t1), "Seconds")
    return commodityCache[int(commodityId)]

def getCommodityNameFromId(commodityId):
    global commodities
    commodity = getCommodityById(commodityId)
    if commodity != None:
        return commodity["name"]
    return None

def getTceTradegoodId(commodityName):
    global connResources
    c = connResources.cursor()
    c.execute("SELECT id from public_Goods WHERE Tradegood LIKE ?", (commodityName, ))
    result = c.fetchone()
    if (result != None):
        return result["id"]
    else:
        return -1

def translateCommodityIdToTCETradegoodId(commodityId):
    global commodityIdToTceCache
    try:
        val = commodityIdToTceCache[commodityId]
    except KeyError:
#		print ("Adding to cache...")
        commodityName = getCommodityNameFromId(commodityId)
        val = getTceTradegoodId(commodityName)
        commodityIdToTceCache[commodityId] = val
    return val

rowCount = 0
updateCount = 0
updateCountSuccess = 0

db.autocommit(False)
c = db.cursor()

c.execute("DROP TABLE IF EXISTS commoditypriceTemp")
c.execute("CREATE TABLE commoditypriceTemp LIKE commodityprice")

try:
    list = []
    for row in listingsCsv:
        rowCount += 1
        if len(list) == 1000:
            c.executemany("""INSERT INTO commoditypriceTemp (stationId, commodityId, supply, buyPrice, sellPrice, demand, collectedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                list)
            list = []
        if (rowCount % 50000 == 0):
            t2 = timeit.default_timer()
            print rowCount, "rows read in", (t2-t1), "seconds"
        if (config.maxRows > 0 and rowCount >= config.maxRows):
            break        
        stationId = row["station_id"]
        commodityId = row["commodity_id"]
        supply = row["supply"]
        buyPrice = row["buy_price"]
        sellPrice = row["sell_price"]
        demand = row["demand"]
        collectedAt = row["collected_at"]
 
        tradegoodId = translateCommodityIdToTCETradegoodId(commodityId)
        if tradegoodId < 0:
            if not fromTce:
                print ("Unable to map commodityId", commodityId, getCommodityNameFromId(commodityId))
            continue

        list.append((stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt))
    
    if (len(list) > 0):
        list.append((stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt))

    db.commit()
except:
    db.rollback()

c.execute("DROP TABLE IF EXISTS commoditypriceOld")
c.execute("RENAME TABLE commodityprice TO commoditypriceOld, commoditypriceTemp TO commodityprice")

t2 = timeit.default_timer()
print ("Import took", t2-t1, "seconds")
