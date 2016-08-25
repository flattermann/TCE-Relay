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

scriptDir = os.path.dirname(os.path.realpath(__file__))

t1 = timeit.default_timer()

db = MySQLdb.connect(db=config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

# These are global
commodities = json.load(open(scriptDir+'/commodities.json'))
listingsCsv = csv.DictReader(open(scriptDir+'/listings.csv'))

connResources = sqlite3.connect(scriptDir+"/Resources.db")
connResources.row_factory = sqlite3.Row

commodityIdToTceCache = {}
commodityCache = {}
missingCommodityMappings = {}

def getCommodityById(commodityId):
    global commodities
    global commodityCache
    if len(commodityCache) == 0:
        t1 = timeit.default_timer()
        for commodity in commodities:
            commodityCache[commodity["id"]] = commodity
        t2 = timeit.default_timer()
        print "Finished... Commodity caching took {} Seconds".format(t2-t1)
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

def addMissingCommodityMapping(id, name):
    global missingCommodityMappings
    try:
        val=missingCommodityMappings[id]
        val[1] += 1
    except KeyError:
        val = [name, 1]
    missingCommodityMappings[id] = val

def translateCommodityIdToTCETradegoodId(commodityId):
    global commodityIdToTceCache
    commodityName = ""
    val = -1
    try:
        val = commodityIdToTceCache[commodityId]
    except KeyError:
#        print ("Adding to cache...")
        commodityName = getCommodityNameFromId(commodityId)
        val = getTceTradegoodId(commodityName)
        commodityIdToTceCache[commodityId] = val
    if val < 0:
        addMissingCommodityMapping(commodityId, commodityName)
    return val

rowCount = 0
updateCount = 0
updateCountSuccess = 0

db.autocommit(False)
c = db.cursor()

list = []

countUpdated=0
for row in listingsCsv:
    rowCount += 1
    if (rowCount % 50000 == 0):
        t2 = timeit.default_timer()
        print "{} rows read in {} seconds".format(rowCount, t2-t1)
    if len(list) == 50000:
#        print "Saving now " +str(len(list))
        for priceInQueue in list:
            stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt = priceInQueue

            c.execute("SELECT collectedAt FROM commodityprice WHERE stationId=%s AND tradegoodId=%s", (stationId, tradegoodId))
            result = c.fetchone()
            if result != None:
                oldCollectedAt = result[0]
            else:
                oldCollectedAt = 0
                if oldCollectedAt < collectedAt:
                    countUpdated += 1
                    c.execute("INSERT INTO commodityprice (stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        priceInQueue)

        list = []
        db.commit()
        #print "Saved {} prices".format(countUpdated)
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
#            print ("Unable to map commodityId", commodityId, getCommodityNameFromId(commodityId))
        continue

    list.append((stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt))

if (len(list) > 0):
    for priceInQueue in list:
        stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt = priceInQueue

        c.execute("SELECT collectedAt FROM commodityprice WHERE stationId=%s AND tradegoodId=%s", (stationId, tradegoodId))
        result = c.fetchone()
        if result != None:
            oldCollectedAt = result[0]
        else:
            oldCollectedAt = 0
            if oldCollectedAt < collectedAt:
                countUpdated += 1
                c.execute("INSERT INTO commodityprice (stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    priceInQueue)
    db.commit()

t2 = timeit.default_timer()
print "Import took {}seconds".format(t2-t1)
print "Updated {} prices".format(countUpdated)
print missingCommodityMappings
