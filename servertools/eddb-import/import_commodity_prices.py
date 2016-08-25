import csv
import timeit
import MySQLdb
import config
import os
import sqlite3
import peewee
from peewee import *
import json

# Import EDDB data to server DB
# 
# needs commodities.json and listings.csv from EDDB
# needs Resources.db from TCE

scriptDir = os.path.dirname(os.path.realpath(__file__))

t1 = timeit.default_timer()

db = MySQLdb.connect(db=config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

class CommodityPrice(peewee.Model):
    id = PrimaryKeyField()
    stationId = peewee.IntegerField(index=True)
    tradegoodId = peewee.IntegerField(index=True)
    supply = peewee.IntegerField(default=0)
    buyPrice = peewee.IntegerField(default=0)
    sellPrice = peewee.IntegerField(default=0)
    demand = peewee.IntegerField(default=0)
    collectedAt = peewee.IntegerField(default=0)

    class Meta:
        database = db
        indexed = (
            (('stationId', 'tradegoodId'), True)
        )
        
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

# db.autocommit(False)
#c = db.cursor()

# c.execute("DROP TABLE IF EXISTS commoditypriceTemp")
# c.execute("CREATE TABLE commoditypriceTemp LIKE commodityprice")

db.connect()

list = []
countUpdated = 0
for row in listingsCsv:
    rowCount += 1
    if len(list) == 1000:
 #       c.executemany("""INSERT INTO commoditypriceTemp (stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
#            list)
        with db.atomic():
            for price in list:
                price.save()
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
#            print ("Unable to map commodityId", commodityId, getCommodityNameFromId(commodityId))
        continue

    price, created = CommodityPrice.get_or_create(stationId=stationId, tradegoodId=tradegoodId)
    if price.collectedAt < collectedAt:
        countUpdated += 1
        # Add update to queue
        price.supply = supply
        price.buyPrice = buyPrice
        price.sellPrice = sellPrice
        price.demand = demand
        price.collectedAt = collectedAt
        list.append(price)

if (len(list) > 0):
    with db.atomic():
        for price in list:
            price.save()
#    c.executemany("""INSERT INTO commoditypriceTemp (stationId, tradegoodId, supply, buyPrice, sellPrice, demand, collectedAt) VALUES (%s, %s, %s, %s, %s, %s, %s)""",
#            list)

    # db.commit()
# except:
    # db.rollback()

# c.execute("DROP TABLE IF EXISTS commoditypriceOld")
# c.execute("RENAME TABLE commodityprice TO commoditypriceOld, commoditypriceTemp TO commodityprice")

t2 = timeit.default_timer()
print ("Import took", t2-t1, "seconds")
print ("Imported", countUpdated, "prices")
print missingCommodityMappings
