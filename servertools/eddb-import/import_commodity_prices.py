import csv
import timeit
import MySQLdb
import config
import os

maxRow = 0

scriptDir = os.path.dirname(__file__)

t1 = timeit.default_timer()

db = MySQLdb.connect(db=config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

listingsCsv = csv.DictReader(open(scriptDir+'/listings.csv'))

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
        if (maxRow > 0 and rowCount >= maxRow):
            break        
        stationId = row["station_id"]
        commodityId = row["commodity_id"]
        supply = row["supply"]
        buyPrice = row["buy_price"]
        sellPrice = row["sell_price"]
        demand = row["demand"]
        collectedAt = row["collected_at"]
 
        list.append((stationId, commodityId, supply, buyPrice, sellPrice, demand, collectedAt))
    
    if (len(list) > 0):
        list.append((stationId, commodityId, supply, buyPrice, sellPrice, demand, collectedAt))

    db.commit()
except:
    db.rollback()

c.execute("DROP TABLE IF EXISTS commoditypriceOld")
c.execute("RENAME TABLE commodityprice TO commoditypriceOld, commoditypriceTemp TO commodityprice")

t2 = timeit.default_timer()
print ("Import took", t2-t1, "seconds")
