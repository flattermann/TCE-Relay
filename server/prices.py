from flask import Blueprint, request, jsonify
import peewee
from peewee import *
import timeit
import zlib
import json
from datetime import datetime, timedelta
import time
import config

prices = Blueprint('prices', __name__)
db = MySQLDatabase(config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])
minApiVersion = 2

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

class Access(peewee.Model):
    id = PrimaryKeyField()
    at = peewee.DateTimeField()
    ip = peewee.CharField(index=True)
    guid = peewee.CharField(index=True)
    clientVersion = peewee.CharField(index=True)
    apiVersion = peewee.IntegerField(index=True)
    knownMarkets = peewee.IntegerField()
    sentMarkets = peewee.IntegerField()
    sentPrices = peewee.IntegerField()
    sentBytes = peewee.IntegerField()
    processTime = peewee.DoubleField()

    class Meta:
        database = db

@prices.before_request
def before_request():
    db.connect()
    db.create_tables([CommodityPrice, Access], safe=True)

@prices.after_request
def after_request(response):
    db.close()
    return response

@prices.route("/prices", methods=['GET', 'POST'])
def show():
    t1 = time.clock()
    data=request.data
    try:
        data=zlib.decompress(data)
    except:
        pass
    jsonData=json.loads(data)
#    json=request.get_json(force=True)

    apiVersion=jsonData["apiVersion"]
    
    if apiVersion < minApiVersion:
        return(jsonify({"error":"apiVersion must be at least "+str(minApiVersion)+", please update your client!"}))
        
    clientVersion=jsonData["clientVersion"]
    knownMarkets=jsonData["knownMarkets"]
    maxAge=jsonData["maxAge"]
    guid=jsonData["guid"]
    collectedAtMin=time.mktime((datetime.utcnow() - timedelta(days=maxAge)).timetuple())

    list = {}
    priceData = {}
    countPrices = 0
    # Limit to 1000 Markets
    for market in knownMarkets[:1000]:
        curStationPrices = []
        marketDate=0
        for price in CommodityPrice.select().where(CommodityPrice.stationId == market["id"], CommodityPrice.collectedAt > collectedAtMin):
            # Force same date on all goods
            if marketDate==0:
                marketDate=price.collectedAt
                if marketDate <= market["t"]:
                    break
            curStationPrices.append(
                {"tgId":price.tradegoodId, "supply":price.supply, "buyPrice":price.buyPrice, "sellPrice":price.sellPrice, "collectedAt":marketDate} 
            )
            countPrices += 1
        if len(curStationPrices)>0:
            priceData[market["id"]]=curStationPrices

    list["priceData"] = priceData
    list["processTime"] = processTime

    ret = jsonify(list)

    t2 = time.clock()

    processTime = (t2-t1)

    clientIp = request.access_route[0]
    access = Access(at=datetime.utcnow(), ip=clientIp, guid=guid, clientVersion=clientVersion, apiVersion=apiVersion, knownMarkets=len(knownMarkets), sentMarkets=len(priceData), sentPrices=countPrices, sentBytes=len(ret), processTime=processTime)
    access.save()
    
#    prices.logger.info("markets="+len(priceData)+"/"+len(knownMarkets)+", processTime="+(t2-t1))
    return ret
