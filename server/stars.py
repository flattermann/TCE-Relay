from flask import Blueprint, request, jsonify
import peewee
from peewee import *
import timeit
import zlib
import json
from datetime import datetime, timedelta
import time
import config

stars  = Blueprint('stars', __name__)
db = MySQLDatabase(config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])
minApiVersion = 2

class Star(peewee.Model):
    id = PrimaryKeyField()
    starClass = peewee.IntegerField()

    class Meta:
        database = db

class AccessStars(peewee.Model):
    id = PrimaryKeyField()
    at = peewee.DateTimeField()
    ip = peewee.CharField(index=True)
    guid = peewee.CharField(index=True)
    clientVersion = peewee.CharField(index=True)
    apiVersion = peewee.IntegerField(index=True)
    sentStars = peewee.IntegerField()
    processTime = peewee.DoubleField()

    class Meta:
        database = db

@stars.before_request
def before_request():
    db.connect()
    db.create_tables([Star, AccessStars], safe=True)

@stars.after_request
def after_request(response):
    db.close()
    return response

@stars.route("/stars", methods=['GET', 'POST'])
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
        return(jsonify({"error":"Version outdated, please update TCE-Relay!"}))
        
    clientVersion=jsonData["clientVersion"]
    guid=jsonData["guid"]

    list = {}
    starData = {}
    countPrices = 0

    # for market in knownMarkets[:config.marketRequestLimit]:
        # curStationPrices = []
        # marketDate=0
        # for price in CommodityPrice.select().where(CommodityPrice.stationId == market["id"], CommodityPrice.collectedAt > collectedAtMin, CommodityPrice.tradegoodId <= maxTradegoodId):
            # # Force same date on all goods
            # if marketDate==0:
                # marketDate=price.collectedAt
                # if marketDate <= market["t"]:
                    # break
            # curStationPrices.append(
                # {"tgId":price.tradegoodId, "supply":price.supply, "buyPrice":price.buyPrice, "sellPrice":price.sellPrice, "collectedAt":marketDate} 
            # )
            # countPrices += 1
        # if len(curStationPrices)>0:
            # priceData[market["id"]]=curStationPrices
        # if config.marketResponseLimit > 0 and len(priceData) > config.marketResponseLimit:
            # break

    t2 = time.clock()

    processTime = (t2-t1)
    list["starData"] = starData
    list["processTime"] = processTime

    clientIp = request.access_route[0]
    access = AccessStars(at=datetime.utcnow(), ip=clientIp, guid=guid, clientVersion=clientVersion, apiVersion=apiVersion, sentStars=len(starData), processTime=processTime)
    access.save()
    
    return jsonify(list)
