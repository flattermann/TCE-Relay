from flask import Blueprint, request, jsonify
import peewee
from peewee import *
import timeit
import zlib
import json
from datetime import datetime, timedelta
import time
import config
import types

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
    reqStarsListSize = peewee.IntegerField()
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
    reqMask = jsonData["reqMask"]
    
    list = {}
    starData = {}
    countPrices = 0

    allStars = Star.select()
    allStarsCacheById = {}
    
    for star in allStars:
        allStarsCacheById[star.id] = star.starClass
        
    starNo = 0
    countRequested = 0
    for reqItem in reqMask:
        starNo += 1
        if reqItem == "1":
            countRequested += 1
            try:
                starData[starNo] = allStarsCacheById[starNo]
            except KeyError:
                pass

    t2 = time.clock()

    processTime = (t2-t1)
    list["starData"] = starData
    list["processTime"] = processTime

    clientIp = request.access_route[0]
    access = AccessStars(at=datetime.utcnow(), ip=clientIp, guid=guid, clientVersion=clientVersion, apiVersion=apiVersion, reqStarsListSize=countRequested, sentStars=len(starData), processTime=processTime)
    access.save()
    
    return jsonify(list)
