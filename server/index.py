from flask import Blueprint
import peewee
from peewee import *
import timeit
from datetime import datetime, timedelta
import time
import config

index = Blueprint('index', __name__)
db = MySQLDatabase(config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

class Access(peewee.Model):
    id = PrimaryKeyField()
    at = peewee.DateTimeField()
    ip = peewee.CharField(index=True)
    guid = peewee.CharField(index=True)
    clientVersion = peewee.CharField(index=True)
    apiVersion = peewee.IntegerField(index=True)
    knownMarkets = peewee.IntegerField()
    maxTradegoodId = peewee.IntegerField()
    sentMarkets = peewee.IntegerField()
    sentPrices = peewee.IntegerField()
    processTime = peewee.DoubleField()

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

@index.before_request
def before_request():
    db.connect()
    db.create_tables([Access, AccessStars], safe=True)

@index.after_request
def after_request(response):
    db.close()
    return response

@index.route("/", methods=['GET'])
def show():
    ret = "<h1 style='color:blue'>TCE Relay for Elite Dangerous</h1><p>See <a href='https://forums.frontier.co.uk/showthread.php/223056-RELEASE-Trade-Computer-Extension-Mk-II'>https://forums.frontier.co.uk/showthread.php/223056-RELEASE-Trade-Computer-Extension-Mk-II</a></p>"
    ret += "<p>You can find the client and server source code on GitHub: <a href='https://github.com/flattermann/TCE-Relay'>https://github.com/flattermann/TCE-Relay</a></p>"

    t1 = time.clock()

    accCount = Access.select().count()
    accStarsCount = AccessStars.select().count()

    sumKnownMarkets = Access.select(fn.Sum(Access.knownMarkets)).scalar()
    sumSentMarkets = Access.select(fn.Sum(Access.sentMarkets)).scalar()
    sumSentPrices = Access.select(fn.Sum(Access.sentPrices)).scalar()
    sumProcessTime = Access.select(fn.Sum(Access.processTime)).scalar()

    sumReqStarsList = AccessStars.select(fn.Sum(AccessStars.reqStarsListSize)).scalar()
    sumSentStars = AccessStars.select(fn.Sum(AccessStars.sentStars)).scalar()
    sumStarsProcessTime = AccessStars.select(fn.Sum(AccessStars.processTime)).scalar()

    ret += "<p>"
    ret += "The DB was accessed <em>{:,}</em> times.<br/>".format(accCount+accStarsCount)
    ret += "</p>"
    ret += "<p>"
    ret += "Data for <em>{:,}</em> markets was requested.".format(sumKnownMarkets)
    ret += "We've delivered <em>{:,}</em> prices for <em>{:,}</em> markets<br/>".format(sumSentPrices, sumSentMarkets)
    ret += "In total this took us <em>{:,}</em> seconds, average process time per call was <em>{:.3}</em>s.".format(int(sumProcessTime), sumProcessTime/accCount)
    ret += "</p>"
    ret += "<p>"
    ret += "Spectral classes were requested using compressed lists with <em>{:,}</em> entries.".format(sumReqStarsList)
    ret += "We've delivered the spectral classes for <em>{:,}</em> stars<br/>".format(sumSentStars)
    ret += "In total this took us <em>{:,}</em> seconds, average process time per call was <em>{:.3}</em>s.".format(int(sumStarsProcessTime), sumStarsProcessTime/accStarsCount)
    ret += "</p>"

    t2 = time.clock()

    ret += "<p>Serving this page took "+str(t2-t1)+" seconds</p>"

    processTime = (t2-t1)

    return ret
