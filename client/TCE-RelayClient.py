# TCE-Relay Client for Elite Dangerous
# 
# Import EDDB prices to TCE
# 
# Copyright (C) flattermann
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
# Note: when using --from-tce, we will output messages in the following format for easier parsing:
#
# Message					TCE should show
# PROGRESS:n,m,text			Progress meter:
#								text (n/m)
# STATUS:text				Status message:
#								text
# ERROR:text				Error message:
#								text
#
# All other messages should be ignored

import math
import json
import csv
import sqlite3
import timeit
import zlib
import requests
from datetime import datetime, timedelta, timezone
import argparse
import sys
import os
import uuid
import re

tceRelayVersion = "0.2-beta"
apiVersion = 2

parser = argparse.ArgumentParser(description='TCE-Relay Client for Elite Dangerous')

parser.add_argument('--from-tce', dest='fromTce', action='store_const',
                    const=True, default=False, help='Set by TCE Launcher to get clean output')
parser.add_argument('--max-age', '-d', dest='maxAge', type=int, action='store',
                    default=14, help='Max age for the prices in days (defaults to 14)')
parser.add_argument('--tce-path', dest='tcePath', action='store',
                    default="c:/TCE", help='Path to TCE (defaults to c:/TCE)')
parser.add_argument('--fetch-older', '-o', dest='fetchOlder', action='store_const',
                    const=True, default=False, help='DEBUG: Even fetch prices that are older than your local DB')
parser.add_argument('--stationname', '-n', dest='stationName', action='append',
                    default=None, help='DEBUG: Set station manually')
parser.add_argument('--systemname', '-N', dest='systemName', action='append',
                    default=None, help='DEBUG: Set system manually')
parser.add_argument('--id', '-i', type=int, dest='id', action='append',
                    help='DEBUG: Update station with this id')
parser.add_argument('--local-id', '-I', type=int, dest='localId', action='append',
                    help='DEBUG: Update station with this local id')
parser.add_argument('--add-market', '-a', metavar='STATIONNAME@SYSTEMNAME', dest='addMarket', action='append',
                    default=None, help='ALPHA: Add market with this name (overrides -i)')
parser.add_argument('--add-markets-near-system', '-A', metavar='SYSTEMNAME,LY,LS,WITHPLANETARY', dest='addMarketsNearSystem', action='append',
                    default=None, help='ALPHA: Add markets near system SYSTEMNAME, LY=max distance, LS=max star distance, WITHPLANETARY=Y/N, e.g. -A "LTT 9810,50,1000,N" (overrides -i)')
parser.add_argument('--offline', dest='offlineMode', action='store_const',
                    const=True, default=False, help='Offline mode (useful for -a)')
parser.add_argument('--version', '-v', action='version',
                    version=tceRelayVersion)
parser.add_argument('--verbose', dest='verbose', action='store_const',
                    const=True, default=False, help='More debug output')
                    
args = parser.parse_args()

verbose = args.verbose
maxAge = args.maxAge
tcePath = args.tcePath
fromTce = args.fromTce
fetchOlder = args.fetchOlder
onlyStationNames = args.stationName
onlySystemNames = args.systemName
addMarketList = args.addMarket
addMarketsNearSystemList = args.addMarketsNearSystem
updateById = args.id
updateByLocalId = args.localId

def getMyPath(filename=None):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        datadir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        datadir = os.path.dirname(__file__)
    if filename == None:
        return datadir
    else:
        return os.path.join(datadir, filename)

tceRelayUrl='http://tcerelay.flat09.de/prices'

# These too
connUserMarkets = sqlite3.connect(tcePath+"/db/TCE_RMarkets.db")
connDefaultMarkets = sqlite3.connect(tcePath+"/db/TCE_UMarkets.db")
connPrices = sqlite3.connect(tcePath+"/db/TCE_Prices.db")
connTceRelayClient = sqlite3.connect(getMyPath("TCE-RelayClient.db"))
connTceRelayClientLocal = sqlite3.connect(getMyPath("TCE-RelayClient_local.db"))
connStars = sqlite3.connect(tcePath+"/db/TCE_Stars.db")

connUserMarkets.row_factory = sqlite3.Row
connDefaultMarkets.row_factory = sqlite3.Row
connPrices.row_factory = sqlite3.Row
connTceRelayClient.row_factory = sqlite3.Row
connTceRelayClientLocal.row_factory = sqlite3.Row
connStars.row_factory = sqlite3.Row

connTceRelayClientLocal.cursor().execute('CREATE TABLE IF NOT EXISTS stringStore (key TEXT, value TEXT, PRIMARY KEY(key))')
# These too, our caches
localMarketIdCache = {}
stationIdCache = {}
localMarketCache = {}

def getUserMarketIdMax():
    global connUserMarkets
    c = connUserMarkets.cursor()
#	print ("Checking market", systemId, stationName)
    c.execute("SELECT ID FROM public_Markets ORDER BY ID DESC")
    result = c.fetchone()
    if (result != None):
        return result["id"]
    else:
        return -1

def getUserMarketId(systemName, stationName):
    global connUserMarkets
    if len(localMarketCache) == 0:
        c = connUserMarkets.cursor()
#	print ("Checking market", systemId, stationName)
        c.execute("SELECT * FROM public_Markets")
#        c.execute("SELECT * FROM public_Markets WHERE StarName=? AND MarketName=?", (systemName, stationName))
        markets = c.fetchall()
        for market in markets:
            key=market["StarName"]+"###"+market["MarketName"]
            localMarketCache[key] = market
    key=systemName+"###"+stationName
    val = None
    try:
        val = localMarketCache[key]
    except KeyError:
        localMarketCache[key] = val
    if val != None:
        return val["ID"]
    else:
        return -1

def getDefaultMarket(systemName, stationName):
    global connDefaultMarkets
    c = connDefaultMarkets.cursor()
    c.execute("SELECT * FROM public_Markets_UR WHERE StarName=? AND MarketName=?", (systemName, stationName))
    result = c.fetchone()
    return result

def addUserMarket(tceDefaultMarket):
    global connUserMarkets
    tdm = tceDefaultMarket
    c = connUserMarkets.cursor()
    nextId = getUserMarketIdMax() + 1
    if not fromTce:
        print ("    Adding Market", nextId, tdm["ID"])
    c.execute("INSERT INTO public_Markets ("
        "ID, MarketName, StarID, StarName, SectorID, AllegianceID, PriEconomy, SecEconomy, DistanceStar, LastDate, LastTime, "
        "MarketType, Refuel, Repair, Rearm, Outfitting, Shipyard, Blackmarket, Hangar, RareID, ShipyardID, Notes, PosX, PosY, PosZ) "
        "VALUES (?" + 24*", ?" + ")", (nextId, tdm["MarketName"], tdm["StarID"], tdm["StarName"], 0, tdm["Allegiance"], tdm["Eco1"], tdm["Eco2"], tdm["DistanceStar"], 
        0, "00:00:00", tdm["Type"], tdm["Refuel"], tdm["Repair"], tdm["Rearm"], tdm["Outfitting"], tdm["Shipyard"], tdm["Blackmarket"], 0, 0, 0, "", 0, 0, 0))
    return nextId

def calcDistance(p1,p2):
    return math.sqrt((p2[0] - p1[0]) ** 2 +
                     (p2[1] - p1[1]) ** 2 +
                     (p2[2] - p1[2]) ** 2)

def getLocalDbString(key):
    global connTceRelayClientLocal
    c = connTceRelayClientLocal.cursor()
    c.execute("SELECT value from stringStore where key=?", (key,))
    value = c.fetchone()
    if value != None:
        return value["value"]
    else:
        return None

def setLocalDbString(key, value):
    global connTceRelayClientLocal
    c = connTceRelayClientLocal.cursor()
    c.execute("REPLACE into stringStore (key, value) VALUES (?, ?)", (key, value))
    connTceRelayClientLocal.commit()
    
def getGuid():
    guid = getLocalDbString("guid")
    if guid == None:
        guid=str(uuid.uuid4())
        setLocalDbString("guid", guid)
    return guid
    
def showProgress(curProgress, maxProgress, text="Progress"):
    print ("PROGRESS:"+str(curProgress)+","+str(maxProgress)+","+text)

def showStatus(text):
    print ("STATUS:"+text)

def showError(text):
    print ("ERROR:"+text)
    
def getLocalMarketId(stationId):
    return localMarketIdCache[int(stationId)]
    
def getStationId(marketName, starName, marketId):
    global connTceRelayClient
    
    try:
        val = stationIdCache[int(marketId)]
    except KeyError:
        marketName = marketName.upper()
        starName = starName.upper()
        c = connTceRelayClient.cursor()
    
        c.execute("SELECT stationId FROM stationIdMappings WHERE stationName=? AND systemName=?", (marketName, starName))
        result = c.fetchone()

        if result != None:
            val = result["stationId"]
        else:
            # Try planetary (systemName = systemName: planetName)
            starName = re.sub(":.*", "", starName)
            c.execute("SELECT stationId FROM stationIdMappings WHERE stationName=? AND systemName=?", (marketName, starName))
            result = c.fetchone()
            if result != None:
                val = result["stationId"]
            else:
                # Giving up
                val = -1
        
        stationIdCache[int(marketId)] = val
        localMarketIdCache[val] = int(marketId)
    return val

def getJsonRequest():
    showStatus("Preparing request")
    global connUserMarkets
    global maxAge
    global guid
    
    t1 = timeit.default_timer()

    cUM = connUserMarkets.cursor()

    cUM.execute("SELECT * FROM public_Markets")

    jsonData = {}
    jsonData["apiVersion"] = apiVersion
    jsonData["clientVersion"] = tceRelayVersion
    jsonData["knownMarkets"] = []
    jsonData["maxAge"] = maxAge
    jsonData["guid"] = getGuid()
    
    count = 0
    markets = cUM.fetchall()
    for market in markets:
        count += 1
        if fromTce: # and count % 10 == 0:
            showProgress(count, len(markets), "Preparing request")
        localMarketId=market["ID"]
        if updateByLocalId == None or localMarketId in updateByLocalId:
            marketName=market["MarketName"]
            starName=market["StarName"]
            stationId = getStationId(marketName, starName, localMarketId)
            oldDateStr = market["LastDate"]
            oldTimeStr = market["LastTime"]
            oldTimeArray = oldTimeStr.split(":")
            oldH = oldTimeArray[0]
            oldM = oldTimeArray[1]
            oldS = oldTimeArray[2]
            oldDate = datetime(613, 12, 31) + timedelta(days=int(oldDateStr), hours=int(oldH), minutes=int(oldM), seconds=int(oldS))

            if stationId >= 0:
                try:
                    # Get UTC timestamp
                    t=int(oldDate.replace(tzinfo=timezone.utc).timestamp())
                except OverflowError:
                    t=0
                # print(marketName, starName, stationId, oldDateStr, oldTimeStr, t)
                if fetchOlder:
                    t=0
                if ((onlyStationNames == None or marketName in onlyStationNames) and 
                    (onlySystemNames == None or starName in onlySystemNames) and
                    (updateById == None or stationId in updateById)):
                    jsonData["knownMarkets"].append({"id":stationId, "t":t})
                else:
                    if verbose and not fromTce:
                        print("Skipping market because of command line params:", marketName, starName, stationId)
                    
            else:
                if not fromTce:
                    print(marketName, starName, stationId, "ID not found!")
        else:
            if verbose and not fromTce:
                print("Skipping market because of --local-id command line params:", marketName, starName, stationId)
        
        # if len(jsonData["knownMarkets"]) > 50:
            # break
        # break
        
        
    t2 = timeit.default_timer()
    if not fromTce:
        print ("Requesting data for",len(jsonData["knownMarkets"]),"markets")
        print ("getJsonRequest took",(t2-t1),"seconds")
    return jsonData

def sendRequest(jsonData):
    showStatus("Sending request")
    t1 = timeit.default_timer()
    # print(jsonData)

    additional_headers = {}
    additional_headers['content-encoding'] = 'gzip'
    jsonAsString = json.dumps(jsonData)
    compressedJson = zlib.compress(jsonAsString.encode())

    if not fromTce:
        print ("Compressed JsonRequest from", len(jsonAsString), "to", len(compressedJson), "bytes")
    r = requests.post(tceRelayUrl, data=compressedJson, headers=additional_headers)

    if not fromTce:
        print(r.status_code)
        print (r.headers)

    jsonResponse=r.json()

    t2 = timeit.default_timer()
    if not fromTce:
        print ("sendRequest took",(t2-t1),"seconds")
    return jsonResponse

def processJsonResponse(jsonResponse):
    showStatus("Processing response")
    t1 = timeit.default_timer()
    if "error" in jsonResponse:
        showError(jsonResponse["error"])
        exit(4)
    if not fromTce:
        print("ServerProcessTime", jsonResponse["processTime"])
    priceData=jsonResponse["priceData"]
    # print(priceData)
    
    if not fromTce:
        print("Got prices for",len(priceData),"markets")
    
    countPricesUpdated = 0
    countStationsUpdated = 0
    countStations = 0
    for stationId in priceData:
        countStations += 1
        if fromTce: # and countStations % 10 == 0:
            showProgress(countStations, len(priceData), "Updating prices")
        curPriceData = priceData[stationId]
        if len(curPriceData) > 0:
            countStationsUpdated += 1
            countPricesUpdated += updateTcePriceData(stationId, curPriceData)
    t2 = timeit.default_timer()
    if not fromTce:
        print ("processJsonResponse took",(t2-t1),"seconds")
        print ("Updated",countStationsUpdated,"stations with",countPricesUpdated,"prices")
    else:
        #text="Updated "+strcountStationsUpdated+" stations with "+countPricesUpdated+"prices"
        #showStatus(text)
        showStatus("Finished")
    
# Update one market
def updateTcePriceData(stationId, curPriceData):
    localMarketId = getLocalMarketId(stationId)
    if localMarketId < 0 or curPriceData == None or len(curPriceData) == 0:
        return
    deletePricesForMarket(localMarketId)
    count=0
    for curPrice in curPriceData:
        tradegoodId = curPrice["tgId"]
        supply = curPrice["supply"]
        buyPrice = curPrice["buyPrice"]
        sellPrice = curPrice["sellPrice"]
        collectedAt = curPrice["collectedAt"]
        success = addTceSinglePrice(localMarketId, tradegoodId, supply, buyPrice, sellPrice)
        if success:
            count += 1
    setLocalMarketLastDate(localMarketId, collectedAt)
    return count

def deletePricesForMarket(localMarketId):
    global connPrices
    c = connPrices.cursor()
    c.execute("DELETE FROM public_MarketPrices WHERE MarketID=?", (localMarketId, ))
    
def setLocalMarketLastDate(localMarketId, collectedAt):
    global connUserMarkets
    c = connUserMarkets.cursor()
    if verbose and not fromTce:
        print ("Updating LastDate for localMarketId", localMarketId, "to", collectedAt)
    # Magic date calculation :)
    collectedDate = datetime.utcfromtimestamp(collectedAt)
    tceBase = collectedDate - datetime(613, 12, 31)
    newTceDate = str(int(tceBase/timedelta(days=1)))
    newTceTime = collectedDate.strftime("%H:%M:%S")
    c.execute("UPDATE public_Markets set LastDate=?, LastTime=? WHERE id=?", (newTceDate, newTceTime, localMarketId))
    
# Update a single price
def addTceSinglePrice(localMarketId, tradegoodId, supply, buyPrice, sellPrice):
    global connPrices
    global connUserMarkets
    c = connPrices.cursor()
    c.execute("INSERT INTO public_MarketPrices ("
        "MarketID, GoodID, Buy, Sell, Stock) "
        "VALUES (?, ?, ?, ?, ?)",
        (localMarketId, tradegoodId, buyPrice, sellPrice, supply))
    return True
    # print("Updating price", localMarketId, tradegoodId, supply, buyPrice, sellPrice, collectedAt)
    #print ("Local market ID", localMarketId)

def addMarkets(list):
    for marketFullName in list:
        marketName, systemName = marketFullName.split("@")
        if getUserMarketId(systemName, marketName) < 0:
            print ("Adding market", marketName.upper(), systemName.upper())
            defaultMarket = getDefaultMarket(systemName, marketName)
            if defaultMarket != None:
                newId = addUserMarket(defaultMarket)
                stationId = getStationId(marketName, systemName, newId)
                updateById.append(stationId)
            else:
                print ("  No matching market found in UMarkets")

def getMarketsForSystem(systemId, maxStarDistance=1000, planetary=False):
    global connDefaultMarkets
    c = connDefaultMarkets.cursor()
    if planetary:
        planetarySql=""
    else:
        planetarySql=" AND (Type<13 OR Type>15)"
    c.execute("SELECT * from public_Markets_UR WHERE StarID=? AND DistanceStar<=?"+planetarySql, (systemId, maxStarDistance))
    return c.fetchall()

def getStarByName(name):
    global connStars
    name = name.upper()
    c = connStars.cursor()
    c.execute("SELECT * from Public_Stars WHERE StarName=?", (name,))
    return c.fetchone()

def getStarsNear(x, y, z, ly):
    global connStars
    c = connStars.cursor()
    c.execute("SELECT * from Public_Stars")
    stars = c.fetchall()
    list = []
    for star in stars:
        if calcDistance((x, y, z), (star["X"], star["Y"], star["Z"])) <= ly:
            list.append(star)
    return list
    
def addMarketsNearSystem(list):
    for baseSystemFull in list:
        baseSystemName, distanceLY, distanceLS, planetary = baseSystemFull.split(',')
        baseSystemName = baseSystemName.upper()
        distanceLY = int(distanceLY)
        distanceLS = int(distanceLS)
        if planetary in ('y', 'Y', 1):
            planetary=True
        else:
            planetary=False
        star = getStarByName(baseSystemName)
        if star != None:
            print ("Searching for markets near", baseSystemName, star["X"], star["Y"], star["Z"], "within", distanceLY, "LY, maxDistanceToStar", distanceLS, "LS , withPlanetary", planetary)
            nearStars=getStarsNear(star["X"], star["Y"], star["Z"], distanceLY)
            print (len(nearStars), "near stars found!")
            nearMarkets = []
            for nearStar in nearStars:
                nearMarkets.extend(getMarketsForSystem(nearStar["ID"], distanceLS, planetary))
            print(len(nearMarkets), "near markets found!")
            countAdded=0
            for nearMarket in nearMarkets:
                marketName = nearMarket["MarketName"]
                systemName = nearMarket["StarName"]
                if getUserMarketId(systemName, marketName) < 0:
                    countAdded+=1
                    print (countAdded, "Adding market", marketName, systemName)
                    newId = addUserMarket(nearMarket)
                    stationId = getStationId(marketName, systemName, newId)
                    updateById.append(stationId)
        else:
            print ("Star not found:", baseSystemName)

t1 = timeit.default_timer()

if addMarketsNearSystemList != None and len(addMarketsNearSystemList) > 0:
    updateById = []
    addMarketsNearSystem(addMarketsNearSystemList)
elif addMarketList != None and len(addMarketList) > 0:
    updateById = []
    addMarkets(addMarketList)

if not args.offlineMode:
    try:
        jsonData = getJsonRequest()
    except:
        showError("Unable to create request!")
        exit(1)

    try:
        jsonResponse = sendRequest(jsonData)
    except:
        showError("Server unreachable!")
        exit(2)

    try:
        processJsonResponse(jsonResponse)
    except:
        showError("Unable to parse response!")
        exit(3)
    
connUserMarkets.commit()
connPrices.commit()

t2 = timeit.default_timer()
if not fromTce:
    print ("Total runtime:",(t2-t1),"seconds")