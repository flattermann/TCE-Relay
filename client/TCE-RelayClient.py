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
from sys import exit
import os
import uuid
import re
import locale
import traceback

tceRelayVersion = "0.3.6-beta"
apiVersion = 2

locale.setlocale(locale.LC_ALL, '')

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
parser.add_argument('--list-markets', '-l', metavar='SYSTEMNAME', dest='listMarketsBySystenName', action='append',
                    default=None, help='DEBUG: List markets by system name')
parser.add_argument('--i-know-the-risks', dest='iKnowTheRisks', action='store_const',
                    const=True, default=False, help='Enable experimental features that will probably harm you DB')
parser.add_argument('--add-market', '-a', metavar='STATIONNAME@SYSTEMNAME', dest='addMarket', action='append',
                    default=None, help='EXPERIMENTAL: Add market with this name (overrides -i)')
parser.add_argument('--add-markets-near-system', '-A', metavar='SYSTEMNAME,LY,LS,WITHPLANETARY', dest='addMarketsNearSystem', action='append',
                    default=None, help='EXPERIMENTAL: Add markets near system SYSTEMNAME, LY=max distance, LS=max star distance, WITHPLANETARY=Y/N, e.g. -A "LTT 9810,50,1000,N" (overrides -i)')
parser.add_argument('--clear-prices', dest='clearPrices', action='store_const',
                    const=True, default=False, help='EXPERIMENTAL: Clear all prices from DB')
parser.add_argument('--remove-problematic', dest='removeProblematic', action='store_const',
                    const=True, default=False, help='EXPERIMENTAL: Remove problematic markets (errors, duplicates...)')
parser.add_argument('--offline', dest='offlineMode', action='store_const',
                    const=True, default=False, help='Offline mode (useful for -a)')
parser.add_argument('--version', '-v', action='version',
                    version=tceRelayVersion)
parser.add_argument('--verbose', dest='verbose', action='store_const',
                    const=True, default=False, help='More debug output')
parser.add_argument('--dry-run', dest='dryRun', action='store_const',
                    const=True, default=False, help='Do not actually change anything - just simulate')
                    
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

tceRelayUrl = 'http://tcerelay.flat09.de'
tceRelayUrlPrices = tceRelayUrl + '/prices'
tceRelayUrlStars = tceRelayUrl + '/stars'

# These too
connUserMarkets = sqlite3.connect(tcePath+"/db/TCE_RMarkets.db")
connDefaultMarkets = sqlite3.connect(tcePath+"/db/TCE_UMarkets.db")
connPrices = sqlite3.connect(tcePath+"/db/TCE_Prices.db")
connTceRelayClient = sqlite3.connect(getMyPath("TCE-RelayClient.db"))
connTceRelayClientLocal = sqlite3.connect(getMyPath("TCE-RelayClient_local.db"))
connStars = sqlite3.connect(tcePath+"/db/TCE_Stars.db")
connResources = sqlite3.connect(tcePath+"/db/Resources.db")

connUserMarkets.row_factory = sqlite3.Row
connDefaultMarkets.row_factory = sqlite3.Row
connPrices.row_factory = sqlite3.Row
connTceRelayClient.row_factory = sqlite3.Row
connTceRelayClientLocal.row_factory = sqlite3.Row
connStars.row_factory = sqlite3.Row
connResources.row_factory = sqlite3.Row

connTceRelayClientLocal.cursor().execute('CREATE TABLE IF NOT EXISTS stringStore (key TEXT, value TEXT, PRIMARY KEY(key))')

# These too, our caches
localMarketIdCache = {}
stationIdCache = {}
localMarketCache = {}

maxTradegoodId = -1

EMPTY_MAGIC = "#EMPTY"

def getMaxTradegoodId():
    global connResources
    global maxTradegoodId
    if maxTradegoodId <= 0:
        c = connResources.cursor()
        c.execute("SELECT max(ID) as maxId FROM public_Goods")
        result = c.fetchone()
        if result != None:
            maxTradegoodId = result["maxId"]
            if verbose:
                print ("MaxTradegoodId is", maxTradegoodId)
    return maxTradegoodId

def getUserMarketIdNext():
    global connUserMarkets
    c = connUserMarkets.cursor()
    c.execute("SELECT (t1.ID+1) as nextId FROM public_Markets AS t1 LEFT JOIN public_Markets as t2 ON t1.ID+1 = t2.ID WHERE t2.ID IS NULL limit 1")
    result = c.fetchone()
    if result != None:
        return result["nextId"]
    else:
        return getUserMarketIdMax()+1

def getUserMarketIdMax():
    global connUserMarkets
    c = connUserMarkets.cursor()
#	print ("Checking market", systemId, stationName)
    c.execute("SELECT ID FROM public_Markets ORDER BY ID DESC")
    result = c.fetchone()
    if (result != None):
        return result["id"]
    else:
        return 0

def getUserMarketId(systemName, stationName):
    global connUserMarkets
    global localMarketCache
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

def addUserMarket(tceDefaultMarket, removeFromDefaultMarkets=True):
    global connUserMarkets
    global connDefaultMarkets
    tdm = tceDefaultMarket
    if getStationId(tdm["MarketName"], tdm["StarName"], -1) < 0:
        print ("Cannot add market", tdm["MarketName"], tdm["StarName"], "because I could not find its EDDB ID")
        return -1
    elif tdm["Type"] <= 0:
        print ("Cannot add market", tdm["MarketName"], tdm["StarName"], "because its station type is unknown")
        return -1
    elif tdm["Allegiance"] <= 0:
        print ("Cannot add market", tdm["MarketName"], tdm["StarName"], "because its allegiance is unknown")
        return -1
    else:
        c = connUserMarkets.cursor()
        nextId = getUserMarketIdNext()
        if not fromTce:
            print ("    Adding Market", nextId, tdm["ID"])
        if not args.dryRun:
            c.execute("INSERT INTO public_Markets ("
                "ID, MarketName, StarID, StarName, SectorID, AllegianceID, PriEconomy, SecEconomy, DistanceStar, LastDate, LastTime, "
                "MarketType, Refuel, Repair, Rearm, Outfitting, Shipyard, Blackmarket, Hangar, RareID, ShipyardID, Notes, PosX, PosY, PosZ) "
                # New in TCE 1.4: Faction, FactionState, Government, Security, BodyName (all text)
                "VALUES (?" + 24*", ?" + ")", (nextId, tdm["MarketName"], tdm["StarID"], tdm["StarName"], 0, tdm["Allegiance"], tdm["Eco1"], tdm["Eco2"], tdm["DistanceStar"], 
                0, "00:00:00", tdm["Type"], tdm["Refuel"], tdm["Repair"], tdm["Rearm"], tdm["Outfitting"], tdm["Shipyard"], tdm["Blackmarket"], 0, 0, 0, "", 0, 0, 0))
            cDM = connDefaultMarkets.cursor()
            if removeFromDefaultMarkets:
                cDM.execute("DELETE FROM public_Markets_UR where ID=?", (tdm["ID"], ))
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
    try:
        return localMarketIdCache[int(stationId)]
    except KeyError:
        return -1
    
def getStationId(marketName, starName, marketId=-1):
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
        
        if int(marketId) > 0:
            stationIdCache[int(marketId)] = val
            localMarketIdCache[val] = int(marketId)
    return val

def getJsonRequestDefault():
    showStatus("Preparing request")
    jsonData = {}
    jsonData["apiVersion"] = apiVersion
    jsonData["clientVersion"] = tceRelayVersion
    jsonData["guid"] = getGuid()
    return jsonData

def sendRequestDefault(jsonData, url):
    showStatus("Sending request")
    t1 = timeit.default_timer()
    # print(jsonData)

    if verbose:
        print(jsonData)

    additional_headers = {}
    additional_headers['content-encoding'] = 'gzip'
    jsonAsString = json.dumps(jsonData)
    compressedJson = zlib.compress(jsonAsString.encode())

    if not fromTce:
        print ("Compressed JsonRequest from", len(jsonAsString), "to", len(compressedJson), "bytes")
    r = requests.post(url, data=compressedJson, headers=additional_headers)

    if not fromTce:
        print(r.status_code)
        print (r.headers)

    jsonResponse=r.json()

    t2 = timeit.default_timer()
    if not fromTce:
        print ("sendRequest took",(t2-t1),"seconds")
    return jsonResponse

def getJsonRequestForPrices():
    global connUserMarkets
    global maxAge
    
    t1 = timeit.default_timer()

    cUM = connUserMarkets.cursor()

    cUM.execute("SELECT * FROM public_Markets")

    jsonData = getJsonRequestDefault()
    jsonData["knownMarkets"] = []
    jsonData["maxAge"] = maxAge
    jsonData["maxTradegoodId"] = getMaxTradegoodId()
    
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

            if stationId >= 0:
                if ((onlyStationNames == None or marketName in onlyStationNames) and 
                    (onlySystemNames == None or starName in onlySystemNames) and
                    (updateById == None or stationId in updateById)):
                    try:
                        # Get UTC timestamp
                        t=parseTceTimeToUnixtime(oldDateStr, oldTimeStr)
                    except OverflowError:
                        t=0
                    # print(marketName, starName, stationId, oldDateStr, oldTimeStr, t)
                    if fetchOlder:
                        t=0
                    jsonData["knownMarkets"].append({"id":stationId, "t":t})
                # else:
                    # if verbose and not fromTce:
                        # print("Skipping market because of command line params:", marketName, starName, stationId)
                    
            elif marketName != EMPTY_MAGIC:
                if not fromTce:
                    print(marketName, starName, stationId, "ID not found!")
        # else:
            # if verbose and not fromTce:
                # print("Skipping market because of --local-id command line params:", localMarketId)
        
        # if len(jsonData["knownMarkets"]) > 50:
            # break
        # break
        
        
    t2 = timeit.default_timer()
    if not fromTce:
        print ("Requesting data for",len(jsonData["knownMarkets"]),"markets")
        if len(jsonData["knownMarkets"]) > 2500:
            print ("-------------------------------------------------------------------------------")
            print ("WARNING: You've requested A LOT of markets!")
            print ("         The server will probably not answer them all for performance reasons.")
            print ("         And TCE is also pretty slow now...")
            print ("         I'd recommend not using more than 1000 markets in TCE.")
            print ("-------------------------------------------------------------------------------")
            
        print ("getJsonRequestForPrices took",(t2-t1),"seconds")

    return jsonData


def sendRequestForPrices(jsonData):
    return sendRequestDefault(jsonData, tceRelayUrlPrices)

def processJsonResponseForPrices(jsonResponse):
    showStatus("Processing response")
    t1 = timeit.default_timer()
    if verbose:
        print(jsonResponse)

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
        print ("processJsonResponseForPrices took",(t2-t1),"seconds")
        print ("Updated",countStationsUpdated,"stations with",countPricesUpdated,"prices")
    else:
        #text="Updated "+strcountStationsUpdated+" stations with "+countPricesUpdated+"prices"
        #showStatus(text)
        showStatus("Finished")

def getJsonRequestForStars():
    global connStars

    MAX_SYSTEM_ID = 88421

    t1 = timeit.default_timer()
    c = connStars.cursor()
    c.execute("SELECT id, Class FROM Public_Stars ORDER BY id")

    jsonData = getJsonRequestDefault()

    count = 0
    countRequested = 0
    stars = c.fetchall()

    prevId = 0

    reqMask = ""

    for star in stars:
        if star["ID"] > MAX_SYSTEM_ID:
            break
        count += 1
        if fromTce and count % 1000 == 0:
            showProgress(count, len(stars), "Preparing request")

        if star["Class"] != None:
            starChar = "0"
        else:
            countRequested += 1
            starChar = "1"

        reqMask += (star["ID"] - prevId) * starChar

        prevId = star["ID"]

    jsonData["reqMask"] = reqMask

    t2 = timeit.default_timer()
    if not fromTce:
        print ("Requesting data for",countRequested,"stars")
        print ("getJsonRequestForStars took",(t2-t1),"seconds")
    return jsonData

def sendRequestForStars(jsonData):
    return sendRequestDefault(jsonData, tceRelayUrlStars)

def processJsonResponseForStars(jsonResponse):
    showStatus("Processing response")
    t1 = timeit.default_timer()
    if verbose:
        print(jsonResponse)

    if "error" in jsonResponse:
        showError(jsonResponse["error"])
        exit(4)
    if not fromTce:
        print("ServerProcessTime", jsonResponse["processTime"])
    starData=jsonResponse["starData"]

    if not fromTce:
        print("Got updated spectral classes for",len(starData),"stars")
    
    countStars = 0
    for starId in starData:
        countStars += 1
        if fromTce:
            showProgress(countStars, len(starData), "Updating stars")
        updateStarClass(starId, starData[starId])

    t2 = timeit.default_timer()
    if not fromTce:
        print ("processJsonResponseForStars took",(t2-t1),"seconds")
        print ("Updated spectral class of",countStars,"stars")
    else:
        #text="Updated "+strcountStationsUpdated+" stations with "+countPricesUpdated+"prices"
        #showStatus(text)
        showStatus("Finished")

def updateStarClass(starId, starClass):
    global connStars
    c = connStars.cursor()
    if not args.dryRun:
        c.execute("UPDATE public_Stars SET Class=? WHERE ID=?", (starClass, starId))

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
    if not args.dryRun:
        c.execute("DELETE FROM public_MarketPrices WHERE MarketID=?", (localMarketId, ))

def parseTceTimeToUnixtime(dateInteger, timeString):
    if verbose:
        print ("Parsing TCE date: ", dateInteger, timeString)
    if dateInteger == 0:
        return 0
    ret=None
    try:
        ret=datetime.strptime(timeString, "%X")
    except ValueError:
        # Try parsing as HH:MM:SS
        try:
            ret=datetime.strptime(timeString, "%H:%M:%S")
        except ValueError:
            # Try parsing as HH:MM:SS AM/PM
            try:
                if timeString.find("AM") >= 0:
                    ret=datetime.strptime(timeString, "%H:%M:%S AM")
                elif timeString.find("PM") >= 0:
                    ret=datetime.strptime(timeString, "%H:%M:%S PM")+timedelta(hours=12)
            except ValueError:
                # Giving up
                pass
    if ret == None:
        # Use default
        ret=datetime(1900,1,1)
    # Some date magic here :)
    ret = ret + timedelta(days=dateInteger-469703)
    if verbose:
        print ("Parsed:", ret)
    unixtime = int(ret.replace(tzinfo=timezone.utc).timestamp())
    if verbose:
        print ("Unix time", unixtime)

    return unixtime

def parseUnixtimeToTceTime(unixtime):
    # Magic date calculation :)
    if verbose:
        print ("Parsing unixtime to TCE:", unixtime)
    collectedDate = datetime.utcfromtimestamp(unixtime)
    tceBase = collectedDate - datetime(613, 12, 31)
    if verbose:
        print ("tceBase", tceBase)
    newTceDate = str(int(tceBase/timedelta(days=1)))
    if verbose:
        print ("tceDate", newTceDate)
    newTceTime = collectedDate.strftime("%X")
    if verbose:
        print ("tceTime", newTceTime)

    return (newTceDate, newTceTime)
    
def setLocalMarketLastDate(localMarketId, collectedAt):
    global connUserMarkets
    c = connUserMarkets.cursor()
    if verbose and not fromTce:
        print ("Updating LastDate for localMarketId", localMarketId, "to", collectedAt)
    newTceDate, newTceTime = parseUnixtimeToTceTime(collectedAt)
    if not args.dryRun:
        c.execute("UPDATE public_Markets set LastDate=?, LastTime=? WHERE id=?", (newTceDate, newTceTime, localMarketId))
    
# Update a single price
def addTceSinglePrice(localMarketId, tradegoodId, supply, buyPrice, sellPrice):
    global connPrices
    global connUserMarkets
    c = connPrices.cursor()
    if tradegoodId <= getMaxTradegoodId():
        if not args.dryRun:
            c.execute("INSERT INTO public_MarketPrices ("
                "MarketID, GoodID, Buy, Sell, Stock) "
                "VALUES (?, ?, ?, ?, ?)",
                (localMarketId, tradegoodId, buyPrice, sellPrice, supply))
        return True
    else:
        print ("Did not add price to DB because tradegoodId is out of range", tradegoodId)
        return False
    # print("Updating price", localMarketId, tradegoodId, supply, buyPrice, sellPrice, collectedAt)
    #print ("Local market ID", localMarketId)

def addMarkets(list):
    for marketFullName in list:
        marketName, systemName = marketFullName.split("@")
        marketName = marketName.upper()
        systemName = systemName.upper()
        if getUserMarketId(systemName, marketName) < 0:
            print ("Adding market", marketName.upper(), systemName.upper())
            defaultMarket = getDefaultMarket(systemName, marketName)
            if defaultMarket != None:
                newId = addUserMarket(defaultMarket)
                if newId > 0:
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
    c.execute("SELECT * from public_Markets_UR WHERE StarID=? AND DistanceStar<=?"+planetarySql+" ORDER BY DistanceStar", (systemId, maxStarDistance))
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
    countAdded=0
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
            count=0
            for nearMarket in nearMarkets:
                marketName = nearMarket["MarketName"]
                systemName = nearMarket["StarName"]
                if getUserMarketId(systemName, marketName) < 0:
                    count += 1
                    print (count, "Adding market", marketName, systemName)
                    newId = addUserMarket(nearMarket)
                    if newId > 0:
                        countAdded += 1
                        stationId = getStationId(marketName, systemName, newId)
                        updateById.append(stationId)
        else:
            print ("Star not found:", baseSystemName)
    print ("Added", countAdded, "Markets to TCE DB")

def listMarketsBySystenName(list):
    for starName in list:
        starName = starName.upper()
        star = getStarByName(starName)
        if star == None:
            print ("Star not found in TCE DB:", starName)
        else:
            starId = star["ID"]
            print ("Found star in TCE DB:", starName, "-> systemId:", starId)
            marketsInSystem = getMarketsForSystem(starId, 1000000, True)
            for market in marketsInSystem:
                marketName = market["MarketName"]
                stationId=getStationId(marketName, starName)
                print ("Found market in TCE DB (UMarkets): {}, DistanceFromStar={}, type={}".format(marketName, market["DistanceStar"], market["type"]))
                localMarketId = getUserMarketId(starName, marketName)
                if localMarketId > 0:
                    print ("    localMarketId: {}, eddbStationId: {}".format(localMarketId, stationId))

def clearPrices(localMarketId = -1):
    global connPrices
    global connUserMarkets
    cP = connPrices.cursor()
    cUM = connUserMarkets.cursor()
    if localMarketId <= 0:
        print("Removing all prices and setting LastDate=0 for all markets")
        if not args.dryRun:
            cP.execute("DELETE FROM Public_MarketPrices")
            cUM.execute("UPDATE Public_Markets set LastDate=0")
    else:
        if not args.dryRun:
            cP.execute("DELETE FROM Public_MarketPrices where MarketID = ?", (localMarketId, ))
            cUM.execute("UPDATE Public_Markets set LastDate=0 where ID = ?", (localMarketId, ))

def removeDuplicates():
    global connUserMarkets
    c = connUserMarkets.cursor()
    list = []
    # Check same StarId and MarketName
    c.execute("SELECT ID, MarketName, StarID from Public_Markets ORDER BY StarID, MarketName")
    result = c.fetchall()
    prevStarId = 0
    prevMarketName = None
    for market in result:
        if market["StarID"] == 0:
            continue
        if market["MarketName"] == prevMarketName and market["StarID"] == prevStarId:
            list.append(market)
        prevMarketName = market["MarketName"]
        prevStarId = market["StarID"]
    print("Found", len(list), "duplicate markets")
    for market in list:
        deleteUserMarket(market["ID"])

def removeProblematicMarkets():
    global connUserMarkets
    c = connUserMarkets.cursor()
    list = []
    # Check same StarId and MarketName
    c.execute("SELECT ID, StarID, AllegianceID, MarketType from Public_Markets")
    result = c.fetchall()
    for market in result:
        if market["StarID"] == 0:
            continue
        if market["AllegianceID"] <= 0:
            list.append(market)
        elif market["MarketType"] <= 0:
            list.append(market)
    print("Found", len(list), "other market problems")
    for market in list:
        deleteUserMarket(market["ID"])

def removeProblematicPrices():
    global connPrices
    global connUserMarkets
    c = connPrices.cursor()
    cUM = connUserMarkets.cursor()
    c.execute("SELECT MarketID from Public_MarketPrices GROUP BY MarketID")
    result = c.fetchall()
    list = []
    for price in result:
        localMarketId = price["MarketID"]
        cUM.execute("SELECT ID from Public_Markets WHERE ID=?", (localMarketId,))
        result = cUM.fetchone()
        if result == None:
            list.append(localMarketId)
    print("Found", len(list), "price problems")
    for market in list:
        clearPrices(market)

def deleteUserMarket(localMarketId):
    global connUserMarkets
    c = connUserMarkets.cursor()
    print("Removing market", localMarketId)
    if not args.dryRun:
        c.execute("UPDATE Public_Markets SET MarketName = '"+EMPTY_MAGIC+"', StarID = 0, StarName = '', SectorID = 0, "
                + "AllegianceID = 0, PriEconomy = 0, SecEconomy = 0, DistanceStar = 0, LastDate = 0, LastTime = '00:00:00', MarketType = 0, "
                + "Refuel = 0, Repair = 0, Rearm = 0, Outfitting = 0, Shipyard = 0, Blackmarket = 0, Hangar = 0, RareID = 0, ShipyardID = 0, "
                + "Notes = '', PosX = 0, PosY = 0, PosZ = 0 WHERE ID = ?", (localMarketId, ))
        clearPrices(localMarketId)

t1 = timeit.default_timer()

if verbose:
    print ("Client GUID is", getGuid())
    
# ut1=parseTceTimeToUnixtime(512309, "10:00:00")
# ut2=parseTceTimeToUnixtime(512309, "10:00:00 AM")
# ut3=parseTceTimeToUnixtime(512309, "10:00:00 PM")

# td1,tt1=parseUnixtimeToTceTime(ut1)
# td2,tt2=parseUnixtimeToTceTime(ut2)
# td3,tt3=parseUnixtimeToTceTime(ut3)
# exit(1)

if args.iKnowTheRisks:
    print ("==========================================================================")
    print ("Enabling experimental features. I hope you made a backup first. Take care.")
    print ("==========================================================================")

if args.dryRun:
    print ("==========================================================================")
    print ("Running in DRY RUN mode, changes will not be written to the database.     ")
    print ("==========================================================================")

if args.clearPrices:
    if not args.iKnowTheRisks:
        print("Error: --clear-prices is EXPERIMENTAL, please set --i-know-the-risks if you really do")
        exit(10)
    else:
        clearPrices()

if args.removeProblematic:
    if not args.iKnowTheRisks:
        print("Error: --remove-problematic is EXPERIMENTAL, please set --i-know-the-risks if you really do")
        exit(10)
    else:
        removeDuplicates()
        removeProblematicMarkets()
        removeProblematicPrices()

if addMarketsNearSystemList != None and len(addMarketsNearSystemList) > 0:
    if not args.iKnowTheRisks:
        print("Error: --add-markets-near-system is EXPERIMENTAL, please set --i-know-the-risks if you really do")
        exit(10)
    else:
        updateById = []
        addMarketsNearSystem(addMarketsNearSystemList)
elif addMarketList != None and len(addMarketList) > 0:
    if not args.iKnowTheRisks:
        print("Error: --add-market is EXPERIMENTAL, please set --i-know-the-risks if you really do")
        exit(10)
    else:
        updateById = []
        addMarkets(addMarketList)

if args.listMarketsBySystenName != None:
    args.offlineMode = True
    listMarketsBySystenName(args.listMarketsBySystenName)
    
if not args.offlineMode:
    try:
        jsonData = getJsonRequestForPrices()
    except:
        print(traceback.format_exc())
        showError("Unable to create request!")
        exit(1)

    try:
        jsonResponse = sendRequestForPrices(jsonData)
    except:
        print(traceback.format_exc())
        showError("Server unreachable!")
        exit(2)

    try:
        processJsonResponseForPrices(jsonResponse)
    except:
        print(traceback.format_exc())
        showError("Unable to parse response!")
        exit(3)

    try:
        jsonData = getJsonRequestForStars()
    except:
        print(traceback.format_exc())
        showError("Unable to create request!")
        exit(1)

    try:
        jsonResponse = sendRequestForStars(jsonData)
    except:
        print(traceback.format_exc())
        showError("Server unreachable!")
        exit(2)

    try:
        processJsonResponseForStars(jsonResponse)
    except:
        print(traceback.format_exc())
        showError("Unable to parse response!")
        exit(3)

connUserMarkets.commit()
connPrices.commit()
connStars.commit()
connDefaultMarkets.commit()

# Close DB connections
connUserMarkets.close()
connDefaultMarkets.close()
connPrices.close()
connTceRelayClient.close()
connTceRelayClientLocal.close()
connStars.close()
connResources.close()

t2 = timeit.default_timer()
if not fromTce:
    print ("Total runtime:",(t2-t1),"seconds")