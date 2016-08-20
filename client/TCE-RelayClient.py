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

tceRelayVersion = "0.1"

parser = argparse.ArgumentParser(description='TCE-Relay Client for Elite Dangerous')

parser.add_argument('--from-tce', dest='fromTce', action='store_const',
					const=True, default=False, help='Set by TCE Launcher to get clean output')
parser.add_argument('--max-age', dest='maxAge', type=int, action='store',
					default=14, help='Max age for the prices in days (defaults to 14)')
parser.add_argument('--tce-path', dest='tcePath', action='store',
					default="c:/TCE", help='Path to TCE (defaults to c:/TCE)')
					
args = parser.parse_args()

maxAge = args.maxAge
tcePath = args.tcePath
fromTce = args.fromTce

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

# These are global
commodities = json.load(open('c:/temp/commodities.json'))

# These too
connUserMarkets = sqlite3.connect(tcePath+"/db/TCE_RMarkets.db")
connPrices = sqlite3.connect(tcePath+"/db/TCE_Prices.db")
connResources = sqlite3.connect(tcePath+"/db/Resources.db")
connTceRelayClient = sqlite3.connect(getMyPath("TCE-RelayClient.db"))

connUserMarkets.row_factory = sqlite3.Row
connPrices.row_factory = sqlite3.Row
connResources.row_factory = sqlite3.Row
connTceRelayClient.row_factory = sqlite3.Row

# These too, our caches
commodityIdToTceCache = {}
commodityCache = {}
localMarketIdCache = {}
stationIdCache = {}
	
def showProgress(curProgress, maxProgress, text="Progress"):
	print ("PROGRESS:"+str(curProgress)+","+str(maxProgress)+","+text)

def showStatus(text):
	print ("STATUS:"+text)
	
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

def getLocalMarketId(stationId):
	return localMarketIdCache[int(stationId)]
	
def getStationId(marketName, starName, marketId):
	global connTceRelayClient
	
	try:
		val = stationIdCache[int(marketId)]
	except KeyError:
		c = connTceRelayClient.cursor()
	
		c.execute("SELECT stationId FROM stationIdMappings WHERE stationName=? AND systemName=?", (marketName, starName))
		result = c.fetchone()
		if (result != None):
			val = result["stationId"]
		else:
			val = -1
		stationIdCache[int(marketId)] = val
		localMarketIdCache[val] = int(marketId)
	return val

def getJsonRequest():
	showStatus("Preparing request")
	global connUserMarkets
	global maxAge
	
	t1 = timeit.default_timer()

	cUM = connUserMarkets.cursor()

	cUM.execute("SELECT * FROM public_Markets")

	jsonData = {}
	jsonData["apiVersion"] = 1
	jsonData["clientVersion"] = tceRelayVersion
	jsonData["knownMarkets"] = []
	jsonData["maxAge"] = maxAge

	count = 0
	markets = cUM.fetchall()
	for market in markets:
		count += 1
		if fromTce: # and count % 10 == 0:
			showProgress(count, len(markets), "Preparing request")
		localMarketId=market["ID"]
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
			jsonData["knownMarkets"].append({"id":stationId, "t":t})
		else:
			if not fromTce:
				print(marketName, starName, stationId, "ID not found!!!!!!!!")
	
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
		commodityId = curPrice["commodityId"]
		supply = curPrice["supply"]
		buyPrice = curPrice["buyPrice"]
		sellPrice = curPrice["sellPrice"]
		collectedAt = curPrice["collectedAt"]
		success = addTceSinglePrice(localMarketId, commodityId, supply, buyPrice, sellPrice)
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
	if not fromTce:
		print ("Updating LastDate for localMarketId", localMarketId, "to", collectedAt)
	# Magic date calculation :)
	collectedDate = datetime.utcfromtimestamp(collectedAt)
	tceBase = collectedDate - datetime(613, 12, 31)
	newTceDate = str(int(tceBase/timedelta(days=1)))
	newTceTime = collectedDate.strftime("%H:%M:%S")
	c.execute("UPDATE public_Markets set LastDate=?, LastTime=? WHERE id=?", (newTceDate, newTceTime, localMarketId))
	
# Update a single price
def addTceSinglePrice(localMarketId, commodityId, supply, buyPrice, sellPrice):
	global connPrices
	global connUserMarkets
	c = connPrices.cursor()
	tradegoodId = translateCommodityIdToTCETradegoodId(commodityId)
	if tradegoodId < 0:
		if not fromTce:
			print ("Unable to map commodityId", commodityId, getCommodityNameFromId(commodityId))
		return False
	c.execute("INSERT INTO public_MarketPrices ("
		"MarketID, GoodID, Buy, Sell, Stock) "
		"VALUES (?, ?, ?, ?, ?)",
		(localMarketId, tradegoodId, buyPrice, sellPrice, supply))
	return True
	# print("Updating price", localMarketId, tradegoodId, supply, buyPrice, sellPrice, collectedAt)
	#print ("Local market ID", localMarketId)
			
t1 = timeit.default_timer()

jsonData = getJsonRequest()
jsonResponse = sendRequest(jsonData)
processJsonResponse(jsonResponse)

connUserMarkets.commit()
connPrices.commit()

t2 = timeit.default_timer()
if not fromTce:
	print ("Total runtime:",(t2-t1),"seconds")