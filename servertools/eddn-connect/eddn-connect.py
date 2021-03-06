#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import zlib
import zmq
import simplejson
import sys, os, datetime, time
import config
import peewee
import sqlite3
from peewee import *
import re
import dateutil.parser
import calendar

""" 
Based on https://github.com/jamesremuscat/EDDN
"""

scriptDir = os.path.dirname(os.path.realpath(__file__))


"""
 "  Configuration
"""
__relayEDDN             = 'tcp://eddn-relay.elite-markets.net:9500'
__timeoutEDDN           = 600000 # 10 minuts
#__timeoutEDDN           = 60000 # 1 minut

# Set False to listen to production stream; True to listen to debug stream
__debugEDDN             = False;

# Set to False if you do not want verbose logging
#__logVerboseFile        = scriptDir + '/Logs_Verbose_EDDN_%DATE%.htm'
__logVerboseFile        = False

# Set to False if you do not want JSON logging
#__logJSONFile           = scriptDir + '/Logs_JSON_EDDN_%DATE%.log'
__logJSONFile           = False

# A sample list of authorised softwares
__authorisedSoftwares   = [
    "EDCE",
    "ED-TD.SPACE",
    "EliteOCR",
    "Maddavo's Market Share",
    "RegulatedNoise",
    "RegulatedNoise__DJ",
    "E:D Market Connector [Windows]"
]

# Used this to excludes yourself for example has you don't want to handle your own messages ^^
__excludedSoftwares     = [
    'My Awesome Market Uploader'
]

# Auth ALL software except excluded
__authorisedByDefault = True

__verbose = False

__fixedNames = {
    ## From EDMC/companion.py
    'Agricultural Medicines'             : 'Agri-Medicines',
    'Ai Relics'                          : 'AI Relics',
    'Animalmeat'                         : 'Animal Meat',
    'Atmospheric Extractors'             : 'Atmospheric Processors',
    'Auto Fabricators'                   : 'Auto-Fabricators',
    'Basic Narcotics'                    : 'Narcotics',
    'Bio Reducing Lichen'                : 'Bioreducing Lichen',
    'C M M Composite'                    : 'CMM Composite',
    'Comercial Samples'                  : 'Commercial Samples',
    'Diagnostic Sensor'                  : 'Hardware Diagnostic Sensor',
    'Drones'                             : 'Limpet',
    'Encripted Data Storage'             : 'Encrypted Data Storage',
    'H N Shock Mount'                    : 'HN Shock Mount',
    'Hafnium178'                         : 'Hafnium 178',
    'Hazardous Environment Suits'        : 'H.E. Suits',
    'Heliostatic Furnaces'               : 'Microbial Furnaces',
    'Low Temperature Diamond'            : 'Low Temperature Diamonds',
    'Marine Supplies'                    : 'Marine Equipment',
    'Meta Alloys'                        : 'Meta-Alloys',
    'Methanol Monohydrate Crystals'      : 'Methanol Monohydrate',
    'Mu Tom Imager'                      : 'Muon Imager',
    'Non Lethal Weapons'                 : 'Non-Lethal Weapons',
    'Power Grid Assembly'                : 'Energy Grid Assembly',
    'Power Transfer Conduits'            : 'Power Transfer Bus',
    'S A P8 Core Container'              : 'SAP 8 Core Container',	# Not seen in E:D 1.4 or later?
    'Skimer Components'                  : 'Skimmer Components',
    'Terrain Enrichment Systems'         : 'Land Enrichment Systems',
    'Trinkets Of Fortune'                : 'Trinkets Of Hidden Fortune',
    'Unknown Artifact'                   : 'Unknown Artefact',
    'Unknown Artifact2'                  : 'Unknown Probe',	# untested
    'U S S Cargo Ancient Artefact'       : 'Ancient Artefact',
    'U S S Cargo Experimental Chemicals' : 'Experimental Chemicals',
    'U S S Cargo Military Plans'         : 'Military Plans',
    'U S S Cargo Prototype Tech'         : 'Prototype Tech',
    'U S S Cargo Rebel Transmissions'    : 'Rebel Transmissions',
    'U S S Cargo Technical Blueprints'   : 'Technical Blueprints',
    'U S S Cargo Trade Data'             : 'Trade Data',
    'Wreckage Components'                : 'Salvageable Wreckage',
    }


db = MySQLDatabase(config.mysql["db"], user=config.mysql["user"], passwd=config.mysql["pw"])

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

connResources = sqlite3.connect(scriptDir+"/Resources.db")
connResources.row_factory = sqlite3.Row

connTceRelayClient = sqlite3.connect(scriptDir+"/TCE-RelayClient.db")
connTceRelayClient.row_factory = sqlite3.Row

stationIdCache = {}
tradegoodCache = {}
missingTradegoodMappings = {}

def getStationId(starName, marketName):
    global connTceRelayClient
    key=starName+"###"+marketName
    try:
        val = stationIdCache[key]
    except KeyError:
        c = connTceRelayClient.cursor()
    
        c.execute("SELECT stationId FROM stationIdMappings WHERE stationName=? AND systemName=?", (marketName.upper(), starName.upper()))
        result = c.fetchone()

        if result != None:
            val = result["stationId"]
        else:
            val = -1
        
        stationIdCache[key] = val
    return val

def getTceTradegoodId(tradegoodName):
    global connResources
    val = -1;
    try:
        val=tradegoodCache[tradegoodName];
    except KeyError:
        c = connResources.cursor()
        c.execute("SELECT id from public_Goods WHERE Tradegood LIKE ?", (tradegoodName, ))
        result = c.fetchone()
        if (result != None):
            val=result["id"]
            tradegoodCache[tradegoodName]=val
    if val < 0:
        addMissingTradegoodMapping(tradegoodName)
    return val;

def addMissingTradegoodMapping(name):
    global missingTradegoodMappings
    try:
        val=missingTradegoodMappings[name]
        val += 1
    except KeyError:
        val = 1
    missingTradegoodMappings[name] = val


"""
 "  Start
"""
def date(__format):
    d = datetime.datetime.utcnow()
    return d.strftime(__format)


__oldTime = False
def echoLog(__str):
    global __oldTime, __logVerboseFile
    
    if __logVerboseFile != False:
        __logVerboseFileParsed = __logVerboseFile.replace('%DATE%', str(date('%Y-%m-%d')))
    
    if __logVerboseFile != False and not os.path.exists(__logVerboseFileParsed):
        f = open(__logVerboseFileParsed, 'w')
        f.write('<style type="text/css">html { white-space: pre; font-family: Courier New,Courier,Lucida Sans Typewriter,Lucida Typewriter,monospace; }</style>')
        f.close()

    try:
        if (__oldTime == False) or (__oldTime != date('%H:%M:%S')):
            __oldTime = date('%H:%M:%S')
            __str = str(__oldTime)  + ' | ' + __str
        else:
            __str = '        '  + ' | ' + __str
        
        print __str
        sys.stdout.flush()

        if __logVerboseFile != False:
            f = open(__logVerboseFileParsed, 'a')
            f.write(__str + '\n')
            f.close()
    except UnicodeEncodeError:
        echoLog("UnicodeEncodeError")
    

def echoLogJSON(__json):
    global __logJSONFile
    
    if __logJSONFile != False:
        __logJSONFileParsed = __logJSONFile.replace('%DATE%', str(date('%Y-%m-%d')))
        
        f = open(__logJSONFileParsed, 'a')
        f.write(str(__json) + '\n')
        f.close()

def getFixedName(commodityName):
    global __fixedNames
    try:
        return __fixedNames[commodityName]
    except KeyError:
        return commodityName

def main():
    echoLog('Starting EDDN Subscriber')
    echoLog('')
    
    context     = zmq.Context()
    subscriber  = context.socket(zmq.SUB)
    
    subscriber.setsockopt(zmq.SUBSCRIBE, "")
    subscriber.setsockopt(zmq.RCVTIMEO, __timeoutEDDN)

    while True:
        try:
            subscriber.connect(__relayEDDN)
            echoLog('Connect to ' + __relayEDDN)
            echoLog('')
            echoLog('')
            
            poller = zmq.Poller()
            poller.register(subscriber, zmq.POLLIN)
 
            while True:
                socks = dict(poller.poll(__timeoutEDDN))
                if socks:
                    if socks.get(subscriber) == zmq.POLLIN:
                        __message   = subscriber.recv(zmq.NOBLOCK)
                        __message   = zlib.decompress(__message)
                        __json      = simplejson.loads(__message)
                        __converted = False
                        
                        
                        # Handle commodity v1
                        if __json['$schemaRef'] == 'http://schemas.elite-markets.net/eddn/commodity/1' + ('/test' if (__debugEDDN == True) else ''):
                            echoLogJSON(__message)
                            echoLog('Receiving commodity-v1 message...')
                            echoLog('    - Converting to v2...')
                            
                            __temp                              = {}
                            __temp['$schemaRef']                = 'http://schemas.elite-markets.net/eddn/commodity/2' + ('/test' if (__debugEDDN == True) else '')
                            __temp['header']                    = __json['header']
                            
                            __temp['message']                   = {}
                            __temp['message']['systemName']     = __json['message']['systemName']
                            __temp['message']['stationName']    = __json['message']['stationName']
                            __temp['message']['timestamp']      = __json['message']['timestamp']
                            
                            __temp['message']['commodities']    = []
                            
                            __commodity                         = {}
                            
                            if 'itemName' in __json['message']:
                                __commodity['name'] = __json['message']['itemName']
                            
                            if 'buyPrice' in __json['message']:
                                __commodity['buyPrice'] = __json['message']['buyPrice']
                            if 'stationStock' in __json['message']:
                                __commodity['supply'] = __json['message']['stationStock']
                            if 'supplyLevel' in __json['message']:
                                __commodity['supplyLevel'] = __json['message']['supplyLevel']
                            
                            if 'sellPrice' in __json['message']:
                                __commodity['sellPrice'] = __json['message']['sellPrice']
                            if 'demand' in __json['message']:
                                __commodity['demand'] = __json['message']['demand']
                            if'demandLevel' in __json['message']:
                                __commodity['demandLevel'] = __json['message']['demandLevel']
                            
                            __temp['message']['commodities'].append(__commodity)
                            __json                              = __temp
                            del __temp, __commodity
                            
                            __converted = True
                        

                        # Handle commodity v2
                        if __json['$schemaRef'] == 'http://schemas.elite-markets.net/eddn/commodity/2' + ('/test' if (__debugEDDN == True) else ''):
                            echoLogJSON(__message)
                            echoLog('Receiving commodity-v2 message...')
                            echoLog('    - Converting to v3...')

                            __json['$schemaRef'] = 'http://schemas.elite-markets.net/eddn/commodity/3' + ('/test' if (__debugEDDN == True) else '')

                            for __commodity in __json['message']['commodities']:
                                if 'supply' in __commodity:
                                    # Rename supply to stock
                                    __commodity["stock"] = __commodity["supply"]

                            __converted = True

                        # Handle commodity v3
                        if __json['$schemaRef'] == 'http://schemas.elite-markets.net/eddn/commodity/3' + ('/test' if (__debugEDDN == True) else ''):
                            if __converted == False:
                                echoLogJSON(__message)
                                echoLog('Receiving commodity-v3 message...')
                            
                            if __authorisedByDefault:
                                __authorised = True
                            else:
                                __authorised = False
                            __excluded   = False
                            
                            if __json['header']['softwareName'] in __authorisedSoftwares:
                                __authorised = True
                            if __json['header']['softwareName'] in __excludedSoftwares:
                                __excluded = True
                        
                            echoLog('    - Software: ' + __json['header']['softwareName'] + ' / ' + __json['header']['softwareVersion'])
#                            echoLog('        - ' + 'AUTHORISED' if (__authorised == True) else
#                                                        ('EXCLUDED' if (__excluded == True) else 'UNAUTHORISED')
#                            )
                            
                            if __authorised == True and __excluded == False:
                                # Do what you want with the data...
                                # Have fun !
                                
                                # For example
                                echoLog('    - Timestamp: ' + __json['message']['timestamp'])
                                echoLog('        - System Name: ' + __json['message']['systemName'])
                                echoLog('        - Station Name: ' + __json['message']['stationName'])
                                echoLog('        - Commodities: ' + str(len(__json['message']['commodities'])))

                                stationId = getStationId(__json['message']['systemName'], __json['message']['stationName'])

                                try:
                                    timestamp = dateutil.parser.parse(__json['message']['timestamp'])
                                    unixtime = calendar.timegm(timestamp.timetuple())
                                    if unixtime > time.time():
                                        unixtime = time.time()
                                except TypeError:
                                    unixtime = time.time()

                                echoLog('        - Station ID: ' + str(stationId))
                                try:
                                    echoLog('    - Uploader ID: ' + __json['header']['uploaderID'])
                                except:
                                    pass
                                echoLog('        - Unixtime ' + str(unixtime))
                                if stationId >= 0:
                                    with db.atomic():
                                        # Delete all old prices of this station
                                        deleteQuery = CommodityPrice.delete().where(CommodityPrice.stationId == stationId)
                                        rowsDeleted = deleteQuery.execute()
                                        echoLog('        - Deleted old prices: ' + str(rowsDeleted))
                                        for __commodity in __json['message']['commodities']:
                                            if __verbose:
                                                echoLog('            - Name: ' + __commodity['name'])
                                                echoLog('                - Buy Price: ' + str(__commodity['buyPrice']))
                                                echoLog('                - Supply: ' + str(__commodity['stock']))
                                                echoLog('                - Sell Price: ' + str(__commodity['sellPrice']))
                                                echoLog('                - Demand: ' + str(__commodity['demand']))
                                            commodityName = __commodity['name'];
                                            commodityName = getFixedName(commodityName)
                                            tradegoodId=getTceTradegoodId(commodityName)
#                                            echoLog('                - TradegoodId: ' + str(tradegoodId))
                                            if __verbose or tradegoodId < 0:
                                                echoLog('                - Name: ' + commodityName + ", " + str(tradegoodId))
                                            if tradegoodId >= 0:
                                                price, created = CommodityPrice.get_or_create(stationId=stationId, tradegoodId=tradegoodId)
                                                if __verbose:
                                                    if created:
                                                        echoLog('                --> Creating entry')
                                                    else:
                                                        echoLog('                --> Updating entry')
                                                price.supply = __commodity['stock']
                                                price.buyPrice = __commodity['buyPrice']
                                                price.sellPrice = __commodity['sellPrice']
                                                price.demand = __commodity['demand']
                                                price.collectedAt = unixtime
                                                price.save()

                            del __authorised, __excluded
                            
                            echoLog('')
#                            echoLog('')

                       # Handle commodity v3
                        elif __json['$schemaRef'] == 'http://schemas.elite-markets.net/eddn/commodity/3' + ('/test' if (__debugEDDN == True) else ''):
                            echoLog('FIXME: Received commodity v3 - not implemented')

                        else:
                            try:
                                echoLog('Received unknown message: '+__json['$schemaRef'])
                            except:
                                pass
                        
                        del __converted
                else:
                    print 'Disconnect from ' + __relayEDDN + ' (After timeout)'
                    echoLog('')
                    echoLog('')
                    sys.stdout.flush()
                    
                    subscriber.disconnect(__relayEDDN)
                    break
                
        except zmq.ZMQError, e:
            subscriber.disconnect(__relayEDDN)
            
            echoLog('')
            echoLog('Disconnect from ' + __relayEDDN + ' (After receiving ZMQError)')
            echoLog('ZMQSocketException: ' + str(e))
            echoLog('')
            
            time.sleep(10)
            
        

if __name__ == '__main__':
    main()
