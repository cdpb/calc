#!/usr/bin/env python3

import geopy.distance
import googlemaps
import logging
from os import getenv
import pymysql

dbhost = getenv('MYSQL_HOST')
dbuser = getenv('MYSQL_USER')
dbpasswd = getenv('MYSQL_PASSWORD')
dbname = getenv('MYSQL_DATABASE')
maxmeter = 1000000
wpmapid = 1

logger = logging.getLogger('calc')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('/tmp/calc.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

try:
    db = pymysql.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpasswd,
        db=dbname
    )
except Exception:
    logger.error("could not connect to %s@%s on %s" % (dbuser, dbhost, dbname))
    exit(98)

cursor = db.cursor()

# default is to fetch all from sql
def sql(query, type="all"):
    cursor.execute(query)
    if type is "all":
        data = cursor.fetchall()
    elif type is "one":
        data = cursor.fetchone()
    else:
        exit(99)

    logger.debug("SQL %s - query: %s" % (type, query))
    logger.debug("SQL %s - data: %s" % (type, data))
    return data

# generate list of data pair
def shift(data):
    finaldata = []
    for index, item in enumerate(data):
        try:
            data[index+1]
        except IndexError:
            return finaldata
        else:
            finaldata.append([item, data[index+1]])

# calculate distance, try google driving first
# if it failes try beeline, otherwise error with distance 0
def calcdistance(dfrom, dto, index):
    directions_result = gmaps.directions(dfrom, dto, mode="driving")
    try:
        method = "googlemaps-drive"
        calcdistance = directions_result[0]["legs"][0]["distance"]["value"]
        # skip if over maxmeter
        if calcdistance > maxmeter:
            logger.error("distance from %s to %s over %i, try beeline"
                         % (dfrom, dto, maxmeter))
            raise IndexError
    except IndexError:
        if not dfrom[0:2].isdigit():
            dfrom = gmaps.geocode(dfrom)
            logger.info("convert %s to geocode - %s" % ("dfrom", dfrom))
        if not dto[0:2].isdigit():
            dto = gmaps.geocode(dto)
            logger.info("convert %s to geocode - %s" % ("dto", dto))

        a = geopy.distance.vincenty(dfrom, dto).km
        if a:
            method = "beeline"
            calcdistance = round(a * 1000)
        else:
            method = "error"
            calcdistance = 0
    finally:
        logger.info("%s - index: %i calculate distance from %s to %s - output %i"
                    % (method, index, dfrom, dto, calcdistance))
        return [calcdistance, method]


directions_sql = sql("select address,lat,lng from wordpress.wp_wpgmza where map_id = %i"
                     % (wpmapid))
directions_sorted = shift(directions_sql)

if directions_sorted:
    gmaps = googlemaps.Client(getenv('GOOGLE_APIKEY'))
    skipping = 0
    calculating = 0
    index_except = 0
    for index, d in enumerate(directions_sorted):
        desc_calc = d[0][0] + " to " + d[1][0]
        dfrom = d[0][1] + ", " + d[0][2]
        dto = d[1][1] + ", " + d[1][2]

        # check if existing calculaten was made and skip
        data_existing = sql("""SELECT description,method FROM wordpress.wp_cdpb_calc WHERE id = %i"""
                            % index_except)
        try:
            index_except += 1
            if not len(data_existing) == 0:
                desc_existing = data_existing[0][0]
                method_existing = data_existing[0][1]

                if desc_existing == desc_calc and method_existing != "test":
                    skipping += 1
                    logger.debug("skip - %s == %s with method %s" % (desc_existing, desc_calc, method_existing))
                    continue
            calculating += 1
        except IndexError:
            continue

        data = calcdistance(dfrom, dto, index)
        distance = data[0]
        method = data[1]
        sql("""INSERT INTO wordpress.wp_cdpb_calc(id, description, distance, method)
            VALUES (%(index)i, "%(desc)s", %(distance)i, "%(method)s")
            ON DUPLICATE KEY
            UPDATE id=%(index)i, description="%(desc)s", distance=%(distance)i, method="%(method)s";"""
            % {"index": index, "desc": desc_calc, "distance": distance, "method": method})
        db.commit()
    distance_total = sql("""SELECT sum(distance) as sum FROM wordpress.wp_cdpb_calc""")
    print("%i m - skipping: %i - calculating: %i " % (distance_total[0][0], skipping, calculating))
db.close()
