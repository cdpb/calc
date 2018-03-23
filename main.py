#!/usr/bin/env python3

import geopy.distance
import googlemaps
import logging
from os import getenv
import pymysql
import random
import string

dbhost = getenv('MYSQL_HOST')
dbuser = getenv('MYSQL_USER')
dbpasswd = getenv('MYSQL_PASSWORD')
dbname = getenv('MYSQL_DATABASE')
dbport = getenv('MYSQL_PORT')
maxmeter = 1000000
wpmapid = 1

logger = logging.getLogger('calc')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

try:
    db = pymysql.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpasswd,
        port=dbport,
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
    return data

def generate_description(a, b, delim="to"):
    combine = "%s %s %s" % (a, delim, b)
    return combine

def generate_uniq_ident(data, chars=string.ascii_uppercase + string.digits):
    random.seed(data)
    return ''.join(random.choice(chars) for _ in range(6))

# generate list of data pair
# output data(description, ident, dfrom, dto)
def generate_data_pair(data):
    finaldata = []
    pairone = None
    pairtwo = None

    for index, item in enumerate(data, start=0):
        try:
            nextitem = data[index+1]
        except IndexError:
            return finaldata
        else:
            dfrom_description = item[0]
            dfrom = item[1] + ", " + item[2]
            dto_description = nextitem[0]
            dto = nextitem[1] + ", " + nextitem[2]
            description = generate_description(dfrom_description,
                                               dto_description)
            ident = generate_uniq_ident(description)
            finaldata.append([description, ident, dfrom, dto])

# try call google maps api
def try_calculate_gmaps(dfrom, dto, mode="driving"):
    gmaps = googlemaps.Client(getenv('GOOGLE_APIKEY'))
    rawdata = gmaps.directions(dfrom, dto, mode=mode)
    distance = rawdata[0]["legs"][0]["distance"]["value"]

    if distance > maxmeter:
        logger.error("distance from %s to %s over %i, try something else"
                     % (dfrom, dto, maxmeter))
        method = None

    if isinstance(distance, int):
        method = "gmaps-%s" % (mode)
    else:
        method = None
    data = (distance, method)
    return data

# try direct way
def try_calculate_beeline(dfrom, dto):
    distance_raw = geopy.distance.vincenty(dfrom, dto).m
    distance = round(distance_raw)
    if isinstance(distance, int):
        method = "beeline"
    else:
        method = None
    data = (distance, method)
    return data


data_wpmaps_raw = sql("SELECT address,lat,lng FROM wordpress.wp_wpgmza WHERE map_id = %i"
                      % (wpmapid))

data_wpcalc = sql("SELECT description,ident,method FROM wordpress.wp_cdpb_calc")
data_wpmaps = generate_data_pair(data_wpmaps_raw)

if data_wpmaps and data_wpcalc:
    skipped = 0
    calculated = 0
    for pairindex, data in enumerate(data_wpmaps):
        description_wpmaps = data[0]
        ident_wpmaps = data[1]
        description_wpcalc = data_wpcalc[pairindex][0]
        ident_wpcalc = data_wpcalc[pairindex][0]

        if ident_wpcalc == ident_wpmaps:
            skipped += 1
            logger.debug("skip - pair: %i - %s "
                         % (ident_wpcalc, description_wpcalc))
        else:
            dfrom = data[2]
            dto = data[3]
            for method in (try_calculate_beeline, try_calculate_gmaps):
                calculate_method_result = method(dfrom, dto)
                distance = calculate_method_result[0]
                method = calculate_method_result[1]
                if method is None:
                    print(calculate_method_result)
                    logger.info("method %s failed, pair: %s - %s"
                                % (method, pairindex, description_wpmaps))
                    continue
                else:
                    logger.info("method %s calculated %i, pair: %s - %s"
                                % (method, distance, pairindex, description_wpmaps))

                    print('''INSERT INTO wordpress.wp_cdpb_calc(id, description, distance, ident, method)
                        VALUES (%(index)i, "%(desc)s",
                                %(distance)i, "%(ident)s", "%(method)s")
                        ON DUPLICATE KEY
                        UPDATE  id=%(index)i, description="%(desc)s",
                                ident="%(ident)s", distance=%(distance)i,
                                method="%(method)s";'''
                        % {"index": pairindex, "desc": description_wpmaps,
                           "distance": distance, "ident": ident_wpmaps,
                           "method": method})
                    db.commit()
                    calculated += 1
                    break

    distance_total = sql("""SELECT ROUND(SUM(distance)/1000) as count
                         FROM wp_cdpb_calc where skipcalculate is not true""")
    logger.info("%i m - skipped: %i - calculated: %i "
                % (distance_total[0][0], skipped, calculated))
db.close()
