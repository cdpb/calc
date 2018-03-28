#!/usr/bin/env python3

import argparse
import geopy.distance
import googlemaps
import logging
from os import getenv
import pymysql
import random
import string

maxmeter = 1000000
wpmapid = 1
global db
global dbcursor
global logger

def init_logging():
    global logger
    logger = logging.getLogger('calc')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def init_dbconnection():
    global db
    global dbcursor
    dbhost = getenv('MYSQL_HOST')
    dbuser = getenv('MYSQL_USER')
    dbpasswd = getenv('MYSQL_PASSWORD')
    dbname = getenv('MYSQL_DATABASE')
    dbport = getenv('MYSQL_PORT')

    try:
        db = pymysql.connect(
            host=dbhost,
            user=dbuser,
            passwd=dbpasswd,
            port=dbport,
            db=dbname
        )
    except Exception:
        logger.error("could not connect to %s@%s on %s"
                     % (dbuser, dbhost, dbname))
        exit(98)

    dbcursor = db.cursor()

def setup_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pair', type=int, help='calculate just a single pair')
    parser.add_argument('--method', type=str, help='prefered method')
    parser.add_argument('--opt', type=str, help='set mode "driving, transit"')
    parser.add_argument('--debug', type=str, help='increase verbose, alot')
    return parser.parse_args()

# default is to fetch all from sql
def sql(query, type="all"):
    global dbcursor
    dbcursor.execute(query)
    if type is "all":
        data = dbcursor.fetchall()
    elif type is "one":
        data = dbcursor.fetchone()
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
        ident_wpcalc = data_wpcalc[pairindex][1]
init_logging()
init_dbconnection()

        if ident_wpcalc == ident_wpmaps:
            skipped += 1
            logger.debug("skip - pair: %s - %s "
                         % (pairindex, description_wpcalc))
        else:
            dfrom = data[2]
            dto = data[3]
            for method in (try_calculate_gmaps, try_calculate_beeline):
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

                    sql('''INSERT INTO wordpress.wp_cdpb_calc(id, description, distance, ident, method)
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
    logger.info("total %i km - skipped: %i - calculated: %i "
                % (distance_total[0][0], skipped, calculated))
db.close()
args = setup_argparse()
if args.pair:
    single_calculation(pair=args.pair, pref_method=args.method, opt=args.opt)
else:
    default_calculation(pref_method=args.method)

try:
    if db.open:
        db.close()
except NameError:
    exit()
