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
    return "%s %s %s" % (a, delim, b)

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

# dump calculation to database
def insert_calculation_sql(index, desc, distance, ident, method, dfrom, dto):
    global db
    sql('''INSERT INTO wordpress.wp_cdpb_calc(id, description, distance, ident, method, dfrom, dto)
        VALUES (%(index)i, "%(desc)s", %(distance)i, "%(ident)s", "%(method)s", "%(dfrom)s", "%(dto)s")
        ON DUPLICATE KEY
        UPDATE id=%(index)i, description="%(desc)s", ident="%(ident)s",
        distance=%(distance)i, method="%(method)s", "%(dfrom)s", "%(dto)s";'''
        % {"index": index, "desc": desc,
           "distance": distance, "ident": ident,
           "method": method, "dfrom": dfrom, "dto": dto})
    db.commit()

def update_calculation_sql(index, distance, method):
    global db
    sql('''UPDATE wordpress.wp_cdpb_calc
        SET
            distance = %(distance)i,
            method = "%(method)s"
        WHERE id = %(index)i;'''
        % {"index": index, "distance": distance, "method": method})
    db.commit()

def connect_gapi():
    return googlemaps.Client(getenv('GOOGLE_APIKEY'))

# try call google maps api
def try_calculate_gmaps(dfrom, dto, mode):
    gmaps = connect_gapi()
    if mode is None:
        mode = "driving"
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
    logger.info("method %s calculated %i - from %s to %s"
                % (method, distance, dfrom, dto))
    return data

# try direct way
def try_calculate_beeline(dfrom, dto):
    distance_raw = geopy.distance.vincenty(dfrom, dto).m
    distance = round(distance_raw)
    if isinstance(distance, int):
        method = "beeline"
    else:
        method = None
    logger.info("method %s calculated %i - from %s to %s"
                % (method, distance, dfrom, dto))
    data = (distance, method)
    return data

# default: try gmaps first, then beeline
def try_calculate_default(dfrom, dto):
    for method in (try_calculate_gmaps, try_calculate_beeline):
        calculate_method_result = method(dfrom, dto)
        distance = calculate_method_result[0]
        method = calculate_method_result[1]
        if method is None:
            logger.info("method %s failed - from %s to %s"
                        % (method, dfrom, dto))
            continue
        else:
            return (distance, method)

# recalculate only one index
def single_calculation(pair, pref_method=None, opt=None):
    data_wpcalc_raw = sql("SELECT dfrom,dto FROM wordpress.wp_cdpb_calc WHERE id = %i"
                          % (pair))
    if data_wpcalc_raw:
        dfrom = data_wpcalc_raw[0][0]
        dto = data_wpcalc_raw[0][1]
        if pref_method is None:
            rst = try_calculate_default(dfrom, dto)
        elif pref_method == "gmaps":
            rst = try_calculate_gmaps(dfrom, dto, mode=opt)
        elif pref_method == "beeline":
            rst = try_calculate_beeline(dfrom, dto)
        else:
            logger.error("No suitable method found - got %s - exit"
                         % (pref_method))
            exit(97)

        if rst[1] == pref_method or rst[1] is not None:
            # index, distance, method
            update_calculation_sql(index=pair, distance=rst[0], method=rst[1])

# default, calculated all, search for skipped
# TODO method prefered
def default_calculation():
    data_wpmaps_raw = sql("SELECT address,lat,lng FROM wordpress.wp_wpgmza WHERE map_id = %i"
                          % (wpmapid))

    data_wpcalc = sql("SELECT description,ident,method FROM wordpress.wp_cdpb_calc")
    data_wpmaps = generate_data_pair(data=data_wpmaps_raw)

    if data_wpmaps and data_wpcalc:
        skipped = 0
        calculated = 0
        for pairindex, data in enumerate(data_wpmaps):
            description_wpmaps = data[0]
            ident_wpmaps = data[1]
            description_wpcalc = data_wpcalc[pairindex][0]
            ident_wpcalc = data_wpcalc[pairindex][1]

            if ident_wpcalc == ident_wpmaps:
                skipped += 1
                logger.debug("skip - pair: %s - %s "
                             % (pairindex, description_wpcalc))
            else:
                dfrom = data[2]
                dto = data[3]
                a = try_calculate_default(pairindex, dfrom, dto)
                distance = a[0]
                method = a[1]
                if method is not None and isinstance(distance, int):
                    insert_calculation_sql(pairindex, description_wpmaps,
                                           distance, ident_wpmaps, method,
                                           dfrom, dto)
                    calculated += 1
                    break
    logger.info("total %i km - skipped: %i - calculated: %i "
                % (get_total_distance(), skipped, calculated))

def get_total_distance():
    a = sql('''SELECT ROUND(SUM(distance)/1000) as count FROM wp_cdpb_calc
            WHERE skip is not true''')
    return a[0][0]


init_logging()
init_dbconnection()

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
