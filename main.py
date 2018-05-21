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
wpmaps_id = 1
wpcalc_table = "wordpress.wp_cdpb_calc"
wpmaps_table = "wordpress.wp_wpgmza"
wpmaps_table_poly = "wordpress.wp_wpgmza_polylines"
wpmaps_table_poly_id = 1
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
    parser.add_argument('--pair', type=int,
                        help='calculate just a single pair')
    parser.add_argument('--method', type=str,
                        help='prefered method (gmaps|beeline)')
    parser.add_argument('--opt', type=str,
                        help='set mode "driving, transit" (gmaps)')
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
            identseed = ''.join([dfrom, dto])
            description = generate_description(dfrom_description,
                                               dto_description)
            ident = generate_uniq_ident(identseed)
            finaldata.append([description, ident, dfrom, dto])


# dump calculation to database
def insert_calculation_sql(id, desc, distance, ident, method, dfrom, dto):
    global db
    sql('''INSERT INTO %(table)s (id, description, distance, ident, method, dfrom, dto)
        VALUES (%(id)i, "%(desc)s", %(distance)i, "%(ident)s", "%(method)s", "%(dfrom)s", "%(dto)s")
        ON DUPLICATE KEY
        UPDATE id=%(id)i, description="%(desc)s", ident="%(ident)s",
        distance=%(distance)i, method="%(method)s", dfrom="%(dfrom)s",
        dto="%(dto)s";'''
        % {"desc": desc, "id": id,
           "distance": distance, "ident": ident,
           "method": method, "dfrom": dfrom, "dto": dto,
           "table": wpcalc_table})
    db.commit()


def update_calculation_sql(index, distance, method):
    global db
    sql('''UPDATE %(table)s
        SET
            distance = %(distance)i,
            method = "%(method)s"
        WHERE id = %(index)i;'''
        % {"index": index, "distance": distance, "method": method,
           "table": wpcalc_table})
    db.commit()


def delete_id_sql(index):
    global db
    sql('''DELETE FROM %(table)s
        WHERE id = %(index)i;'''
        % {"index": index,
           "table": wpcalc_table})
    db.commit()


def update_id_sql(id_old, id_new, ident_new):
    global db
    sql('''UPDATE %(table)s
        SET
            id = %(new)i,
            ident = "%(ident)s"
        WHERE id = %(old)i;'''
        % {"new": id_new, "old": id_old,
           "table": wpcalc_table, "ident": ident_new
           })
    db.commit()


def polyline_sql_update(data):
    global db
    sql('''UPDATE %(table)s
        SET
            polydata = "%(data)s"
        WHERE id = %(id)i;'''
        % {"data": data, "id": wpmaps_table_poly_id,
           "table": wpmaps_table_poly
           })
    db.commit()


def connect_gapi():
    return googlemaps.Client(getenv('GOOGLE_APIKEY'))


# try call google maps api
def try_calculate_gmaps(dfrom, dto, mode="driving"):
    gmaps = connect_gapi()
    error = False
    distance = None

    if mode is None:
        mode = "driving"

    rawdata = gmaps.directions(dfrom, dto, mode=mode)
    try:
        distance = rawdata[0]["legs"][0]["distance"]["value"]
        if distance > maxmeter:
            logger.error("distance from %s to %s over %i (%s), try something else"
                         % (dfrom, dto, maxmeter, "gmaps"))
            error = True
    except IndexError:
        error = True

    if isinstance(distance, int) and not error:
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


# default: try gmaps first, then beeline
def try_calculate_default(dfrom, dto):
    for method in (try_calculate_gmaps, try_calculate_beeline):
        calculate_method_result = method(dfrom, dto)
        distance = calculate_method_result[0]
        returned_method = calculate_method_result[1]
        if returned_method is None:
            logger.info("method %s failed - from %s to %s"
                        % (method, dfrom, dto))
            continue
        else:
            return (distance, returned_method)


# recalculate only one index
def single_calculation(pair, pref_method=None, opt=None):
    data_wpcalc_raw = sql('''SELECT dfrom,dto FROM %(table)s WHERE id = %(pair)i;'''
                          % ({"pair": pair, "table": wpcalc_table}))
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
            update_calculation_sql(index=pair, distance=rst[0], method=rst[1])
        else:
            logger.error("return value not suitable, no update")


# id -> ident matcher, update id accouring to new order
# needed when record is updated or deleted
def ident_matcher(data_wpmaps_pair):
    data_wpcalc_sql = sql('''SELECT id,ident FROM %(table)s;'''
                          % ({"table": wpcalc_table}))
    for wpmaps_index, wpmaps_pair in enumerate(data_wpmaps_pair, start=1):
        identmatch = False
        wpmaps_ident = wpmaps_pair[1]

        for wpcalc_data in data_wpcalc_sql:
            wpcalc_index, wpcalc_ident = wpcalc_data

            if wpcalc_ident == wpmaps_ident:
                identmatch = True
                if wpmaps_index != wpcalc_index:
                    try:
                        update_id_sql(wpcalc_index, wpmaps_index, wpmaps_ident)
                        break
                    except pymysql.err.IntegrityError:
                        logger.error("duplicate key - %s" % (wpmaps_ident))
                        continue

        # assume reindex
        if not identmatch:
            logger.error("no matched ident - reindex on assumed index - %s - %s"
                         % (wpmaps_index, wpmaps_ident))
            update_id_sql(wpmaps_index, wpmaps_index, wpmaps_ident)

    # remove highest index if len() mismatch
    if len(data_wpcalc_sql) > len(data_wpmaps_pair):
        index = max(data_wpcalc_sql)[0]
        logger.error("remove index - %i - doesn't fit in" % (index))
        delete_id_sql(index)


# default, calculated all, search for skipped
# TODO method prefered
def default_calculation(pref_method=None):
    data_wpmaps_sql = sql('''SELECT address,lat,lng FROM %(table)s WHERE map_id = %(map)i;'''
                          % ({"map": wpmaps_id, "table": wpmaps_table}))
    data_wpcalc_sql = sql('''SELECT description,ident,method FROM %(table)s;'''
                          % ({"table": wpcalc_table}))

    data_wpmaps_pair = generate_data_pair(data=data_wpmaps_sql)
    ident_matcher(data_wpmaps_pair)

    if data_wpmaps_pair:
        skipped = 0
        calculated = 0
        for wpmaps_index, wpmaps_pair in enumerate(data_wpmaps_pair, start=1):
            ident_found = False
            description_wpmaps = wpmaps_pair[0]
            ident_wpmaps = wpmaps_pair[1]

            # search for ident, if ident exist skip pair
            for wpcalc_pair in data_wpcalc_sql:
                ident_wpcalc = wpcalc_pair[1]

                if ident_wpcalc == ident_wpmaps:
                    description_wpcalc = wpcalc_pair[0]
                    skipped += 1
                    logger.debug("skip - %s " % (description_wpcalc))
                    ident_found = True
                    break

            if ident_found:
                # skip
                continue
            else:
                dfrom = wpmaps_pair[2]
                dto = wpmaps_pair[3]
                a = try_calculate_default(dfrom, dto)
                distance = a[0]
                method = a[1]
                if method is not None and isinstance(distance, int):
                    insert_calculation_sql(wpmaps_index, description_wpmaps,
                                           distance, ident_wpmaps, method,
                                           dfrom, dto)
                    calculated += 1
                    logger.info("calculated - %s - %i with %s" % (description_wpmaps, distance, method))
        logger.info("total %i km - skipped: %i - calculated: %i "
                    % (get_total_distance(), skipped, calculated))


def polyline_getdirections():
    final = ""
    data = sql('''SELECT lat,lng FROM %(table)s;''' % ({"table": wpmaps_table}))
    for a in data:
        lat, lng = a
        final += ("(%s, %s)," % (lat, lng))

    return final[:-1]


def get_total_distance():
    a = sql('''SELECT ROUND(SUM(distance)/1000) as count FROM %(table)s
            WHERE skip is not true;''' % ({"table": wpcalc_table}))
    return a[0][0]


init_logging()
init_dbconnection()

args = setup_argparse()
if args.pair:
    single_calculation(pair=args.pair, pref_method=args.method, opt=args.opt)
else:
    default_calculation(pref_method=args.method)
    polyline_sql_update(polyline_getdirections())

try:
    if db.open:
        db.close()
except NameError:
    exit()
