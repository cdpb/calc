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


# setup logging, default INFO
def init_logging():
    global logger
    logger = logging.getLogger('calc')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# connect to mysql
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
            port=int(dbport),
            db=dbname
        )
    except Exception:
        logger.error("could not connect to %s@%s:%s on %s"
                     % (dbuser, dbhost, dbport, dbname))
        exit(98)

    dbcursor = db.cursor()


# set some args on cli, default is to do default_calculation() without args
def setup_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pair', type=int,
                        help='calculate just a single pair')
    parser.add_argument('--method', type=str,
                        help='prefered method (gmaps|beeline)')
    parser.add_argument('--opt', type=str,
                        help='set mode "driving, transit" (gmaps)')
    return parser.parse_args()


# fetch from sql
def sql(query):
    global dbcursor
    dbcursor.execute(query)
    return dbcursor.fetchall()


# return 'foo to bar'
def generate_description(a, b, delim="to"):
    return "%s %s %s" % (a, delim, b)


# random 6 charakter string, seeded by destination from AND to
# identseed = ''.join([dfrom, dto])
def generate_uniq_ident(data, chars=string.ascii_uppercase + string.digits):
    random.seed(data)
    return ''.join(random.choice(chars) for _ in range(6))


# generate list of data pair
# input wpmaps_data_sql(address,lat,lng) from default_calculation()
# output data(description, ident, dfrom, dto)
def generate_data_pair(data):
    finaldata = []

    for index, item in enumerate(data, start=0):
        try:
            nextitem = data[index + 1]
        except IndexError:
            return finaldata
        else:
            dfrom_description = item[0]
            dfrom = item[1] + ", " + item[2]
            dto_description = nextitem[0]
            dto = nextitem[1] + ", " + nextitem[2]

            description = generate_description(dfrom_description,
                                               dto_description)
            ident = generate_uniq_ident(''.join([dfrom, dto]))
            finaldata.append([description, ident, dfrom, dto])


# dump full calculation to database
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


# used for single calculation, just update distance and method
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


# used for ident_matcher()
def delete_id_sql(index):
    global db
    sql('''DELETE FROM %(table)s
        WHERE id = %(index)i;'''
        % {"index": index,
           "table": wpcalc_table})
    db.commit()


# used for ident_matcher()
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


# override polyline to db
def update_polyline_sql(data):
    global db
    sql('''UPDATE %(table)s
        SET
            polydata = "%(data)s"
        WHERE id = %(id)i;'''
        % {"data": data, "id": wpmaps_table_poly_id,
           "table": wpmaps_table_poly
           })
    db.commit()


# connect to gapi
def connect_gapi():
    return googlemaps.Client(getenv('GOOGLE_APIKEY'))


# try call google maps api
def try_calculate_gmaps(dfrom, dto, mode="driving"):
    gmaps = connect_gapi()
    error = False
    distance = False

    if mode is None:
        mode = "driving"

    rawdata = gmaps.directions(dfrom, dto, mode=mode)
    try:
        distance = rawdata[0]["legs"][0]["distance"]["value"]
        if distance > maxmeter:
            logger.error("distance from %s to %s over %i (%s),try other method"
                         % (dfrom, dto, maxmeter, "gmaps"))
            error = True
    except IndexError:
        error = True

    if isinstance(distance, int) and not error:
        method = "gmaps-%s" % (mode)
    else:
        method = None
    return (distance, method)


# try direct way
def try_calculate_beeline(dfrom, dto):
    distance = round(geopy.distance.geodesic(dfrom, dto).m)
    if isinstance(distance, int):
        method = "beeline"
    else:
        method = None
    data = (distance, method)
    return data


# default: try gmaps first, then beeline
def try_calculate_default(dfrom, dto):
    for method in (try_calculate_gmaps, try_calculate_beeline):
        distance, rmethod = method(dfrom, dto)
        if rmethod is None:
            logger.info("method %s failed - from %s to %s"
                        % (method, dfrom, dto))
            continue
        else:
            return (distance, rmethod)


# recalculate only one index
def single_calculation(pair, pref_method=None, opt=None):
    wpcalc_data_sql = sql('''SELECT dfrom,dto FROM %(table)s
                          WHERE id = %(pair)i;'''
                          % ({"pair": pair, "table": wpcalc_table}))
    if wpcalc_data_sql:
        dfrom, dto = wpcalc_data_sql[0]

        if pref_method is None:
            distance, method = try_calculate_default(dfrom, dto)
        elif pref_method == "gmaps":
            distance, method = try_calculate_gmaps(dfrom, dto, mode=opt)
        elif pref_method == "beeline":
            distance, method = try_calculate_beeline(dfrom, dto)
        else:
            logger.error("No suitable method found - got %s - exit"
                         % (pref_method))
            exit(97)

        if method == pref_method or method is not None:
            update_calculation_sql(pair, distance, method)
            logger.info("updated pair %i with method %s and distance %sm"
                        % (
                            pair, method, distance
                        ))
        else:
            logger.error("return value not suitable, no update - %s"
                         % (method))


# id -> ident matcher, update id accourding to new order
# needed when record is updated or deleted
# called by default_calculation()
def ident_matcher(wpmaps_pair):
    wpcalc_data_sql = sql('''SELECT id,ident FROM %(table)s;'''
                          % ({"table": wpcalc_table}))

    shiftids = []
    for wpmaps_index, wpmaps_data in enumerate(wpmaps_pair, start=1):
        wpmaps_ident = wpmaps_data[1]
        for wpcalc_index, wpcalc_ident in wpcalc_data_sql:
            if wpmaps_ident == wpcalc_ident:
                if wpmaps_index != wpcalc_index:
                    try:
                        # update id accourding to wpmaps_index
                        update_id_sql(id_old=wpcalc_index,
                                      id_new=wpmaps_index,
                                      ident_new=wpmaps_ident)
                        break
                    except pymysql.err.IntegrityError:
                        logger.info("P1 - duplicate key %s to %s - %s"
                                    % (
                                        wpcalc_index,
                                        wpmaps_index,
                                        wpmaps_ident
                                    ))
                        # multiple with random int
                        rnd = random.randint(1000, 1500)
                        wpmaps_index_tmp = wpmaps_index * rnd
                        shiftids.append([wpmaps_index,
                                         wpmaps_index_tmp,
                                         wpmaps_ident])
                        try:
                            # try again to update id, with intermediate random index
                            update_id_sql(id_old=wpcalc_index,
                                          id_new=wpmaps_index_tmp,
                                          ident_new=wpmaps_ident)
                            logger.info("P1 - shifting id from %s to %s - %s"
                                        % (
                                            wpcalc_index,
                                            wpmaps_index_tmp,
                                            wpmaps_ident
                                        ))
                            break
                        except pymysql.err.IntegrityError:
                            logger.info("P2 - failed shifting id from %s to %s - %s"
                                        % (
                                            wpcalc_index,
                                            wpmaps_index_tmp,
                                            wpmaps_ident
                                        ))
                        continue

    for shiftid in shiftids:
        wpmaps_index, wpmaps_index_tmp, wpmaps_ident = shiftid
        try:
                # shift back to original index
            update_id_sql(id_old=wpmaps_index_tmp,
                          id_new=wpmaps_index,
                          ident_new=wpmaps_ident)
            logger.info("P2 - shifting id from %s to %s - %s"
                        % (
                            wpmaps_index_tmp,
                            wpmaps_index,
                            wpmaps_ident
                        ))
        except pymysql.err.IntegrityError:
            logger.error("P3 - failed - hopeless %s to %s - %s"
                         % (
                             wpmaps_index_tmp,
                             wpmaps_index,
                             wpmaps_ident
                         ))

    # remove highest index if len() mismatch, assume deleted record
    if len(wpcalc_data_sql) > len(wpmaps_pair):
        index = max(wpcalc_data_sql)[0]
        logger.error("remove index - %i - doesn't fit in" % (index))
        delete_id_sql(index)


# default, calculated all, search for skipped
def default_calculation():
    wpmaps_data_sql = sql('''SELECT address,lat,lng FROM %(table)s
                          WHERE map_id = %(map)i;'''
                          % ({"map": wpmaps_id, "table": wpmaps_table}))
    wpcalc_data_sql = sql('''SELECT ident FROM %(table)s;'''
                          % ({"table": wpcalc_table}))

    # wpmaps_pair = "['foo to bar', IDENT, 'foo position', 'bar position']"
    wpmaps_pair = generate_data_pair(wpmaps_data_sql)
    ident_matcher(wpmaps_pair)

    if wpmaps_pair:
        skipped = 0
        calculated = 0
        for wpmaps_index, wpmaps_pair in enumerate(wpmaps_pair, start=1):
            ident_found = False
            wpmaps_description, wpmaps_ident, dfrom, dto = wpmaps_pair

            # search for same ident, if ident exist skip pair
            for wpcalc_ident in wpcalc_data_sql:
                if wpcalc_ident[0] == wpmaps_ident:
                    skipped += 1
                    ident_found = True
                    break

            # else assume new calculaten, proceed
            if not ident_found:
                distance, method = try_calculate_default(dfrom, dto)
                if method is not None and isinstance(distance, int):
                    insert_calculation_sql(wpmaps_index, wpmaps_description,
                                           distance, wpmaps_ident, method,
                                           dfrom, dto)
                    calculated += 1
                    logger.info("calculated - %s - %i with %s"
                                % (wpmaps_description, distance, method))

        logger.info("total %i km - skipped: %i - calculated: %i "
                    % (get_total_distance(), skipped, calculated))


# draw a line, combine all positions with delim ','
def polyline_getdirections():
    final = ""
    data = sql('''SELECT lat,lng FROM %(table)s;'''
               % ({"table": wpmaps_table}))
    for a in data:
        lat, lng = a
        final += ("(%s, %s)," % (lat, lng))

    return final[:-1]


# return rounded distance sum in kilometer
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
    default_calculation()
    update_polyline_sql(polyline_getdirections())

try:
    if db.open:
        db.close()
except NameError:
    exit()
