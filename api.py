#!/usr/bin/python
"Crunchy Data Loader Demo for Containers"
# -*- coding: utf-8 -*-
# pylint: disable=no-member
import os
import json
import sys
from pprint import pprint
import psycopg2
from flask import *
from werkzeug.utils import secure_filename
import urllib.request

dir_path = os.path.dirname(os.path.realpath(__file__))

app = Flask(__name__)
app.debug = False
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

db_user = os.getenv('PG_USER')
db_password = os.getenv('PG_PASSWORD')
db_name = os.getenv('PG_DATABASE_1')
db_port = os.getenv('PG_PORT')
db_host = os.getenv('PG_HOST')
table_name = os.getenv('TABLE_NAME')

def db_conn(db_name, db_user, db_host, db_password, db_port):
    conn_string = "dbname='%s' user='%s' host='%s' password='%s' port='%s'" % (
        db_name, db_user, db_host, db_password, db_port)
    try:
        conn = psycopg2.connect(conn_string)
    except psycopg2.Error as e:
        if debug:
            print(process_psycopg2_error(e))
            flash(process_psycopg2_error(e))
        return None
    else:
        return conn

#applies a buffer to a parcel gid and returns all parcels that intersect with that buffer
@app.route("/buffer/gid=<gid>/buffer=<buffer>")
def parcel_distance(gid, buffer, table_name):
    conn = db_conn(db_name, db_user, db_host, db_password, db_port)
    print(gid, buffer)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("select array_to_json(array_agg(row_to_json(t))) FROM (SELECT a.gid, a.apn FROM % a, % b WHERE ST_DWithin(a.geom, b.geom, %s) AND b.gid = %s) t;", (table_name, buffer, gid))
            result = cursor.fetchall()
            print(result)
        except psycopg2.Error as e:
            if debug:
                print(process_psycopg2_error(e))
            return None
        finally:
            conn.close
            cursor.close()
            return str(result)
    else:
        return None

#returns all parcels from assessor_parcels based on firehazard attribute input as geojson
def execute_geojson_query(table_name, attribute):
    conn = db_conn(db_name, db_user, db_host, db_password, db_port)
    print(attribute)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("select array_to_json(array_agg(row_to_json(t))) FROM ( SELECT gid, siteadd, sitcity, ST_AsGeoJSON(geom) FROM %s WHERE firehazard = %s) t;", (table_name, attribute,))
            hazard_result = cursor.fetchall()
            print('success')
        except psycopg2.Error as e:
            if debug:
                print(process_psycopg2_error(e))
            return None
        finally:
            conn.close
            cursor.close()
            return str(hazard_result)
    else:
        return None

#uses the assessor_parcels dataset from the Santa Cruz GIS site, returns as JSON       
def execute_query(attribute):
    conn = db_conn(db_name, db_user, db_host, db_password, db_port)
    print(attribute)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("select array_to_json(array_agg(row_to_json(t))) FROM ( SELECT gid, siteadd, sitcity FROM %s WHERE firehazard = %s) t;", (table_name, attribute,))
            hazard_result = cursor.fetchall()
            print(hazard_result)
        except psycopg2.Error as e:
            if debug:
                print(process_psycopg2_error(e))
            return None
        finally:
            conn.close
            cursor.close()
            return str(hazard_result)
    else:
        return None            
#uses the assesor_parcels dataset from the Santa Cruz GIS site, filters on the firehazard column 
def execute_update(tabel_name, attribute, gid):
    conn = db_conn(db_name, db_user, db_host, db_password, db_port)
    print(attribute, gid)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE %s set firehazard = %s where gid = %s;", (table_name, attribute, gid))
            conn.commit()
        except psycopg2.Error as e:
            if debug:
                print(process_psycopg2_error(e))
            return None
        finally:
            conn.close
            cursor.close()
            return ("Success",200)
    else:
        return None

#uses the assessor_parcels dataset from the Santa Cruz GIS site, updates the firehazard column 
@app.route("/firehazard/gid=<gid>/attribute=<attribute>", methods=['PUT'])
def hazard_mods(gid, attribute):
    attribute = attribute.capitalize()
    if (attribute != 'Yes' and attribute != 'No'):
        return ("Value must be Yes or No", 200)
    return execute_update(attribute, gid)

#uses the facilities and the assesor_parcels datasets from the Santa Cruz GIS site
@app.route("/facility/gid=<gid>/buffer=<buffer>", methods=['GET'])
def facility_query(gid, buffer):
    conn = db_conn(db_name, db_user, db_host, db_password, db_port)
    print(table_name, gid, buffer)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("select array_to_json(array_agg(row_to_json(t))) FROM (SELECT DISTINCT ON (a.gid) a.gid, f.address, f.phone, f.notes, f.geom, f.gid FROM facilities f LEFT JOIN %s a ON ST_DWithin(f.geom, a.geom, %s) AND a.gid= %s ORDER BY a.gid, ST_Distance(f.geom, a.geom)) t;", (table_name, buffer, gid))
            facility_result = cursor.fetchall()
            print(facility_result)
        except psycopg2.Error as e:
            if debug:
                print(process_psycopg2_error(e))
            return None
        finally:
            conn.close
            cursor.close()
            return str(facility_result)
    else:
        return None

#uses the assessor_parcels dataset from the Santa Cruz GIS site, filters based on the firehazard column 
@app.route("/firehazard/output=<output>/attribute=<attribute>", methods=['GET'])
def hazard_select(output, attribute):
    attribute = attribute.capitalize()
    try:
         if (attribute != 'Yes' and attribute != 'No'):
            return InvalidAttributeError
         elif (output != "JSON" and output != 'GEOJSON'):
             output = output.upper()
             if (output != "JSON" and output != 'GEOJSON'):
                 return InvalidOutputError
    except:
        InvalidAttributeError = print("Value must be Yes or No", 200),
        InvalidOutputError = print('Output must be in the format of JSON or GEOJSON', 200)
    else:
        print ('no input errors')
        if (output == 'JSON'):
            return execute_query(attribute) 
        elif (output =='GEOJSON'):
            return execute_geojson_query(attribute)

def process_psycopg2_error(error):
    if error is psycopg2.Error or issubclass(type(error), psycopg2.Error):
        if error.pgerror:
            return error.pgerror
        else:
            return error
    else:
        raise TypeError('error must be psycopg2.Error')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
