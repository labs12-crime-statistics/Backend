"""Creates Flask backend application."""

from flask import Flask, request, Response
from flask_cors import CORS
import redis
from rq import Worker, Queue, Connection
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from decouple import config
from geomet import wkb, wkt
import pandas as pd
import numpy as np

import json
import datetime
import math
import io
import sys

from models import *
from utils import get_data, get_download, get_shapes, get_predictions


# Create Flask app and allow for CORS
app       = Flask(__name__)
redis_url = config('REDIS_URL')
q         = Queue('high', connection=redis.from_url(redis_url))
CORS(app)

# Connect to DB and create session with DB
DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)


# Query for job
def get_status(job):
    status = {
        'id': job.id,
        'result': job.result,
        'status': 'failed' if job.is_failed else 'pending' if job.result == None else 'completed'
    }
    status.update(job.meta)
    return status


# Endpoints for backend
@app.route("/", methods=["GET"])
def health_check():
    return Response(
        response=json.dumps({'error': 'none', 'data': 'Health check good.'}),
        status=200,
        mimetype='application/json'
    )


# Get list of cities in json format
@app.route("/cities", methods=["GET"])
def get_cities():
    """Get all cities in DB with respective id and user friendly name."""
    cities = []
    SESSION = Session()
    for instance in SESSION.query(City).all():
        if instance.state:
            cities.append({
                "id": instance.id,
                "string": "{}, {}, {}".format(
                    instance.city,
                    instance.state,
                    instance.country
                ).title()
            })
        else:
            cities.append({
                "id": instance.id,
                "string": "{}, {}".format(
                    instance.city,
                    instance.country
                ).title()
            })
    SESSION.close()
    return Response(
        response=json.dumps({"cities": cities, "error": "none"}),
        status=200,
        mimetype='application/json'
    )


@app.route("/city/<int:cityid>/location", methods=["GET"])
def get_location_blockid(cityid):
    try:
        SESSION = Session()
        lat = float(request.args.get("lat"))
        lng = float(request.args.get("lng"))
        query = """SELECT id FROM block WHERE ST_CONTAINS(shape, ST_GEOMFROMTEXT('POINT(:lat :lng)')) AND cityid = :cityid LIMIT 1;"""
        blockid = SESSION.execute(text(query), {"lat": lat, "lng": lng, "cityid": cityid}).fetchone()
        SESSION.close()
        if blockid:
            blockid = blockid[0]
            return Response(
                response=json.dumps({"blockid": blockid, "error": "none"}),
                status=200,
                mimetype='application/json'
            )
        else:
            return Response(
                response=json.dumps({"error": "NO_BLOCK"}),
                status=200,
                mimetype='application/json'
            )
    except:
        return Response(
            response=json.dumps({"error": "Incorrect location format."}),
            status=404,
            mimetype='application/json'
        )


# Get zipcode and census tract geometries
@app.route("/city/<int:cityid>/shapes", methods=["GET"])
def get_city_shapes(cityid):
    """Get all blocks for a specific City id with their respective id, shape
        and the city center coordinates."""
    query_id = request.args.get('job')
    if query_id:
        found_job = q.fetch_job(query_id)
        if found_job:
            output = get_status(found_job)
            if output["status"] == "completed":
                SESSION = Session()
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["id"] = query_id
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-2)).delete()
                SESSION.commit()
                SESSION.close()
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
            else:
                output["id"] = query_id
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
        else:
            output = { 'id': None, 'error_message': 'No job exists with the id number ' + query_id }
            return Response(
                response=json.dumps(output),
                status=403,
                mimetype='application/json'
            )
    else:
        new_job = q.enqueue(get_shapes, cityid)
        output = get_status(new_job)
        return Response(
            response=json.dumps(output),
            status=200,
            mimetype='application/json'
        )


# Get prediction values for city
@app.route("/city/<int:cityid>/predict", methods=["GET"])
def get_predict_data(cityid):
    query_id = request.args.get('job')
    if query_id:
        found_job = q.fetch_job(query_id)
        if found_job:
            output = get_status(found_job)
            if output["status"] == "completed":
                SESSION = Session()
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["id"] = query_id
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-2)).delete()
                SESSION.commit()
                SESSION.close()
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
            else:
                output["id"] = query_id
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
        else:
            output = { 'id': None, 'error_message': 'No job exists with the id number ' + query_id }
            return Response(
                response=json.dumps(output),
                status=403,
                mimetype='application/json'
            )
    else:
        new_job = q.enqueue(get_predictions, cityid)
        output = get_status(new_job)
        return Response(
            response=json.dumps(output),
            status=200,
            mimetype='application/json'
        )


# Start job in queue or download incident data for city
@app.route("/city/<int:cityid>/download", methods=["GET"])
def download_data(cityid):
    query_id = request.args.get('job')
    if query_id:
        found_job = q.fetch_job(query_id)
        if found_job:
            output = get_status(found_job)
            if output["status"] == "completed":
                SESSION = Session()
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["id"] = query_id
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-2)).delete()
                SESSION.commit()
                SESSION.close()
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
            else:
                output["id"] = query_id
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
        else:
            output = { 'id': None, 'error_message': 'No job exists with the id number ' + query_id }
            return Response(
                response=json.dumps(output),
                status=403,
                mimetype='application/json'
            )
    else:
        config_dict = {}
        config_dict["cityid"] = cityid
        config_dict["sdt"] = request.args.get("sdt","01/01/1900")
        config_dict["edt"] = request.args.get("edt","01/01/2100")
        config_dict["cyear"] = int(request.args.get("cyear"))
        config_dict["stime"] = int(request.args.get("s_t","0"))
        config_dict["etime"] = int(request.args.get("e_t","24"))
        dotw = request.args.get("dotw","")
        crimevio = request.args.get("crimeviolence","")
        crimeppo = request.args.get("crimeppos","")
        locgroups = request.args.get("locgroups","")
        new_job = q.enqueue(get_download, config_dict, dotw, crimevio, crimeppo, locgroups)
        output = get_status(new_job)
        return Response(
            response=json.dumps(output),
            status=200,
            mimetype='application/json'
        )


# Get aggregate data for city
@app.route("/city/<int:cityid>/data", methods=["GET"])
def get_city_data(cityid):
    """Get values for specified parameters and city."""
    query_id = request.args.get('job')
    if query_id:
        found_job = q.fetch_job(query_id)
        if found_job:
            output = get_status(found_job)
            if output["status"] == "completed":
                SESSION = Session()
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-2)).delete()
                SESSION.commit()
                SESSION.close()
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
            else:
                output["id"] = query_id
                return Response(
                    response=json.dumps(output),
                    status=200,
                    mimetype='application/json'
                )
        else:
            output = { 'id': None, 'error_message': 'No job exists with the id number ' + query_id }
            return Response(
                response=json.dumps(output),
                status=403,
                mimetype='application/json'
            )
    else:
        config_dict = {}
        config_dict["cityid"] = cityid
        config_dict["sdt"] = request.args.get("sdt","01/01/1900")
        config_dict["edt"] = request.args.get("edt","01/01/2100")
        config_dict["stime"] = request.args.get("stime","0")
        config_dict["etime"] = request.args.get("etime","23")
        config_dict["loadtype"] = request.args.get("type","")
        poss_load = ["map","date","dateall","time","timeall","dow","dowall","crimevioall","crimevioblock","crimeppoall","crimeppoblock","locall","locblock"]
        if config_dict["loadtype"] not in poss_load:
            config_dict["loadtype"] = ""
        if config_dict["sdt"] == "//":
            config_dict["sdt"] = "01/01/1900"
        if config_dict["edt"] == "//":
            config_dict["edt"] = "01/01/2100"
        if config_dict["stime"] == "":
            config_dict["stime"] = "0"
        config_dict["stime"] = int(config_dict["stime"])
        if config_dict["etime"] == "":
            config_dict["etime"] = "23"
        config_dict["etime"] = int(config_dict["etime"])
        blockid = int(request.args.get("blockid","-1"))
        dotw = request.args.get("dotw","")
        crimevio = request.args.get("crimeviolence","")
        crimeppo = request.args.get("crimeppos","")
        locgroups = request.args.get("locgroups","")
        new_job = q.enqueue(get_data, config_dict, blockid, dotw, crimevio, crimeppo, locgroups)
        output = get_status(new_job)
        return Response(
            response=json.dumps(output),
            status=200,
            mimetype='application/json'
        )


if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
