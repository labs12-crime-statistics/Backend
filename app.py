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
from utils import get_data, get_download


# Create Flask app and allow for CORS
app     = Flask(__name__)
redis_url = config('REDIS_URL')
q         = Queue('high', connection=redis.from_url(redis_url))
CORS(app)

# Connect to DB and create session with DB
DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()


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
    return Response(
        response=json.dumps({"cities": cities, "error": "none"}),
        status=200,
        mimetype='application/json'
    )


@app.route("/city/<int:cityid>/location", methods=["GET"])
def get_location_blockid(cityid):
    try:
        lat = float(request.args.get("lat"))
        lng = float(request.args.get("lng"))
        query = """SELECT id FROM block WHERE ST_CONTAINS(shape, ST_GEOMFROMTEXT('POINT(:lat :lng)')) AND cityid = :cityid LIMIT 1;"""
        blockid = SESSION.execute(text(query), {"lat": lat, "lng": lng, "cityid": cityid}).fetchone()
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
    if SESSION.query(City).filter(City.id == cityid).count() > 0:
        citycoords_req = SESSION.query(City.location).filter(City.id == cityid)
        citycoords_bytes = citycoords_req.one()[0].data.tobytes()
        citycoords = wkb.loads(citycoords_bytes)["coordinates"]
        blocks = [{
            "id": block.id,
            "shape": wkb.loads(block.shape.data.tobytes())["coordinates"]
        } for block in
            SESSION.query(Blocks).filter(Blocks.cityid == cityid).all()]
        zipcodes = [{
            "zipcode": zipcode.zipcode,
            "shape": wkb.loads(zipcode.shape.data.tobytes())["coordinates"]
        } for zipcode in
            SESSION.query(ZipcodeGeom).filter(ZipcodeGeom.cityid == cityid).all()]
        return Response(
            response=json.dumps({
                "error": "none",
                "blocks": blocks,
                "zipcodes": zipcodes,
                "citylocation": citycoords}),
            status=200,
            mimetype='application/json'
        )
    return Response(
        response=json.dumps({"error": "Incorrect city id value."}),
        status=404,
        mimetype='application/json'
    )


# Get prediction values for city
@app.route("/city/<int:cityid>/predict", methods=["GET"])
def get_predict_data(cityid):
    query = f"""SELECT id, ENCODE(prediction::BYTEA, 'hex') AS predict, month, year, population FROM block WHERE cityid = {cityid} AND prediction IS NOT NULL;"""
    prediction = {}
    all_dates = []
    block_date = {}
    population = {}
    with ENGINE.connect() as CONN:
        df = pd.read_sql_query(query, CONN)
    df.loc[:,"start"] = df.apply(lambda x: x["month"]+12*x["year"], axis=1)
    df.loc[:,"prediction"] = df["prediction"].apply(lambda x: np.frombuffer(bytes.fromhex(x), dtype=np.float64).reshape((12,7,24)))
    all_dates = list(range(df["start"].min(), df["start"].max()+12))
    predictions_n = {}
    predictionall = np.zeros((len(all_dates),7,24))
    for k in df["prediction"]:
        dift = block_date[k]-all_dates[0]
        predictions_n[k] = np.zeros((len(all_dates),7,24))
        predictions_n[k][dift:dift+12,:,:] = prediction[k]
        predictionall += predictions_n[k] * population[k]
        predictions_n[k] = predictions_n[k].tolist()
    all_dates_format = ["{}/{}".format(x%12+1,x//12) for x in all_dates]
    predictionall = (predictionall / sum([population[x] for x in population])).tolist()
    return Response(
        response=json.dumps({"error": "none", "predictionAll": predictionall, "allDatesFormatted": all_dates_format, "allDatesInt": all_dates, "prediction": predictions_n}),
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
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["id"] = query_id
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-1)).delete()
                SESSION.commit()
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
        crimetypes = request.args.get("crimetypes","").split(",")
        locdesc1 = request.args.get("locdesc1","").split(",")
        locdesc2 = request.args.get("locdesc2","").split(",")
        locdesc3 = request.args.get("locdesc3","").split(",")    
        new_job = q.enqueue(get_download, config_dict, dotw, crimetypes, locdesc1, locdesc2, locdesc3)
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
                job = SESSION.query(Job).filter(Job.id == output["result"]).one()
                output["result"] = job.result
                SESSION.query(Job).filter(Job.datetime < datetime.datetime.utcnow() + datetime.timedelta(hours=-1)).delete()
                SESSION.commit()
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
        poss_load = ["","time","dow","crimeall","crimeblock","locall","locblock"]
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
        crimetypes = request.args.get("crimetypes","")
        locdesc1 = request.args.get("locdesc1","").split(",")
        locdesc2 = request.args.get("locdesc2","").split(",")
        locdesc3 = request.args.get("locdesc3","").split(",")
        new_job = q.enqueue(get_data, config_dict, blockid, dotw, crimetypes, locdesc1, locdesc2, locdesc3)
        output = get_status(new_job)
        return Response(
            response=json.dumps(output),
            status=200,
            mimetype='application/json'
        )


if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
