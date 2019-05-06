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

import json
import datetime
import math
import io

from models import *
from utils import get_data, get_data_download


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
@app.route("/predict/<int:cityid>", methods=["GET"])
def get_predict_data(cityid):
    query = """SELECT blockid, prediction FROM block WHERE cityid = :cityid AND prediction ID NOT NULL;"""
    prediction = {}
    for row in SESSION.execute(text(query), {"cityid": cityid}).fetchall():
        prediction[r[0]] = np.frombuffer(row[1], dtype=np.float64).reshape((12,168)).tolist()
    return Response(
        response=json.dumps({"error": "none", "prediction": json.dumps(prediction)}),
        status=200,
        mimetype='application/json'
    )


# Start job in queue or download incident data for city
@app.route("/city/<int:cityid>/download", methods=["GET"])
def download_data(cityid):
    config_dict = {}
    config_dict["cityid"] = cityid
    config_dict["sdt"] = request.args.get("sdt","01/01/1900")
    config_dict["edt"] = request.args.get("edt","01/01/2100")
    config_dict["cyear"] = int(request.args.get("cyear"))
    config_dict["stime"] = int(request.args.get("s_t","0"))
    config_dict["etime"] = int(request.args.get("e_t","24"))
    dotw = request.args.get("dotw","")
    crimetypes = request.args.get("crimetypes","")
    locdesc1 = request.args.get("locdesc1","").split(",")
    locdesc2 = request.args.get("locdesc2","").split(",")
    locdesc3 = request.args.get("locdesc3","").split(",")
    new_job = q.enqueue(get_data_download, config_dict, dotw, crimetypes, locdesc1, locdesc2, locdesc3)
    output = get_status(new_job)

    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE('{sdt}', 'MM/DD/YYYY') AND datetime <= TO_DATE('{edt}', 'MM/DD/YYYY')"
    query_year    = "incident.year = {cyear}"
    query_time    = "incident.hour >= {stime} AND hour <= {etime}"
    query_dotw    = "incident.dow = ANY({dotw})"
    query_crmtyp  = "crimetype.category = ANY({crimetypes})"
    query_locdesc = "(locdesctype.key1, locdesctype.key2, locdesctype.key3) = ANY({lockeys})"
    query_join    = "INNER JOIN crimetype ON incident.crimetypeid = crimetype.id INNER JOIN locdesctype ON incident.locdescid = locdesctype.id INNER JOIN city ON incident.cityid = city.id AND "

    base_list = [query_city, query_date, query_time]
    outputs   = ", ".join(["city.city", "city.state", "city.country", "incident.datetime", "incident.location", "crimetype.category", "locdesctype.key1 AS location_key1", "locdesctype.key2 AS location_key2", "locdesctype.key3 AS location_key3"])
    if dotw != "":
        config_dict["dotw"] = dotw.split(",")
        base_list.append(query_dotw)
    if crimetypes != "":
        config_dict["crimetypes"] = ["'{}'".format(x) for x in crimetypes.split(",")]
        config_dict["crimetypes"] = "ARRAY[{}]".format(", ".join(config_dict["crimetypes"]))
        base_list.append(query_crmtyp)
    if locdesc1 != [""] and locdesc2 != [""] and locdesc3 != [""] and len(locdesc1) == len(locdesc2) and len(locdesc2) == len(locdesc3):
        config_dict["lockeys"] = []
        for i in range(len(locdesc1)):
            config_dict["lockeys"].append("('{}', '{}', '{}')".format(locdesc1[i], locdesc2[i], locdesc3[i]))
        config_dict["lockeys"] = "ARRAY[{}]".format(", ".join(config_dict["lockeys"]))
        base_list.append(query_locdesc)

    query = "COPY (SELECT " + outputs + query_base + query_join + (" AND ".join(base_list)).format(**config_dict) +") TO STDOUT WITH DELIMITER ',' CSV;"
    with io.StringIO() as f:
        RAW_CONN = create_engine(DB_URI).raw_connection()
        cursor = RAW_CONN.cursor()
        cursor.copy_expert(query, f)
        cursor.close()
        RAW_CONN.close()
        f.seek(0)
        data = pd.read_csv(f, sep=",")
        data.loc[:,"location"] = data.loc[:,"location"].apply(lambda x: [float(y) for y in wkt.dumps(wkb.loads(bytes.fromhex(x))).replace("(", "").replace(")", "").split(" ")[1:]])
        data.loc[:,"latitude"] = data.loc[:,"location"].apply(lambda x: x[0])
        data.loc[:,"longitude"] = data.loc[:,"location"].apply(lambda x: x[1])
        data = data.drop(columns=["location"])
    with io.StringIO() as f:
        data.to_csv(f, index=False)
        return Response(
            response=f.getvalue(),
            status=200,
            mimetype='text/csv'
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
                SESSION.delete(job)
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
        config_dict["sdt"] = request.args.get("s_d","01/01/1900")
        config_dict["edt"] = request.args.get("e_d","01/01/2100")
        config_dict["stime"] = int(request.args.get("s_t","0"))
        config_dict["etime"] = int(request.args.get("e_t","23"))
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
