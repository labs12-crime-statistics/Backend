"""Creates Flask backend application."""

from flask import Flask, request
from flask_cors import CORS
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from decouple import config
from geomet import wkb
import json

from models import *


# Create Flask app and allow for CORS
app     = Flask(__name__)
CORS(app)

# Connect to DB and create session with DB
DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()


# Endpoints for backend
@app.route("/", methods=["GET"])
def health_check():
    return json.dumps({'error': 'none', 'data': 'Health check good.'})


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
    return json.dumps({"cities": cities, "error": "none"})


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
        return json.dumps({
            "error": "none",
            "blocks": blocks,
            "citylocation": citycoords})
    return json.dumps({"error": "Incorrect city id value."})


@app.route("/city/<int:cityid>/data", methods=["GET"])
def get_city_shapes(cityid):
    """Get values for specified parameters and city."""
    config_dict["s_date"] = datetime.datetime(request.args.get("s_d","1/1900"))
    config_dict["e_date"] = datetime.datetime(request.args.get("e_d","12/2099"))
    config_dict["s_time"] = int(request.args.get("s_t","0"))
    config_dict["e_time"] = int(request.args.get("e_t","24"))
    blockid = int(request.args.get("blockid","-1"))
    dotw = request.args.get("dotw","")
    crimetypes = request.args.get("crimetypes","")

    query = """SELECT SUM(crimetype.severity)
        FROM crimetype, instance
        WHERE
            instance.datetime >= :s_date::date
            AND instance.datetime <= :e_date::date
            AND EXTRACT(hour FROM instance.datetime) >= :s_time
            AND EXTRACT(hour FROM instance.datetime) >= :e_time
    """
    if dotw != "":
        config_dict["dotw"] = [int(x) for x in dotw.split(",")]
        query += "AND EXTRACT(DOW FROM instance.datetime) IN :dotw\n"
    if crimetypes != "":
        config_dict["crimetypes"] = crimetypes.split(",")
        query += """AND (SELECT crimetype.category 
                            FROM instance, crimetype
                            WHERE instance.crimetypeid = crimetype.id)
                    IN :crimetypes\n"""
    if blockid != -1:
        config_dict["blockid"] = blockid
        query += "AND instance.blockid != :blockid\n"
    query += "GROUP BY instance.blockid;"
    res = SESSION.execute(text(query), config_dict).fetchall()
    print(res)

    return json.dumps({})


if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
