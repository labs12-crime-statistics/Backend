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


# @app.route("/city/<int:cityid>/data", methods=["GET"])
# def get_city_shapes(cityid):
#     startdate  = request.args.get("startdate")
#     enddate    = request.args.get("enddate")
#     dotw       = request.args.get("dotw")
#     crimetypes = request.args.get("crimetypes")

#     return json.dumps({
#         "cityid": cityid,
#         "start": startdate,
#         "end": enddate,
#         "dotw": dotw,
#         "types": crimetypes
#     })


# @app.route("/add/city", methods=["POST"]):
# def add_data():
#     data = request.form.get("data")
#     if data:
#         data = json.loads(data)
#         for city in data:
#             if "city" in city and "state" in city and "country" in city and "zipcode" in zipcode:
#                 if SESSION.query(City).filter(
#                         City.city == city["city"],
#                         City.state == city["state"],
#                         City.country == city["country"],
#                         City.zipcode == city["zipcode"]
#                     ).count() > 0:
#                     city = SESSION.query(City).filter(
#                         City.city == city["city"],
#                         City.state == city["state"],
#                         City.country == city["country"],
#                         City.zipcode == city["zipcode"]
#                     ).one()
#                 else:
#                     city = City(city["city"], city["state"], city["country"], city["zipcode"])
#                     SESSION.add(city)
#                     SESSION.refresh(city)
#             elif "city" in city and "country" in city and "zipcode" in zipcode:
#                 if SESSION.query(City).filter(
#                         City.city == city["city"],
#                         City.country == city["country"],
#                         City.zipcode == city["zipcode"]
#                     ).count() > 0:
#                     city = SESSION.query(City).filter(
#                         City.city == city["city"],
#                         City.country == city["country"],
#                         City.zipcode == city["zipcode"]
#                     ).one()
#                 else:
#                     city = City(city["city"], city["country"], city["zipcode"])
#                     SESSION.add(city)
#                     SESSION.refresh(city)
#             blocks = []
#             for block in city["blocks"]:
#                 str_poly = "MULTIPOLYGON("
#                 for i0 in block["coordinates"]:
#                     str_poly += "("
#                     for ind, i1 in enumerate(i0):
#                         if ind > 0:
#                             str_poly += ","    
#                         str_poly += "("+",".join(["{} {}".format(j[0], j[1]) for j in i1])+")"
#                     str_poly += ")"
#                 str_poly += ")"
#                 blocks.append(Blocks(cityid=city.id, shape=str_poly, population=block["population"]))
#             SESSION.add_all(blocks)
#             SESSION.commit()
#         return json.dumps({"success": "true", "error": "none"})



# @app.route("/add/data", methods=["POST"]):
# def add_data():
#     data = request.form.get("data")

if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
