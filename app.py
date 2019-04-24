from flask import Flask, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from decouple import config
import json

from models import *


app     = Flask(__name__)
DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
SESSION = sessionmaker(bind=ENGINE)


@app.route("/cities", methods=["GET"])
def get_cities():
    cities = []
    for instance in SESSION.query(City).all():
        if instance.state:
            cities.append({
                "id": instance.id,
                "string": "{}, {}, {}".format(
                    instance.city,
                    instance.state,
                    instance.country
                )
            })
        else:
            cities.append({
                "id": instance.id,
                "string": "{}, {}".format(
                    instance.city,
                    instance.country
                )
            })
    return json.dumps({"cities": cities})


@app.route("/city/<cityid>/shapes", methods=["GET"])
def get_city_shapes(cityid):
    startdate  = request.args.get("startdate")
    enddate    = request.args.get("enddate")
    dotw       = request.args.get("dotw")
    crimetypes = request.args.get("crimetypes")

    return json.dumps({
        "cityid": cityid,
        "start": startdate,
        "end": enddate,
        "dotw": dotw,
        "types": crimetypes
    })


# @app.route("/city/<cityid>/data", methods=["GET"])
# def get_city_shapes(cityid):
#     return json.dumps({"cityid": cityid})


# @app.route("/add/city", methods=["POST"]):
# def add_data():
#     data = request.form.get("data")
#     if data:
#         data = json.loads(data)
#         for city in data:



# @app.route("/add/data", methods=["POST"]):
# def add_data():
#     data = request.form.get("data")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
