"""Creates Flask backend application."""

from flask import Flask, request, Response
from flask_cors import CORS
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from decouple import config
from geomet import wkb
import pandas as pd
import json
import datetime

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
    return Response(
        response=json.dumps({'error': 'none', 'data': 'Health check good.'}),
        status=200,
        mimetype='application/json'
    )


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
    return Response(
        response=json.dumps({"error": "Incorrect city id value."}),
        status=200,
        mimetype='application/json'
    )


@app.route("/city/<int:cityid>/data", methods=["GET"])
def get_city_data(cityid):
    """Get values for specified parameters and city."""
    config_dict = {}
    config_dict["cityid"] = int(cityid)
    config_dict["sdt"] = datetime.datetime.strptime(request.args.get("s_d","01/01/1900"), "%m/%d/%Y")
    config_dict["edt"] = datetime.datetime.strptime(request.args.get("e_d","01/01/2100"), "%m/%d/%Y")
    config_dict["stime"] = int(request.args.get("s_t","0"))
    config_dict["etime"] = int(request.args.get("e_t","24"))
    blockid = int(request.args.get("blockid","-1"))
    dotw = request.args.get("dotw","")
    crimetypes = request.args.get("crimetypes","")
    crimeprim = request.args.get("crimeprim","")

    query_base   = " FROM incident "
    query_city   = "incident.cityid = :cityid"
    query_date   = "datetime >= :sdt AND datetime <= :edt"
    query_time   = "EXTRACT(hour FROM datetime) >= :stime AND EXTRACT(hour FROM datetime) <= :etime"
    query_block  = "blockid = :blockid"
    query_dotw   = "EXTRACT(dow FROM datetime) = ANY(:dotw)"
    query_crmtyp = "category = ANY(:crimetypes)"
    query_crmprm = "SPLIT_PART(category, ' | ', 1) = ANY(:crimeprim)"
    query_join   = "INNER JOIN block ON incident.blockid = block.id INNER JOIN crimetype ON incident.crimetypeid = crimetype.id AND "
    q_base_end   = "blockid, EXTRACT(year FROM datetime), EXTRACT(month FROM datetime)"
    q_date_end   = "EXTRACT(year FROM datetime), EXTRACT(month FROM datetime)"
    q_time_end   = "EXTRACT(hour FROM datetime)"
    q_dotw_end   = "EXTRACT(dow FROM datetime)"
    q_crmtyp_end = "category"

    base_list = {"city": query_city, "date": query_date, "time": query_time}
    if dotw != "":
        config_dict["dotw"] = [int(x) for x in dotw.split(",")]
        base_list["dow"] = query_dotw
    if crimeprim != "" and crimetypes != "":
        config_dict["crimetypes"] = crimetypes.split(",")
        config_dict["crimeprim"] = crimeprim.split(",")
        base_list["crime"] = "({} OR {})".format(query_crmtyp, query_crmprm)
    elif crimetypes != "":
        config_dict["crimetypes"] = crimetypes.split(",")
        base_list["crime"] = query_crmtyp
    elif crimeprim != "":
        config_dict["crimeprim"] = crimeprim.split(",")
        base_list["crime"] = query_crmprm
    if blockid != -1:
        config_dict["blockid"] = blockid

    funcs = {
        "map": lambda res: [{"severity": r[0], "blockid": int(r[1]), "month": int(r[3]), "year": int(r[2])} for r in res],
        "date": lambda res: [{"severity": r[0], "month": int(r[2]), "year": int(r[1])} for r in res],
        "time": lambda res: [{"severity": r[0], "hour": int(r[1])} for r in res],
        "dotw": lambda res: [{"severity": r[0], "dow": int(r[1])} for r in res],
        "crmtyp": lambda res: [{"count": r[0], "category": r[1]} for r in res],
        "date_all": lambda res: [{"severity": r[0], "month": int(r[2]), "year": int(r[1])} for r in res],
        "time_all": lambda res: [{"severity": r[0], "hour": int(r[1])} for r in res],
        "dotw_all": lambda res: [{"severity": r[0], "dow": int(r[1])} for r in res],
        "crmtyp_all": lambda res: [{"count": r[0], "category": r[1]} for r in res]
    }
    
    charts = {
        "map": "SELECT SUM(severity), " + q_base_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_base_end,
        "date_all": "SELECT SUM(severity), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "date"]) + " GROUP BY " + q_date_end,
        "time_all": "SELECT SUM(severity), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]) + " GROUP BY " + q_time_end,
        "dotw_all": "SELECT SUM(severity), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]) + " GROUP BY " + q_dotw_end,
        "crmtyp_all": "SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]) + " GROUP BY " + q_crmtyp_end
    }
    if blockid != -1:
        charts["date"] = "SELECT SUM(severity), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "date"]+[query_block]) + " GROUP BY " + q_date_end
        charts["time"] = "SELECT SUM(severity), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]+[query_block]) + " GROUP BY " + q_time_end
        charts["dotw"] = "SELECT SUM(severity), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]+[query_block]) + " GROUP BY " + q_dotw_end
        charts["crmtyp"] = "SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]+[query_block]) + " GROUP BY " + q_crmtyp_end
    results = {}
    for k in charts:
        res = SESSION.execute(text(charts[k]), config_dict).fetchall()
        results[k] = funcs[k](res)
    
    result = {
        "error": "none",
        "main": {
            "all": {
                "values_date": [],
                "values_time": [],
                "values_dow": [],
                "values_type": []
            }
        },
        "other": [],
        "timeline": []
    }
    
    map_df = pd.DataFrame(results["map"])
    map_cross = pd.crosstab(map_df["blockid"], [map_df["year"], map_df["month"]], values=map_df["severity"], aggfunc='sum').fillna(0.0)
    result["timeline"] = [{"year": c[0], "month": c[1]} for c in map_cross]
    for i in map_cross.index:
        result["other"].append({
            "id": i,
            "values": list(map_cross.loc[i,:].values)
        })
    
    result["main"]["all"]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"]} for c in results["date_all"]]
    result["main"]["all"]["values_time"] = [{"x": c["hour"], "y": c["severity"]} for c in results["time_all"]]
    result["main"]["all"]["values_dow"] = [{"x": c["dow"], "y": c["severity"]} for c in results["dotw_all"]]

    data = {}
    for r in results["crmtyp_all"]:
        vals = r["category"].split(" | ")
        if vals[0] not in data:
            data[vals[0]] = {}
        data[vals[0]][vals[1]] = r["count"]
    n_data = {
        "name": "Crime Type for All Data",
        "children": []
    }
    for k1 in data:
        t_d = {"name": k1, "children": []}
        for k2 in data[k1]:
            t_d["children"].append({"name": "{} | {}".format(k1,k2), "loc": data[k1][k2]})
        n_data["children"].append(t_d)
    result["main"]["all"]["values_type"] = n_data

    if blockid != -1:
        result["main"][blockid] = {}
        result["main"][blockid]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"]} for c in results["date"]]
        result["main"][blockid]["values_time"] = [{"x": c["hour"], "y": c["severity"]} for c in results["time"]]
        result["main"][blockid]["values_dow"] = [{"x": c["dow"], "y": c["severity"]} for c in results["dotw"]]

        data = {}
        for r in results["crmtyp"]:
            vals = r["category"].split(" | ")
            if vals[0] not in data:
                data[vals[0]] = {}
            data[vals[0]][vals[1]] = r["count"]
        n_data = {
            "name": "Crime Type for All Data",
            "children": []
        }
        for k1 in data:
            t_d = {"name": k1, "children": []}
            for k2 in data[k1]:
                t_d["children"].append({"name": "{} | {}".format(k1,k2), "loc": data[k1][k2]})
            n_data["children"].append(t_d)
        result["main"][blockid]["values_type"] = n_data
    return Response(
        response=json.dumps(result),
        status=200,
        mimetype='application/json'
    )


if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
