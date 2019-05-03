"""Creates Flask backend application."""

from flask import Flask, request, Response
from flask_cors import CORS
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

@app.route("/city/<int:cityid>/download", methods=["GET"])
def download_data(cityid):
    config_dict = {}
    config_dict["cityid"] = cityid
    config_dict["sdt"] = request.args.get("s_d","01/01/1900")
    config_dict["edt"] = request.args.get("e_d","01/01/2100")
    config_dict["stime"] = int(request.args.get("s_t","0"))
    config_dict["etime"] = int(request.args.get("e_t","24"))
    dotw = request.args.get("dotw","")
    crimetypes = request.args.get("crimetypes","")
    locdesc1 = request.args.get("locdesc1","").split(",")
    locdesc2 = request.args.get("locdesc2","").split(",")
    locdesc3 = request.args.get("locdesc3","").split(",")

    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE('{sdt}', 'MM/DD/YYYY') AND datetime <= TO_DATE('{edt}', 'MM/DD/YYYY')"
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

    query = "COPY (SELECT " + outputs + query_base + query_join + (" AND ".join(base_list)).format(**config_dict) +") TO STDOUT WITH DELIMITER ',' CSV HEADER;"
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
    return Response(
        response=json.dumps({"error": "Incorrect city id value."}),
        status=404,
        mimetype='application/json'
    )


@app.route("/city/<int:cityid>/data", methods=["GET"])
def get_city_data(cityid):
    """Get values for specified parameters and city."""
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

    query = """SELECT MAX(categories.severity)
        FROM (
            SELECT SUM(crimetype.severity)/AVG(block.population) AS severity
            FROM incident
            INNER JOIN block ON incident.blockid = block.id
            INNER JOIN crimetype ON incident.crimetypeid = crimetype.id
                AND block.population > 0
            GROUP BY
                incident.blockid,
                incident.year,
                incident.month,
                incident.dow,
                incident.hour
        ) AS categories;"""
    severity = float(SESSION.execute(text(query)).fetchone()[0]) * 24 * 7
    query = """SELECT COUNT(*) FROM (
        SELECT COUNT(*)
        FROM incident
        WHERE incident.datetime >= TO_DATE(:sdt, 'MM/DD/YYYY') AND incident.datetime <= TO_DATE(:edt, 'MM/DD/YYYY')
        GROUP BY incident.year, incident.month
    ) AS month_count;"""
    months_mult = 1.0 / SESSION.execute(text(query), {"sdt": config_dict["sdt"], "edt": config_dict["edt"]}).fetchone()[0]

    query_base    = " FROM incident "
    query_city    = "incident.cityid = :cityid"
    query_date    = "incident.datetime >= TO_DATE(:sdt, 'MM/DD/YYYY') AND incident.datetime <= TO_DATE(:edt, 'MM/DD/YYYY')"
    query_time    = "incident.hour >= :stime AND incident.hour <= :etime"
    query_block   = "incident.blockid = :blockid"
    query_dotw    = "incident.dow = ANY(:dotw)"
    query_crmtyp  = "crimetype.category = ANY(:crimetypes)"
    query_locdesc = "ARRAY [locdesctype.key1, locdesctype.key2, locdesctype.key3] = ANY(:lockeys)"
    query_pop     = "block.population > 0"
    query_join    = "INNER JOIN block ON incident.blockid = block.id INNER JOIN crimetype ON incident.crimetypeid = crimetype.id INNER JOIN locdesctype ON incident.locdescid = locdesctype.id AND "
    q_base_end    = "incident.blockid, incident.year, incident.month"
    q_date_end    = "incident.year, incident.month"
    q_time_end    = "incident.hour"
    q_dotw_end    = "incident.dow"
    q_crmtyp_end  = "crimetype.category"
    q_locdesc_end = "locdesctype.key1, locdesctype.key2, locdesctype.key3"
    mult_time = 24.0 / min(config_dict["etime"] - config_dict["stime"] + 1, 24)
    mult_dow = 1


    base_list = {"city": query_city, "date": query_date, "time": query_time, "pop": query_pop}
    if dotw != "":
        config_dict["dotw"] = [int(x) for x in dotw.split(",")]
        base_list["dow"] = query_dotw
        mult_dow = 7.0 / len(config_dict["dotw"])
    if crimetypes != "":
        config_dict["crimetypes"] = crimetypes.split(",")
        base_list["crime"] = query_crmtyp
    if locdesc1 != [""] and locdesc2 != [""] and locdesc3 != [""] and len(locdesc1) == len(locdesc2) and len(locdesc2) == len(locdesc3):
        config_dict["lockeys"] = []
        for i, _ in enumerate(locdesc1):
            config_dict["lockeys"].append([locdesc1[i], locdesc2[i], locdesc3[i]])
        base_list["locdesc"] = query_locdesc
    if blockid != -1:
        config_dict["blockid"] = blockid

    funcs = {
        "map": lambda res: [{"severity": math.pow(mult_dow * mult_time * float(r[0]) / severity, 0.1), "blockid": int(r[1]), "month": int(r[3]), "year": int(r[2])} for r in res],
        "date": lambda res: [{"severity": math.pow(mult_dow * mult_time * float(r[0]) / severity, 0.1), "month": int(r[2]), "year": int(r[1])} for r in res],
        "time": lambda res: [{"severity": math.pow(24 * mult_dow * months_mult * float(r[0]) / severity, 0.1), "hour": int(r[1])} for r in res],
        "dotw": lambda res: [{"severity": math.pow(7 * months_mult * mult_time * float(r[0]) / severity, 0.1), "dow": int(r[1])} for r in res],
        "crmtyp": lambda res: [{"count": r[0], "category": r[1]} for r in res],
        "locdesc": lambda res: [{"count": r[0], "locdesc1": r[1], "locdesc2": r[2], "locdesc3": r[3]} for r in res],
        "date_all": lambda res: [{"severity": math.pow(mult_dow * mult_time * float(r[0]) / severity, 0.1), "month": int(r[2]), "year": int(r[1])} for r in res],
        "time_all": lambda res: [{"severity": math.pow(24 * mult_dow * months_mult * float(r[0]) / severity, 0.1), "hour": int(r[1])} for r in res],
        "dotw_all": lambda res: [{"severity": math.pow(7 * months_mult * mult_time * float(r[0]) / severity, 0.1), "dow": int(r[1])} for r in res],
        "crmtyp_all": lambda res: [{"count": r[0], "category": r[1]} for r in res],
        "locdesc_all": lambda res: [{"count": r[0], "locdesc1": r[1], "locdesc2": r[2], "locdesc3": r[3]} for r in res]
    }
    
    charts = {
        "map": "SELECT SUM(crimetype.severity)/AVG(block.population), " + q_base_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_base_end,
        "date_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "date"]) + " GROUP BY " + q_date_end,
        "time_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]) + " GROUP BY " + q_time_end,
        "dotw_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]) + " GROUP BY " + q_dotw_end,
        "crmtyp_all": "SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]) + " GROUP BY " + q_crmtyp_end,
        "locdesc_all": "SELECT COUNT(*), " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]) + " GROUP BY " + q_locdesc_end,
    }
    if blockid != -1:
        charts["date"] = "SELECT SUM(crimetype.severity)/AVG(block.population), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "date"]+[query_block]) + " GROUP BY " + q_date_end
        charts["time"] = "SELECT SUM(crimetype.severity)/AVG(block.population), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]+[query_block]) + " GROUP BY " + q_time_end
        charts["dotw"] = "SELECT SUM(crimetype.severity)/AVG(block.population), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]+[query_block]) + " GROUP BY " + q_dotw_end
        charts["crmtyp"] = "SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]+[query_block]) + " GROUP BY " + q_crmtyp_end
        charts["locdesc"] = "SELECT COUNT(*), " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]+[query_block]) + " GROUP BY " + q_locdesc_end
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
                "values_type": [],
                "values_locdesc": []
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
    result["main"]["all"]["values_time"] = sorted([{"x": c["hour"], "y": c["severity"]} for c in results["time_all"]], key=lambda x: x.get("x"))
    result["main"]["all"]["values_dow"] = [{"x": c["dow"], "y": c["severity"]} for c in results["dotw_all"]]

    data = {}
    for r in results["crmtyp_all"]:
        data[r["category"]] = r["count"]
    n_data = {
        "name": "Crime Type for All Data",
        "children": []
    }
    for k1 in data:
        t_d = {"name": k1, "count": data[k1]}
        n_data["children"].append(t_d)
    result["main"]["all"]["values_type"] = n_data

    data = {}
    for r in results["locdesc_all"]:
        if r["locdesc1"] not in data:
            data[r["locdesc1"]] = {}
        if r["locdesc2"] not in data[r["locdesc1"]]:
            data[r["locdesc1"]][r["locdesc2"]] = {}
        data[r["locdesc1"]][r["locdesc2"]][r["locdesc3"]] = r["count"]
    n_data = {
        "name": "Location Description for All Data",
        "children": []
    }
    for k1 in data:
        t_d = {"name": k1, "children": []}
        for k2 in data[k1]:
            t_e = {"name": "{} | {}".format(k1,k2), "children": []}
            for k3 in data[k1][k2]:
                t_e["children"].append({"name": "{} | {} | {}".format(k1,k2,k3), "count": data[k1][k2][k3]})
            t_d["children"].append(t_e)
        n_data["children"].append(t_d)
    result["main"]["all"]["values_locdesc"] = n_data

    if blockid != -1:
        result["main"][blockid] = {}
        result["main"][blockid]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"]} for c in results["date"]]
        result["main"][blockid]["values_time"] = sorted([{"x": c["hour"], "y": c["severity"]} for c in results["time"]], key=lambda x: x.get("x"))
        result["main"][blockid]["values_dow"] = [{"x": c["dow"], "y": c["severity"]} for c in results["dotw"]]

        data = {}
        for r in results["crmtyp"]:
            data[r["category"]] = r["count"]
        n_data = {
            "name": "Crime Type for All Data",
            "children": []
        }
        for k1 in data:
            t_d = {"name": k1, "count": data[k1]}
            n_data["children"].append(t_d)
        result["main"][blockid]["values_type"] = n_data

        data = {}
        for r in results["locdesc"]:
            if r["locdesc1"] not in data:
                data[r["locdesc1"]] = {}
            if r["locdesc2"] not in data[r["locdesc1"]]:
                data[r["locdesc1"]][r["locdesc2"]] = {}
            data[r["locdesc1"]][r["locdesc2"]][r["locdesc3"]] = r["count"]
        n_data = {
            "name": "Location Description for All Data",
            "children": []
        }
        for k1 in data:
            t_d = {"name": k1, "children": []}
            for k2 in data[k1]:
                t_e = {"name": "{} | {}".format(k1,k2), "children": []}
                for k3 in data[k1][k2]:
                    t_e["children"].append({"name": "{} | {} | {}".format(k1,k2,k3), "count": data[k1][k2][k3]})
                t_d["children"].append(t_e)
            n_data["children"].append(t_d)
        result["main"][blockid]["values_locdesc"] = n_data
    return Response(
        response=json.dumps(result),
        status=200,
        mimetype='application/json'
    )


if __name__ == "__main__":
    # Run server
    app.run(host='0.0.0.0', port=config(PORT), debug=True)
