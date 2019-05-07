from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from decouple import config
from geomet import wkb, wkt
import pandas as pd

import json
import datetime
import math
import io
import sys

from models import *


DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()

def get_download(config_dict, dotw, crimetypes, locdesc1, locdesc2, locdesc3):
    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE('{sdt}', 'MM/DD/YYYY') AND datetime <= TO_DATE('{edt}', 'MM/DD/YYYY')"
    query_year    = "incident.year = {cyear}"
    query_time    = "incident.hour >= {stime} AND hour <= {etime}"
    query_dotw    = "incident.dow = ANY({dotw})"
    query_crmtyp  = "crimetype.category = ANY({crimetypes})"
    query_locdesc = "(locdesctype.key1, locdesctype.key2, locdesctype.key3) = ANY({lockeys})"
    query_join    = "INNER JOIN crimetype ON incident.crimetypeid = crimetype.id INNER JOIN locdesctype ON incident.locdescid = locdesctype.id INNER JOIN city ON incident.cityid = city.id AND "

    base_list = [query_city, query_date, query_year, query_time]
    outputs   = ", ".join(["city.city", "city.state", "city.country", "incident.datetime", "ST_XMAX(incident.location) AS latitude", "ST_YMAX(incident.location) AS longitude", "crimetype.category", "locdesctype.key1 AS location_key1", "locdesctype.key2 AS location_key2", "locdesctype.key3 AS location_key3"])
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
        job = Job(result=f.getvalue())
        SESSION.add(job)
        SESSION.commit()
        return job.id

def get_data(config_dict, blockid, dotw, crimetypes, locdesc1, locdesc2, locdesc3):
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
        "date_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_date_end,
        "time_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]) + " GROUP BY " + q_time_end,
        "dotw_all": "SELECT SUM(crimetype.severity)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]) + " GROUP BY " + q_dotw_end,
        "crmtyp_all": "SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]) + " GROUP BY " + q_crmtyp_end,
        "locdesc_all": "SELECT COUNT(*), " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]) + " GROUP BY " + q_locdesc_end,
    }
    if blockid != -1:
        charts["date"] = "SELECT SUM(crimetype.severity)/AVG(block.population), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]+[query_block]) + " GROUP BY " + q_date_end
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
    all_times = [{"x": i, "y": 0.0} for i in range(24)]
    for c in results["time_all"]:
        all_times[c["hour"]]["y"] = c["severity"]
    all_times = [{"x": -1, "y": all_times[-1]["y"]}] + all_times + [{"x": 24, "y": all_times[0]["y"]}, {"x": 25, "y": all_times[1]["y"]}]
    result["main"]["all"]["values_time"] = all_times
    all_dows = [{"x": i, "y": 0.0} for i in range(7)]
    for c in results["dotw_all"]:
        all_dows[c["dow"]]["y"] = c["severity"]
    all_dows = [{"x": -1, "y": all_dows[-1]["y"]}] + all_dows + [{"x": 7, "y": all_dows[0]["y"]}]
    result["main"]["all"]["values_dow"] = all_dows

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
        times = [{"x": i, "y": 0.0} for i in range(23)]
        for c in results["time_all"]:
            times[c["hour"]]["y"] = c["severity"]
        times = [{"x": -1, "y": times[-1]["y"]}] + times + [{"x": 24, "y": times[0]["y"]}, {"x": 25, "y": times[1]["y"]}]
        result["main"][blockid]["values_time"] = times
        dows = [{"x": i, "y": 0.0} for i in range(7)]
        for c in results["dotw_all"]:
            dows[c["dow"]]["y"] = c["severity"]
        dows = [{"x": -1, "y": dows[-1]["y"]}] + dows + [{"x": 7, "y": dows[0]["y"]}]
        result["main"][blockid]["values_dow"] = dows

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
    job = Job(result=json.dumps(result))
    SESSION.add(job)
    SESSION.commit()
    return job.id
