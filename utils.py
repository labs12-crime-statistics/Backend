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
        job = Job(result=f.getvalue(), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id


def get_data(config_dict, blockid, dotw, crimetypes, locdesc1, locdesc2, locdesc3):
    query = "SELECT * FROM max_count;"
    severity = float(SESSION.execute(text(query)).fetchone()[0])
    vals_s = config_dict["sdt"].split("/")
    vals_e = config_dict["edt"].split("/")
    months_mult = 1.0 / (int(vals_e[0])+int(vals_e[2])*12-int(vals_s[0])-int(vals_e[2])*12+1)

    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE({sdt}, 'MM/DD/YYYY') AND incident.datetime <= TO_DATE({edt}, 'MM/DD/YYYY')"
    query_time    = "incident.hour >= {stime} AND incident.hour <= {etime}"
    query_block   = "incident.blockid = {blockid}"
    query_dotw    = "incident.dow = ANY({dotw})"
    query_crmtyp  = "crimetype.category = ANY({crimetypes})"
    query_locdesc = "ARRAY [locdesctype.key1, locdesctype.key2, locdesctype.key3] = ANY({lockeys})"
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

    maps = []
    date = []
    time = []
    dow  = []
    crimetype = []
    locdesc   = []

    funcs = {
        "date": lambda res: date.append({"severity": math.pow(mult_dow * mult_time * float(res['severity']) / severity, 0.1), "month": int(res['month']), "year": int(res['year']), "date": datetime.datetime.strptime("{:02d}/{}".format(int(res['month']),res['year']), '%m/%Y')}),
        "time": lambda res: time.append({"severity": math.pow(24 * mult_dow * months_mult * float(res['severity']) / severity, 0.1), "hour": int(res['hour'])}),
        "dotw": lambda res: dow.append({"severity": math.pow(7 * months_mult * mult_time * float(res['severity']) / severity, 0.1), "dow": int(res['dow'])}),
        "crmtyp": lambda res: crimetype.append({"count": res['count'], "category": r['category']}),
        "locdesc": lambda res: locdesc.append({"count": res['count'], "locdesc1": res['locdesc1'], "locdesc2": res['locdesc2'], "locdesc3": res['locdesc3']}),
    }

    charts = {
        "map": ("SELECT COUNT(*)/AVG(block.population), " + q_base_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_base_end).format(**config_dict),
        "date_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_date_end).format(**config_dict),
        "time_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]) + " GROUP BY " + q_time_end).format(**config_dict),
        "dotw_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]) + " GROUP BY " + q_dotw_end).format(**config_dict),
        "crmtyp_all": ("SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]) + " GROUP BY " + q_crmtyp_end).format(**config_dict),
        "locdesc_all": ("SELECT COUNT(*), " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]) + " GROUP BY " + q_locdesc_end).format(**config_dict),
    }
    if blockid != -1:
        charts["date"] = ("SELECT COUNT(*)/AVG(block.population), " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]+[query_block]) + " GROUP BY " + q_date_end).format(**config_dict)
        charts["time"] = ("SELECT COUNT(*)/AVG(block.population), " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]+[query_block]) + " GROUP BY " + q_time_end).format(**config_dict)
        charts["dotw"] = ("SELECT COUNT(*)/AVG(block.population), " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]+[query_block]) + " GROUP BY " + q_dotw_end).format(**config_dict)
        charts["crmtyp"] = ("SELECT COUNT(*), " + q_crmtyp_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crime"]+[query_block]) + " GROUP BY " + q_crmtyp_end).format(**config_dict)
        charts["locdesc"] = ("SELECT COUNT(*), " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]+[query_block]) + " GROUP BY " + q_locdesc_end).format(**config_dict)

    CONN = ENGINE.connect()

    if config_dict["loadtype"] == "dow":        
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_dow": [],
                }
            }
        }
        results = []
        
        all_dows = [{"x": i, "y": 0.0} for i in range(7)]
        pd.read_sql_query(charts["dotw_all"], CONN).apply(funcs["dotw"])
        for d in dow:
            all_dows[d["dow"]]["y"] = d["severity"]
        result["main"]["all"]["values_dow"] = [{"x": -1, "y": all_dows[-1]["y"]}] + all_dows + [{"x": 7, "y": all_dows[0]["y"]}]
        
        if blockid != -1:
            dow = []
            result["main"]["Block "+str(blockid)] = {}
            dows = [{"x": i, "y": 0.0} for i in range(7)]
            pd.read_sql_query(charts["dotw"], CONN).apply(funcs["dotw"])
            for d in dow:
                dows[c["dow"]]["y"] = c["severity"]
            result["main"]["Block "+str(blockid)]["values_dow"] = [{"x": -1, "y": dows[-1]["y"]}] + dows + [{"x": 7, "y": dows[0]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "time":
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_time": []
                }
            }
        }

        all_times = [{"x": i, "y": 0.0} for i in range(24)]
        pd.read_sql_query(charts["time_all"], CONN).apply(funcs["time"])
        for c in time:
            all_times[c["hour"]]["y"] = c["severity"]
        result["main"]["all"]["values_time"] = [{"x": -1, "y": all_times[-1]["y"]}] + all_times + [{"x": 24, "y": all_times[0]["y"]}, {"x": 25, "y": all_times[1]["y"]}]
        
        if blockid != -1:
            time = []
            result["main"]["Block "+str(blockid)] = {}
            times = [{"x": i, "y": 0.0} for i in range(24)]
            pd.read_sql_query(charts["time"], CONN).apply(funcs["time"])
            for c in time:
                times[c["hour"]]["y"] = c["severity"]
            result["main"]["Block "+str(blockid)]["values_time"] = [{"x": -1, "y": times[-1]["y"]}] + times + [{"x": 24, "y": times[0]["y"]}, {"x": 25, "y": times[1]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "crimeall":
        result = {
            "error": "none",
            "main": {
                "all": {}
            }
        }
        
        data = {}
        pd.read_sql_query(charts["crmtyp_all"], CONN).apply(funcs["crmtyp"])
        for r in crimetype:
            data[r["category"]] = r["count"]
        n_data = {
            "name": "Crime Type for All Data",
            "children": []
        }
        for k1 in data:
            t_d = {"name": k1, "count": data[k1]}
            n_data["children"].append(t_d)
        result["main"]["all"]["values_type"] = n_data

        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "crimeblock" and config_dict["blockid"] != "":
        result = {
            "error": "none",
            "main": {
                "Block "+str(blockid): {}
            }
        }
        data = {}
        pd.read_sql_query(charts["crmtyp"], CONN).apply(funcs["crmtyp"])
        for r in crimetype:
            data[r["category"]] = r["count"]
        n_data = {
            "name": "Crime Type for All Data",
            "children": []
        }
        for k1 in data:
            t_d = {"name": k1, "count": data[k1]}
            n_data["children"].append(t_d)
        result["main"]["Block "+str(blockid)]["values_type"] = n_data
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "locall":
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_locdesc": []
                }
            }
        }

        data = {}
        pd.read_sql_query(charts["locdesc_all"], CONN).apply(funcs["locdesc"])
        for r in locdesc:
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
                    t_e["children"].append({"name": "{} | {} | {}".format(k1,k2,k3), "count": data[k1][k2][k3], "alpha": 1.0})
                t_d["children"].append(t_e)
            n_data["children"].append(t_d)
        result["main"]["all"]["values_locdesc"] = n_data

        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "locblock" and config_dict["blockid"] != -1:
        result = {
            "error": "none",
            "main": {
                "Block "+str(config_dict["blockid"]): {}
            }
        }
        
        data = {}
        pd.read_sql_query(charts["locdesc"], CONN).apply(funcs["locdesc"])
        for r in locdesc:
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
                    t_e["children"].append({"name": "{} | {} | {}".format(k1,k2,k3), "count": data[k1][k2][k3], "alpha": 1.0})
                t_d["children"].append(t_e)
            n_data["children"].append(t_d)
        result["main"]["Block "+str(blockid)]["values_locdesc"] = n_data
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    elif config_dict["loadtype"] == "":
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_date": []
                }
            },
            "other": [],
            "timeline": []
        }
        
        map_df = pd.read_sql_query(charts["map"], CONN)
        map_df.loc[:,"severity"] = map_df["severity"].apply(lambda x: math.pow(mult_dow * mult_time * float(x) / severity, 0.1))
        map_cross = pd.crosstab(map_df["blockid"], [map_df["year"], map_df["month"]], values=map_df["severity"], aggfunc='sum').fillna(0.0)
        result["timeline"] = [{"year": c[0], "month": c[1]} for c in map_cross]
        for i in map_cross.index:
            result["other"].append({
                "id": i,
                "values": list(map_cross.loc[i,:].values)
            })

        pd.read_sql_query(charts["date_all"], CONN).apply(funcs["date"])
        result["main"]["all"]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"]} for c in sorted(date, key=lambda k: k['date'])]
        
        if blockid != -1:
            date = []
            result["main"]["Block "+str(blockid)] = {}
            pd.read_sql_query(charts["date"], CONN).apply(funcs["date"])
            result["main"]["Block "+str(blockid)]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"]} for c in sorted(funcs["date"](SESSION.execute(text(charts["date"]), config_dict).fetchall()), key=lambda k: k['date'])]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        return job.id
    raise Exception('INCORRECT FORMAT')