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


DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)


def get_shapes(cityid):
    SESSION = Session()
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
    result=json.dumps({
            "error": "none",
            "blocks": blocks,
            "zipcodes": zipcodes,
            "citylocation": citycoords})
    job = Job(result=result, datetime=datetime.datetime.utcnow())
    SESSION.add(job)
    SESSION.commit()
    JOB_ID = job.id
    SESSION.close()
    return JOB_ID


def get_tips(config_dict):
    SESSION = Session()
    
    query = f"""
        SELECT
            ENCODE(block.prediction::BYTEA, 'hex') AS predictions
        FROM block
        WHERE block.cityid = 1
            AND block.id = {config_dict['blockid']};
    """
    crime_future = SESSION.execute(text(query)).fetchone()[0]
    
    query = f"""
        SELECT
            COUNT(*)/AVG(block.population) AS crime_rate
        FROM incident
        INNER JOIN block ON incident.blockid = block.id
        INNER JOIN crimetype ON incident.crimetypeid = crimetype.id
            AND block.population > 0
            AND incident.cityid = 1
            AND incident.year = 2014
            AND block.id = {config_dict['blockid']};
    """
    crime_block_past = SESSION.execute(text(query)).fetchone()[0]

    query = f"""
        SELECT
            COUNT(*)/AVG(block.population) AS crime_rate
        FROM incident
        INNER JOIN block ON incident.blockid = block.id
        INNER JOIN crimetype ON incident.crimetypeid = crimetype.id
            AND block.population > 0
            AND incident.cityid = 1
            AND incident.year = 2018
            AND block.id = {config_dict['blockid']};
    """
    crime_block_curr = SESSION.execute(text(query)).fetchone()[0]
    
    query = """
        SELECT
            COUNT(*)/(
                SELECT SUM(block.population) AS city_population
                FROM block) AS crime_rate
        FROM incident
        INNER JOIN block ON incident.blockid = block.id
        INNER JOIN crimetype ON incident.crimetypeid = crimetype.id
            AND block.population > 0
            AND incident.cityid = 1
            AND incident.year = 2018;
    """
    crime_all_curr = SESSION.execute(text(query)).fetchone()[0]

    query = """
        SELECT STDDEV(count_block)
        FROM (
            SELECT
                COUNT(*)/AVG(block.population) AS count_block
            FROM incident
            INNER JOIN block ON incident.blockid = block.id
            INNER JOIN crimetype ON incident.crimetypeid = crimetype.id
                AND block.population > 0
                AND incident.cityid = 1
                AND incident.year = 2018
            GROUP BY
                incident.blockid
        ) AS count_all;
    """
    crime_all_std = SESSION.execute(text(query)).fetchone()[0]

    crime_future = np.frombuffer(bytes.fromhex(crime_future), dtype=np.float64).reshape((12,7,24))
    future_m = crime_future.sum((1,2))
    future_d = crime_future.sum((0,2))
    future_h = crime_future.sum((0,1))

    future_m = future_m - future_m.mean()
    future_d = future_d - future_d.mean()
    future_h = future_h - future_h.mean()

    crime_change_past = 0.2 * (crime_block_curr - crime_block_past) / crime_block_past
    crime_change_pred = (crime_block_future.sum() - crime_block_curr) / crime_block_curr
    std_val = (crime_block_curr - crime_all_curr) / crime_all_std

    result = {
        "changePast": crime_change_past,
        "changeFuture": crime_change_pred,
        "cityComp": std_val,
        "diffMonth": future_m.tolist(),
        "diffDow": future_d.tolist(),
        "diffHour": future_h.tolist()
    }
    
    job = Job(result=JSON.dumps(result), datetime=datetime.datetime.utcnow())
    SESSION.add(job)
    SESSION.commit()
    JOB_ID = job.id
    SESSION.close()
    return JOB_ID


def get_predictions(cityid):
    SESSION = Session()
    query = "SELECT * FROM max_count;"
    max_risk = float(SESSION.execute(text(query)).fetchone()[0])
    query = f"""SELECT id, ENCODE(prediction::BYTEA, 'hex') AS predict, month, year, population FROM block WHERE cityid = {cityid} AND prediction IS NOT NULL;"""
    prediction = {}
    all_dates = []
    block_date = {}
    population = {}
    with ENGINE.connect() as CONN:
        df = pd.read_sql_query(query, CONN)
    df.loc[:,"start"] = df.apply(lambda x: x["month"]+12*x["year"], axis=1)
    df.loc[:,"predict"] = df["predict"].apply(lambda x: np.frombuffer(bytes.fromhex(x), dtype=np.float64).reshape((12,7,24)))
    all_dates = list(range(df["start"].min(), df["start"].min()+12))
    predictions_n = {}
    predictionall = np.zeros((len(all_dates),7,24))
    for k in df.index:
        dift = df.loc[k,"start"]-all_dates[0]
        predictions_n[str(df.loc[k,"id"])] = np.zeros((len(all_dates),7,24))
        predictions_n[str(df.loc[k,"id"])][dift:12-dift,:,:] = df.loc[k,"predict"]
        predictionall += predictions_n[str(df.loc[k,"id"])] * df.loc[k,"population"]
        predictions_n[str(df.loc[k,"id"])] = predictions_n[str(df.loc[k,"id"])].tolist()
    all_dates_format = ["{}/{}".format(x%12+1,x//12) for x in all_dates]
    predictionall = (predictionall / float(df["population"].sum())).tolist()
    result = json.dumps({"error": "none", "predictionAll": predictionall, "allDatesFormatted": all_dates_format, "allDatesInt": all_dates, "prediction": predictions_n, "maxRisk": max_risk})
    job = Job(result=result, datetime=datetime.datetime.utcnow())
    SESSION.add(job)
    SESSION.commit()
    JOB_ID = job.id
    SESSION.close()
    return JOB_ID


def get_download(config_dict, dotw, crimeviolence, crimeppos, locgroups):
    SESSION = Session()
    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE('{sdt}', 'MM/DD/YYYY') AND datetime <= TO_DATE('{edt}', 'MM/DD/YYYY')"
    query_year    = "incident.year = {cyear}"
    query_time    = "incident.hour >= {stime} AND hour <= {etime}"
    query_dotw    = "incident.dow = ANY({dotw})"
    query_crmvio  = "crimetype.category = ANY({crimeviolence})"
    query_crmppo  = "crimetype.category = ANY({crimeppos})"
    query_locdesc = "locdesctype.locgroup = ANY({locgroups})"
    query_join    = "INNER JOIN crimetype ON incident.crimetypeid = crimetype.id INNER JOIN locdesctype ON incident.locdescid = locdesctype.id INNER JOIN city ON incident.cityid = city.id AND "

    base_list = [query_city, query_date, query_year, query_time]
    outputs   = ", ".join(["city.city", "city.state", "city.country", "incident.datetime", "ST_XMAX(incident.location) AS latitude", "ST_YMAX(incident.location) AS longitude", "crimetype.category", "locdesctype.key1 AS location_key1", "locdesctype.key2 AS location_key2", "locdesctype.key3 AS location_key3"])
    if dotw != "":
        config_dict["dotw"] = dotw.split(",")
        base_list.append(query_dotw)
    if crimeviolence != "":
        config_dict["crimeviolence"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in crimeviolence.split(',')]))
        base_list.append(query_crmvio)
    if crimeppos != "":
        config_dict["crimeppos"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in crimeppos.split(',')]))
        base_list.append(query_crmppo)
    if locgroups != "":
        config_dict["locgroups"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in locgroups.split(',')]))
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
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID


def get_data(config_dict, blockid, dotw, crimeviolence, crimeppos, locgroups):
    SESSION = Session()
    query = """SELECT COUNT(*) FROM (
        SELECT COUNT(*)
        FROM incident
        WHERE incident.datetime >= TO_DATE(:sdt, 'MM/DD/YYYY') AND incident.datetime <= TO_DATE(:edt, 'MM/DD/YYYY')
        GROUP BY incident.year, incident.month
    ) AS month_count;"""
    months_mult = 1.0 / SESSION.execute(text(query), {"sdt": config_dict["sdt"], "edt": config_dict["edt"]}).fetchone()[0]

    if config_dict["stime"] > config_dict["etime"]:
        config_dict["query_time_or"] = "OR"
    else:
        config_dict["query_time_or"] = "AND"

    query_base    = " FROM incident "
    query_city    = "incident.cityid = {cityid}"
    query_date    = "incident.datetime >= TO_DATE('{sdt}', 'MM/DD/YYYY') AND incident.datetime <= TO_DATE('{edt}', 'MM/DD/YYYY')"
    query_time    = "incident.hour >= {stime} {query_time_or} incident.hour <= {etime}"
    query_block   = "incident.blockid = {blockid}"
    query_dotw    = "incident.dow = ANY({dotw})"
    query_crmvio  = "crimetype.violence = ANY({crimeviolence})"
    query_crmppo  = "crimetype.ppo = ANY({crimeppos})"
    query_locdesc = "locdesctype.locgroup = ANY({locgroups})"
    query_pop     = "block.population > 0"
    query_join    = "INNER JOIN block ON incident.blockid = block.id INNER JOIN crimetype ON incident.crimetypeid = crimetype.id INNER JOIN locdesctype ON incident.locdescid = locdesctype.id AND "
    q_base_end    = "incident.blockid, incident.year, incident.month"
    q_date_end    = "incident.year, incident.month"
    q_time_end    = "incident.hour"
    q_dotw_end    = "incident.dow"
    q_crmvio_end  = "crimetype.violence"
    q_crmppo_end  = "crimetype.ppo"
    q_locdesc_end = "locdesctype.locgroup"
    mult_time = 24.0 / min(config_dict["etime"] - config_dict["stime"] + 1, 24)
    mult_dow = 1

    base_list = {"city": query_city, "date": query_date, "time": query_time, "pop": query_pop}
    if dotw != "":
        mult_dow = 7.0 / len(dotw.split(","))
        config_dict["dotw"] = "ARRAY[{}]".format(dotw)
        base_list["dow"] = query_dotw
    if crimeviolence != "":
        config_dict["crimeviolence"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in crimeviolence.split(',')]))
        base_list["crimevio"] = query_crmvio
    if crimeppos != "":
        config_dict["crimeppos"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in crimeppos.split(',')]))
        base_list["crimeppo"] = query_crmppo
    if locgroups != "":
        config_dict["locgroups"] = "ARRAY[{}]".format(",".join(["'{}'".format(x) for x in locgroups.split(',')]))
        base_list["locdesc"] = query_locdesc
    if blockid != -1:
        config_dict["blockid"] = blockid

    maps = []
    date = []
    time = []
    dow  = []
    crimevio = []
    crimeppo = []
    locdesc   = []

    funcs = {
        "date": lambda res: date.append({"severity": 1000.0 * mult_dow * mult_time * float(res['severity']), "month": int(res['month']), "year": int(res['year']), "date": datetime.datetime.strptime("{:02d}/{}".format(int(res['month']),int(res['year'])), '%m/%Y')}),
        "time": lambda res: time.append({"severity": 1000.0 * 24 * mult_dow * months_mult * float(res['severity']), "hour": int(res['hour'])}),
        "dotw": lambda res: dow.append({"severity": 1000.0 * 7 * months_mult * mult_time * float(res['severity']), "dow": int(res['dow'])}),
        "crmvio": lambda res: crimevio.append({"value": res['count'], "id": res['violence'], "label": (" ".join(res["violence"].split("_"))).title()}),
        "crmppo": lambda res: crimeppo.append({"value": res['count'], "id": res['ppo'], "label": (" ".join(res["ppo"].split("_"))).title()}),
        "locgroup": lambda res: locdesc.append({"value": res['count'], "id": res['locgroup'], "label": (" ".join(res["locgroup"].split("_"))).title()})
    }

    charts = {
        "map": ("SELECT COUNT(*)/AVG(block.population) AS severity, " + q_base_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_base_end).format(**config_dict),
        "date_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)) AS severity, " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]) + " GROUP BY " + q_date_end).format(**config_dict),
        "time_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)) AS severity, " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]) + " GROUP BY " + q_time_end).format(**config_dict),
        "dotw_all": ("SELECT COUNT(*)/(AVG(block.population)*COUNT(DISTINCT incident.blockid)) AS severity, " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]) + " GROUP BY " + q_dotw_end).format(**config_dict),
        "crmvio_all": ("SELECT COUNT(*) AS count, " + q_crmvio_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crimevio"]) + " GROUP BY " + q_crmvio_end).format(**config_dict),
        "crmppo_all": ("SELECT COUNT(*) AS count, " + q_crmppo_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crimeppo"]) + " GROUP BY " + q_crmppo_end).format(**config_dict),
        "locdesc_all": ("SELECT COUNT(*) AS count, " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]) + " GROUP BY " + q_locdesc_end).format(**config_dict),
    }
    if blockid != -1:
        charts["date"] = ("SELECT COUNT(*)/AVG(block.population) AS severity, " + q_date_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list]+[query_block]) + " GROUP BY " + q_date_end).format(**config_dict)
        charts["time"] = ("SELECT COUNT(*)/AVG(block.population) AS severity, " + q_time_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "time"]+[query_block]) + " GROUP BY " + q_time_end).format(**config_dict)
        charts["dotw"] = ("SELECT COUNT(*)/AVG(block.population) AS severity, " + q_dotw_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "dow"]+[query_block]) + " GROUP BY " + q_dotw_end).format(**config_dict)
        charts["crmvio"] = ("SELECT COUNT(*) AS count, " + q_crmvio_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crimevio"]+[query_block]) + " GROUP BY " + q_crmvio_end).format(**config_dict)
        charts["crmppo"] = ("SELECT COUNT(*) AS count, " + q_crmppo_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "crimeppo"]+[query_block]) + " GROUP BY " + q_crmppo_end).format(**config_dict)
        charts["locdesc"] = ("SELECT COUNT(*) AS count, " + q_locdesc_end + query_base + query_join + " AND ".join([base_list[k] for k in base_list if k != "locdesc"]+[query_block]) + " GROUP BY " + q_locdesc_end).format(**config_dict)

    CONN = ENGINE.connect()

    if config_dict["loadtype"] == "dowall":        
        result = {
            "error": "none",
            "main": {
                "all": {}
            }
        }
        
        all_dows = [{"x": i, "y": 0.0} for i in range(7)]
        pd.read_sql_query(charts["dotw_all"], CONN).apply(funcs["dotw"], axis=1)
        for d in dow:
            all_dows[d["dow"]]["y"] = d["severity"]
        result["main"]["all"]["values_dow"] = [{"x": -1, "y": all_dows[-1]["y"]}] + all_dows + [{"x": 7, "y": all_dows[0]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "dowblock" and blockid != "":
        result = {
            "error": "none",
            "main": {}
        }

        result["main"]["Block "+str(blockid)] = {}
        dows = [{"x": i, "y": 0.0} for i in range(7)]
        pd.read_sql_query(charts["dotw"], CONN).apply(funcs["dotw"], axis=1)
        for d in dow:
            dows[c["dow"]]["y"] = c["severity"]
        result["main"]["Block "+str(blockid)]["values_dow"] = [{"x": -1, "y": dows[-1]["y"]}] + dows + [{"x": 7, "y": dows[0]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "timeall":
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_time": []
                }
            }
        }

        all_times = [{"x": i, "y": 0.0} for i in range(24)]
        pd.read_sql_query(charts["time_all"], CONN).apply(funcs["time"], axis=1)
        for c in time:
            all_times[c["hour"]]["y"] = c["severity"]
        result["main"]["all"]["values_time"] = [{"x": -1, "y": all_times[-1]["y"]}] + all_times + [{"x": 24, "y": all_times[0]["y"]}, {"x": 25, "y": all_times[1]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "timeblock" and blockid != "":
        result = {
            "error": "none",
            "main": {}
        }

        result["main"]["Block "+str(blockid)] = {}
        times = [{"x": i, "y": 0.0} for i in range(24)]
        pd.read_sql_query(charts["time"], CONN).apply(funcs["time"], axis=1)
        for c in time:
            times[c["hour"]]["y"] = c["severity"]
        result["main"]["Block "+str(blockid)]["values_time"] = [{"x": -1, "y": times[-1]["y"]}] + times + [{"x": 24, "y": times[0]["y"]}, {"x": 25, "y": times[1]["y"]}]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "crimeppoall":
        result = {
            "error": "none",
            "main": {
                "all": {}
            }
        }

        pd.read_sql_query(charts["crmppo_all"], CONN).apply(funcs["crmppo"], axis=1)
        t = 0.0
        for c in range(len(crimeppo)):
            t += crimeppo[c]["value"]
        for c in range(len(crimeppo)):
            crimeppo[c]["value"] /= t / 100.0
        result["main"]["all"]["values_type"] = crimeppo
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "crimeppoblock" and config_dict["blockid"] != "":
        result = {
            "error": "none",
            "main": {
                "Block "+str(blockid): {}
            }
        }
        
        pd.read_sql_query(charts["crmppo"], CONN).apply(funcs["crmppo"], axis=1)
        t = 0.0
        for c in range(len(crimeppo)):
            t += crimeppo[c]["value"]
        for c in range(len(crimeppo)):
            crimeppo[c]["value"] /= t / 100.0
        result["main"]["Block "+str(blockid)]["values_type"] = crimeppo
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "crimevioall":
        result = {
            "error": "none",
            "main": {
                "all": {}
            }
        }

        pd.read_sql_query(charts["crmvio_all"], CONN).apply(funcs["crmvio"], axis=1)
        t = 0.0
        for c in range(len(crimevio)):
            t += crimevio[c]["value"]
        for c in range(len(crimevio)):
            crimevio[c]["value"] /= t / 100.0
        result["main"]["all"]["values_type"] = crimevio
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "crimevioblock" and config_dict["blockid"] != "":
        result = {
            "error": "none",
            "main": {
                "Block "+str(blockid): {}
            }
        }
        
        pd.read_sql_query(charts["crmvio"], CONN).apply(funcs["crmvio"], axis=1)
        t = 0.0
        for c in range(len(crimevio)):
            t += crimevio[c]["value"]
        for c in range(len(crimevio)):
            crimevio[c]["value"] /= t / 100.0
        result["main"]["Block "+str(blockid)]["values_type"] = crimevio
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "locall":
        result = {
            "error": "none",
            "main": {
                "all": {
                    "values_locdesc": []
                }
            }
        }

        pd.read_sql_query(charts["locdesc_all"], CONN).apply(funcs["locgroup"], axis=1)
        t = 0.0
        for c in range(len(locdesc)):
            t += locdesc[c]["value"]
        for c in range(len(locdesc)):
            locdesc[c]["value"] /= t / 100.0
        result["main"]["all"]["values_locdesc"] = locdesc
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "locblock" and config_dict["blockid"] != -1:
        result = {
            "error": "none",
            "main": {
                "Block "+str(config_dict["blockid"]): {}
            }
        }
        
        pd.read_sql_query(charts["locdesc"], CONN).apply(funcs["locgroup"], axis=1)
        t = 0.0
        for c in range(len(locdesc)):
            t += locdesc[c]["value"]
        for c in range(len(locdesc)):
            locdesc[c]["value"] /= t / 100.0
        result["main"]["Block "+str(blockid)]["values_locdesc"] = locdesc
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "map":
        result = {
            "error": "none",
            "other": [],
            "timeline": []
        }

        sev = SESSION.execute(text("SELECT * FROM max_count;")).fetchone()[0]
        
        map_df = pd.read_sql_query(charts["map"], CONN)
        map_df.loc[:,"severity"] = map_df["severity"].apply(lambda x: (mult_dow * mult_time * float(x) / float(sev))**0.1)
        map_cross = pd.crosstab(map_df["blockid"], [map_df["year"], map_df["month"]], values=map_df["severity"], aggfunc='sum').fillna(0.0)
        result["timeline"] = [{"year": c[0], "month": c[1]} for c in map_cross]
        for i in map_cross.index:
            result["other"].append({
                "id": i,
                "values": list(map_cross.loc[i,:].values)
            })
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "dateall":
        result = {
            "error": "none",
            "main": {
                "all": {}
            }
        }

        pd.read_sql_query(charts["date_all"], CONN).apply(funcs["date"], axis=1)
        result["main"]["all"]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"], "style": {"strokeDasharray": "12, 6", "strokeWidth": 2}} for c in sorted(date, key=lambda k: k['date'])]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    elif config_dict["loadtype"] == "date" and blockid != -1:
        result = {
            "error": "none",
            "main": {}
        }

        result["main"]["Block "+str(blockid)] = {}
        pd.read_sql_query(charts["date"], CONN).apply(funcs["date"], axis=1)
        result["main"]["Block "+str(blockid)]["values_date"] = [{"x": "{}/{}".format(c["month"], c["year"]), "y": c["severity"], "style": {"strokeDasharray": "12, 6", "strokeWidth": 2}} for c in sorted(date, key=lambda k: k['date'])]
        job = Job(result=json.dumps(result), datetime=datetime.datetime.utcnow())
        SESSION.add(job)
        SESSION.commit()
        JOB_ID = job.id
        SESSION.close()
        return JOB_ID
    return "ERROR"