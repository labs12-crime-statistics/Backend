from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from decouple import config
import pandas as pd
from models import *
import json


DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()


def reset_tables():
    BASE.metadata.drop_all(bind=ENGINE)
    BASE.metadata.create_all(bind=ENGINE)


def add_city_chicago():
    chicago = City(city="CHICAGO", state="ILLINOIS", country="UNITED STATES OF AMERICA", zipcode=60007, location="POINT(41.8781 -87.6298)")
    SESSION.add(chicago)
    SESSION.flush()
    SESSION.refresh(chicago)
    return chicago.id


def add_blocks_chicago(cityid):
    with open("boundaries.json", "r") as fp:
        boundaries = json.load(fp)["features"]
    with open("boundaries_tracts.json", "r") as fp:
        boundaries_tracts = json.load(fp)["features"]
    pops = pd.read_csv("populations.csv")
    popu = {}
    for shape in boundaries:
        if shape["properties"]["tractce10"] not in popu:
            popu[shape["properties"]["tractce10"]] = 0
        try:
            popu[shape["properties"]["tractce10"]] += int(pops.loc[pops["CENSUS BLOCK FULL"]==int(shape["properties"]["geoid10"]),"TOTAL POPULATION"].values[0])
        except:
            pass
    shapes = []
    for shape in boundaries_tracts:
        str_poly = "MULTIPOLYGON("
        for i0 in shape["geometry"]["coordinates"]:
            str_poly += "("
            for ind, i1 in enumerate(i0):
                if ind > 0:
                    str_poly += ","    
                str_poly += "("+",".join(["{} {}".format(j[0], j[1]) for j in i1])+")"
            str_poly += ")"
        str_poly += ")"
        shapes.append(Blocks(cityid=cityid, shape=str_poly, population=popu[shape["properties"]["tractce10"]]))
    SESSION.add_all(shapes)
    SESSION.flush()


def add_crimetypes():
    with open("crimeseverity.csv", "r") as fp:
        crimes = fp.read().split("\n")
    crime_rows = []
    for crime in crimes:
        if crime != "":
            vals = crime.split(",")
            crime_rows.append(CrimeType(category=vals[0], severity=float(vals[1])))
    SESSION.add_all(crime_rows)
    SESSION.flush()
    d = {}
    for crime in crime_rows:
        d[crime.category] = crime.id
    return d


def add_incidents_chicago(cityid, crimevals):
    with open("incidents.csv", "r") as fp:
        incidents = fp.read().split("\n")
    incident_rows = []
    for crime in incidents:
        if crime != "":
            vals = crime.split(",")
            blockid = SESSION.query(Blocks.id).filter(func.ST_Contains(Blocks.shape, "POINT({} {})".format(vals[1], vals[2]))).one()
            print(blockid)
            incident_rows.append(Incident(crimetypeid=crimevals[vals[0]], cityid=cityid, blockid=blockid, location="POINT({} {})".format(vals[1], vals[2]), datetime=datetime.datetime(vals[3])))
    SESSION.add_all(incident_rows)
    SESSION.flush()


if __name__ == "__main__":
    reset_tables()
    cityid = add_city_chicago()
    add_blocks_chicago(cityid)
    # crimevals = add_crimetypes()
    # add_incidents_chicago(cityid, crimevals)
    SESSION.commit()
