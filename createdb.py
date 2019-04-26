"""Create DB tables and input rows for tables."""

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from decouple import config
from shapely import wkb, wkt
import pandas as pd
from models import *
import json
import datetime


# Connect to DB
DB_URI  = config('DB_URI')
ENGINE  = create_engine(DB_URI)
Session = sessionmaker(bind=ENGINE)
SESSION = Session()

# Bas serverity values
SEVERITY_HIGH = 1.0
SEVERITY_MEDIUM = 0.5
SEVERITY_LOW  = 0.2


def str_contains_any_substr(string, substrings):
    """Tests to see if any subtrings are contained within string.

    Args:
        string (str): string to search
        substrings (list(str)): list of substrings to search for in string
    Returns:
        (bool) if a substring is contained within string
    """
    for subs in substrings:
        if subs in string:
            return True
    return False


def crime_severity(x):
    """Turns description of crime into numerical crime severity.

    Args:
        x (list(str)): list of [primary type of crime, description of crime]
    Returns:
        (float) crime severity
    """
    primary_type = x[0]
    description  = x[1]
    if (primary_type == 'THEFT'):
        if (description == 'PURSE-SNATCHING') or \
            (description == 'POCKET-PICKING') or \
            (description == 'FROM_BUILDING'):
            return SEVERITY_MEDIUM
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'BATTERY'):
        if 'AGG' in description and \
            'DOMESTIC' not in description:
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'NARCOTICS'):
        if 'DEL' in description or \
            'CONSPIRACY' in description:
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'OTHER OFFENSE'):
        if str_contains_any_substr(
            description, ['GUN', 'SEX', 'VOILENT', 'PAROLE', 'ARSON']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'ASSAULT'):
        if str_contains_any_substr(
            description, ['AGG']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'ROBBERY'):
        if str_contains_any_substr(
            description, ['AGG', 'ARMED']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'BURGLARY'):
        if str_contains_any_substr(
            description, ['INVASION']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'CRIMINAL TRESPASS'):
        if str_contains_any_substr(
            description, ['RESIDENCE']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'MOTOR VEHICLE THEFT'):
        if str_contains_any_substr(
            description, ['AUTO']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'WEAPONS VIOLATION'):
        if str_contains_any_substr(
            description, ['USE', 'SALE']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'CONCEALED CARRY LICENSE VIOLATION'):
        if str_contains_any_substr(
            description, ['INFLUENCE']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'PUBLIC PEACE VIOLATION'):
        if str_contains_any_substr(
            description, ['RECKLESS', 'MOB', 'ARSON', 'BOMB']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_MEDIUM
        
    if (primary_type == 'INTERFERENCE WITH PUBLIC OFFICER'):
        if str_contains_any_substr(
            description, ['OBSTRUCT']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'STALKING'):
        if str_contains_any_substr(
            description, ['AGG']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'SEX OFFENSE'):
        if str_contains_any_substr(
            description, ['CRIM', 'CHILD', 'INDECEN']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
        
    if (primary_type == 'LIQUOR LAW VIOLATION'):
        if str_contains_any_substr(
            description, ['MINOR']):
            return SEVERITY_HIGH
        else:
            return SEVERITY_LOW
    
    if (primary_type == 'HOMICIDE') or \
        (primary_type == 'CRIM SEXUAL ASSAULT') or \
        (primary_type == 'ARSON') or \
        (primary_type == 'OFFENSE INVOLVING CHILDREN') or \
        (primary_type == 'PROSTITUTION') or \
        (primary_type == 'KIDNAPPING') or \
        (primary_type == 'HUMAN TRAFFICKING') or \
        (primary_type == 'NON-CRIMINAL (SUBJECT SPECIFIED)'):
        return SEVERITY_HIGH

    if (primary_type == 'INTIMIDATION') or \
        (primary_type == 'OTHER NARCOTIC VIOLATION') or \
        (primary_type == 'OBSCENITY') or \
        (primary_type == 'PUBLIC INDECENCY'):
        return SEVERITY_MEDIUM

    if (primary_type == 'DECEPTIVE PRACTICE') or \
        (primary_type == 'CRIMINAL DAMAGE') or \
        (primary_type == 'NON-CRIMINAL') or \
        (primary_type == 'GAMBLING'):
        return SEVERITY_LOW

    raise ValueError(f'Could not find severity for "{primary_type}" and "{description}"')


def reset_tables():
    """Reset DB tables."""
    BASE.metadata.drop_all(bind=ENGINE)
    BASE.metadata.create_all(bind=ENGINE)


def add_city_chicago():
    """Add Chicago into city table in DB.

    Returns:
        (int) city id of Chicago in city table
    """
    chicago = City(city="CHICAGO", state="ILLINOIS", country="UNITED STATES OF AMERICA", zipcode=60007, location="POINT(41.8781 -87.6298)")
    SESSION.add(chicago)
    SESSION.flush()
    SESSION.refresh(chicago)
    return chicago.id


def add_blocks_chicago(cityid):
    """Add blocks to block table, corresponding to cityid in city table.

    Args:
        cityid (int): cityid of exisiting city in city table
    """
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
    """Add crime types into crimetype table."""
    incidents = pd.read_csv("incidents.csv")
    incidents.loc[:,"crimefull"] = incidents[["Primary Type", "Description"]].apply(lambda x: " | ".join(x), axis=1)
    incidents = incidents.loc[:,["crimefull"]]
    incidents = incidents.drop_duplicates(subset="crimefull")
    incidents.loc[:,"severity"] = incidents["crimefull"].apply(lambda x: crime_severity(x.split(" | ")))
    SESSION.add_all([CrimeType(category=incidents.loc[i,"crimefull"], severity=incidents.loc[i,"severity"]) for i in incidents.index.values])
    SESSION.flush()


def add_incidents_chicago(cityid):
    """Add incidents into incident table, relating to cityid in city table.

    Args:
        cityid (int): id of city existing in city table
    """
    
    def find_blockid(x):
        geom = wkt.loads(x)
        for k in block_dict:
            if block_dict[k].contains(geom):
                return k
        return None

    def find_crimetypeid(x):
        for k in crimetype_dict:
            if crimetype_dict[k] == x:
                return k
        return None

    
    incidents = pd.read_csv("incidents.csv")
    incidents = incidents.loc[:,["Date", "Primary Type", "Description", "Latitude", "Longitude"]]
    incidents = incidents.dropna()
    incidents.loc[:,"crimefull"] = incidents[["Primary Type", "Description"]].apply(lambda x: " | ".join(x), axis=1)
    incidents.loc[:,"locfull"] = incidents[["Longitude", "Latitude"]].apply(lambda x: "POINT({})".format(" ".join([str(y) for y in x])), axis=1)
    crimetypes = SESSION.query(CrimeType).all()
    crimetype_dict = {}
    for c in crimetypes:
        crimetype_dict[c.id] = c.category
    blocks = SESSION.query(Blocks).all()
    block_dict = {}
    for b in blocks:
        block_dict[b.id] = wkb.loads(b.shape.data.tobytes())
    incidents.loc[:,"crimetypeid"] = incidents["crimefull"].apply(find_crimetypeid)
    incidents.loc[:,"blockid"] = incidents["locfull"].apply(find_blockid)
    SESSION.add_all([Incident(crimetypeid=incidents.loc[i,"crimetypeid"], cityid=cityid, blockid=incidents.loc[i,"blockid"], location=incidents.loc[i,"locfull"], datetime=datetime.datetime(incidents.loc[i,"Date"])) for i in incidents.index.values])
    SESSION.flush()


if __name__ == "__main__":
    # Run through all functions for creating info
    cityid = 1
    # reset_tables()
    # cityid = add_city_chicago()
    # add_blocks_chicago(cityid)
    # add_crimetypes()
    add_incidents_chicago(cityid)
    SESSION.commit()
