#
# Synpuf data reader
#
# This script reads patient information from a PostgreSQL database located
# at synpuf.c6pyjs3halpn.us-west-2.rds.amazonaws.com.
#
# The main function is read_synpuf_data
#
import psycopg2
import pandas
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import itertools
import pprint
import csv
import datetime

concepts = {}
cur = None

def getConcept(n,db_prefix="synpuf5."):
    global concepts
    global cur
    if n==None:
        return None
    try:
        return concepts[n][1]
    except KeyError:
        query_string = "select * from " + db_prefix + "concept where concept_id="+str(n)
        cur.execute(query_string)
        rows = cur.fetchall()
        if (len(rows)<1):
            return None
        concepts[n] = rows[0]
        return concepts[n][1]

locations = {}

def init_locations(db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix +"location"
    cur.execute(query_string)
    rows = cur.fetchall()
    for r in rows:
        locations[r[0]] = r

def getLocation(n, db_prefix="synpuf5."):
    global concepts
    global cur
    try:
        return locations[n]
    except KeyError:
        query_string = "select * from " + db_prefix + "location where location_id="+str(n)
        cur.execute(query_string)
        rows = cur.fetchall()
        locations[n] = rows[0]
        return locations[n]


def collect_patients(cur,npatients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "person"
    cur.execute(query_string)
    rows = cur.fetchall()

    patients = {}

    for x in rows:
        if x[0] <= npatients:
            patient = {}
            patient["sex"] = getConcept(x[1], db_prefix)
            patient["dob"] = datetime.date(x[2],x[3],x[4])
            patient["race"] = getConcept(x[6], db_prefix)
            patient["died"] = "No"
            patient["ethnicity"] = getConcept(x[7], db_prefix)
            l = getLocation(x[8])
            patient["State"] = l[4]
            patient["Zip"] = l[6]
            patients[x[0]] = patient
            patient['events'] = {}

    return patients

def add_death_records(cur,patients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "death"
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        try:
            patient = patients[r[0]]
            patient["died"] = "Yes"
            patient["dod"] = r[1]
            patient["death_cause"] = getConcept(r[2], db_prefix)
        except KeyError:
            pass

def add_procedure_records(cur,patients,limit, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "procedure_occurrence where person_id <= "+str(limit)
    cur.execute(query_string)

    rows = cur.fetchall()
    for x in patients.keys():
        patients[x]["events"] = {}
    
    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"procedure",
                           "procedure":getConcept(r[2], db_prefix),
                           "procedure_type":getConcept(r[4], db_prefix),
                           "modifier":getConcept(r[5], db_prefix),
                           "quantity":r[6],
                           "provider":r[7],
                           "visit":r[8],
                           "procedure_source_value":r[9],
                           "procedure_source":getConcept(r[10], db_prefix),
                           "qualifier_source_value":r[11] })
            patient["events"][r[3]] = events
        except KeyError:
            pass

def add_visit_records(cur,patients,npatients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "visit_occurrence where person_id <= "+str(npatients)
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            try:
                events = patient["events"][r[5]]
            except KeyError:
                events = []
            events.append({"type":"visit_end",
                           "id":r[0],
                           "visit":getConcept(r[2], db_prefix),
                           "visit_type":getConcept(r[7], db_prefix),
                           "provider":r[8],
                           "care_site":r[9],
                           "visit_source_value":r[10],
                           "visit_source_concept":getConcept(r[11], db_prefix) })
            patient["events"][r[5]] = events
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"visit_start",
                           "id":r[0],
                           "visit":getConcept(r[2], db_prefix),
                           "visit_type":getConcept(r[7], db_prefix),
                           "provider":r[8],
                           "care_site":r[9],
                           "visit_source_value":r[10],
                           "visit_source_concept":getConcept(r[11], db_prefix) })
            patient["events"][r[3]] = events
        except KeyError:
            pass

def add_device_exposures(cur,patients,npatients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "device_exposure where person_id <= "+str(npatients)
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"device_start",
                           "device":getConcept(r[2], db_prefix),
                           "device_type":getConcept(r[5], db_prefix),
                           "unique_device_id":r[6],
                           "quantity":r[7],
                           "provider_id":r[8],
                           "visit_occurrence_id":r[9],
                           "device_source_value":r[10],
                           "device_source":getConcept(r[11], db_prefix) })
            patient["events"][r[3]] = events
            try:
                events = patient["events"][r[4]]
            except KeyError:
                events = []
            events.append({"type":"device_end",
                           "device":getConcept(r[2], db_prefix),
                           "device_type":getConcept(r[5], db_prefix),
                           "unique_device_id":r[6],
                           "quantity":r[7],
                           "provider_id":r[8],
                           "visit_occurrence_id":r[9],
                           "device_source_value":r[10],
                           "device_source":getConcept(r[11], db_prefix) })
            patient["events"][r[4]] = events

        except KeyError:
            pass

def add_condition_occurrences(cur,patients,npatients,db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "condition_occurrence where person_id <= "+str(npatients)
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"condition_start",
                           "condition":getConcept(r[2], db_prefix),
                           "condition_type":getConcept(r[5], db_prefix),
                           "stop_reason":r[6],
                           "provider_id":r[7],
                           "visit_occurrence_id":r[8],
                           "condition_source_value":r[9],
                           "condition_source":getConcept(r[10], db_prefix) })
            patient["events"][r[3]] = events
            try:
                events = patient["events"][r[4]]
            except KeyError:
                events = []
            events.append({"type":"condition_end",
                           "condition":getConcept(r[2], db_prefix),
                           "condition_type":getConcept(r[5], db_prefix),
                           "stop_reason":r[6],
                           "provider_id":r[7],
                           "visit_occurrence_id":r[8],
                           "device_source_value":r[9],
                           "device_source":getConcept(r[10], db_prefix) })
            patient["events"][r[4]] = events
        except KeyError:
            pass

def add_measurements(cur,patients,npatients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "measurement where person_id <= "+str(npatients)
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"measurement",
                           "measurement":getConcept(r[2], db_prefix),
                           "measurement_type":getConcept(r[5], db_prefix),
                           "operator":getConcept(r[6], db_prefix),
                           "value_as_number":r[7],
                           "value":getConcept(r[8], db_prefix),
                           "unit":getConcept(r[9], db_prefix),
                           "range_low":r[10],
                           "range_high":r[11],
                           "provider_id":r[12],
                           "visit_occurrence_id":r[13],
                           "measurement_source_value":r[14],
                           "measurement_source":getConcept(r[15], db_prefix),
                           "unit_source_value":r[16],
                           "value_source_value":r[17] })
            patient["events"][r[3]] = events
        except KeyError:
            pass

def add_drug_exposures(cur,patients,npatients, db_prefix="synpuf5."):
    query_string = "select * from " + db_prefix + "drug_exposure where person_id <= "+str(npatients)
    cur.execute(query_string)
    rows = cur.fetchall()

    for r in rows:
        #print r[1]
        try:
            patient = patients[r[1]]
            print r
            if r[4] != None:
                try:
                    events = patient["events"][r[4]]
                except KeyError:
                    events = []
                events.append({"type":"drug_end",
                               "id":r[0],
                               "drug":getConcept(r[2], db_prefix),
                               "drug_type":getConcept(r[5], db_prefix),
                               "stop_reason":r[6],
                               "refills":r[7],
                               "quantity":r[8],
                               "days_supply":r[9],
                               "sig":r[10],
                               "route":getConcept(r[11], db_prefix),
                               "effective_drug_dose":r[12],
                               "dose_unit":getConcept(r[13], db_prefix),
                               "lot_number":r[14],
                               "provider_id":r[15],
                               "visit_occurrence_id":r[16],
                               "drug_source_value":r[17],
                               "drug_source":getConcept(r[18], db_prefix),
                               "route_source_value":r[19],
                               "route_source":getConcept(r[20], db_prefix) })
                patient["events"][r[4]] = events
            try:
                events = patient["events"][r[3]]
            except KeyError:
                events = []
            events.append({"type":"drug_start",
                           "id":r[0],
                           "drug":getConcept(r[2], db_prefix),
                           "drug_type":getConcept(r[5], db_prefix),
                           "stop_reason":r[6],
                           "refills":r[7],
                           "quantity":r[8],
                           "days_supply":r[9],
                           "sig":r[10],
                           "route":getConcept(r[11], db_prefix),
                           "effective_drug_dose":r[12],
                           "dose_unit":getConcept(r[13], db_prefix),
                           "lot_number":r[14],
                           "provider_id":r[15],
                           "visit_occurrence_id":r[16],
                           "drug_source_value":r[17],
                           "drug_source":getConcept(r[18], db_prefix),
                           "route_source_value":r[19],
                           "route_source":getConcept(r[20], db_prefix) })
            patient["events"][r[3]] = events
        except KeyError:
            pass

def read_omop_data(npatients=1,dbname='ohdsi', user='synpuf', host='synpuf.c6pyjs3halpn.us-west-2.rds.amazonaws.com', password='...', db_prefix='synpuf5.'):

    global cur
    # Open the database
    try:
        conn = psycopg2.connect( "dbname='"+dbname+"' user='"+user+"' host='"+host+"' password='"+password+"' ")
        cur = conn.cursor()
        cur.execute("SET search_path TO ohdsi")
    except:
        print("Can not connect to the data base")
        return None
    init_locations()

    print("Collecting patients...")
    patients = collect_patients(cur,npatients,db_prefix)
    print("Collecting death records...")
    add_death_records(cur,patients,db_prefix)
    print("Collecting procedure records...")
    add_procedure_records(cur,patients,npatients,db_prefix)
    print("Collecting visit records...")
    add_visit_records(cur,patients,npatients,db_prefix)
    print("Collecting device exposures...")
    add_device_exposures(cur,patients,npatients,db_prefix)
    print("Collecting condition occurrences...")
    add_condition_occurrences(cur,patients,npatients,db_prefix)
    print("Collecting measurements...")
    add_measurements(cur,patients,npatients,db_prefix)
    print("Collecting medications...")
    add_drug_exposures(cur,patients,npatients,db_prefix)

    return patients

