#
# generate.py
#
# This script generates a profile given a python data structure created by
# one of the readers
#
# The main function is read_synpuf_data
#
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
import json
import math

def patient_has_condition(patient,conditions,condition_map={}):
    for e in patient["events"].keys():
        for d in patient["events"][e]:
            if d["type"]=="condition_start":
                try:
                    condition = condition_map[d["condition"]]
                except KeyError:
                    condition = d["condition"]
                if condition in conditions:
                    return True
    return False

def patient_had_procedure(patient,procedures,procedure_map={}):
    for e in patient["events"].keys():
        for d in patient["events"][e]:
            if d["type"]=="procedure":
                try:
                    procedure = procedure_map[d["procedure"]]
                except KeyError:
                    procedure = d["procedure"]
                if procedure in procedures:
                    return True
    return False

def patient_had_medication(patient,medications,medication_map={}):
    for e in patient["events"].keys():
        for d in patient["events"][e]:
            if d["type"]=="drug_start":
                try:
                    condition = medication_map[d["drug"]]
                except KeyError:
                    condition = d["drug"]
                if condition in medications:
                    return True
    return False

def filter_patients(patients,conditions={},procedures={},medications={},condition_map={},procedure_map={},medication_map={}):
    res = {}
    for p in patients.keys():
        patient = patients[p]
        if conditions=={}:
            condition_satisfied = True
        else:
            condition_satisfied = patient_has_condition(patient,conditions,condition_map)
            
        if procedures=={}:
            procedure_satisfied = True
        else:
            procedure_satisfied = patient_had_procedure(patient,procedures,procedure_map)

        if medications=={}:
            medication_satisfied = True
        else:
            medication_satisfied = patient_had_medication(patient,medications,medication_map)
            
        if condition_satisfied and procedure_satisfied and medication_satisfied:
            res[p] = patient

    return res

def tabulate_conditions(patients,condition_map={}):
    raw_conditions = {}
    for p in patients.keys():
        patient = patients[p]
        for e in patient["events"].keys():
            for d in patient["events"][e]:
                if d["type"]=="condition_start":
                    try:
                        raw_conditions[d["condition"]]=raw_conditions[d["condition"]]+1
                    except KeyError:
                        raw_conditions[d["condition"]]=1

    #print(raw_conditions.keys())
    #print(len(raw_conditions.keys()))

    conditions = {}

    for c in raw_conditions.keys():
        try:
            cc = condition_map[c]
        except KeyError:
            cc = c
        try:
            conditions[cc] = conditions[cc]+raw_conditions[cc]
        except KeyError:
            conditions[cc] = raw_conditions[cc]

    x = conditions.keys()
    x.sort(key=lambda q: 10000-conditions[q])

    for z in x:
        print("%d: %s")%(conditions[z],z)

    return conditions

def tabulate_procedures(patients,procedure_map={}):
    #print patients.keys()
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(patients[patients.keys()[0]])

    raw_procedures = {}

    for p in patients.keys():
        patient = patients[p]
        for e in patient["events"].keys():
            for d in patient["events"][e]:
                if d["type"]=="procedure":
                    try:
                        raw_procedures[d["procedure"]]=raw_procedures[d["procedure"]]+1
                    except KeyError:
                        raw_procedures[d["procedure"]]=1

    procedures = {}

    for c in raw_procedures.keys():
        try:
            cc = procedure_map[c]
        except KeyError:
            cc = c
        try:
            procedures[cc] = procedures[cc]+raw_procedures[cc]
        except KeyError:
            procedures[cc] = raw_procedures[cc]

    x = procedures.keys()
    x.sort(key=lambda q: 10000-procedures[q])

    for z in x:
        print("%d: %s")%(procedures[z],z)
    return procedures

def tabulate_measurements(patients,measurement_map={}):
    measurements = {}
    for p in patients.keys():
        patient = patients[p]
        m = {}
        count = 0
        min = {}
        max = {}
        sum = {}
        count = {}
        for e in patient["events"].keys():
            for d in patient["events"][e]:
                if d["type"]=="measurement":
                    #print(d)
                    measurement = d["measurement"]
                    try:
                        measurement = measurement_map[measurement]
                    except KeyError:
                        pass
                    measurements[measurement] = "Yes"
                    value = d["value"]
                    value_as_number = d["value_as_number"]
                    try:
                        c = count[measurement]
                        s = sum[measurement]
                        mi = min[measurement]
                        ma = max[measurement]
                    except KeyError:
                        c = 0
                        s = 0
                        mi = None
                        ma = None
                    if value_as_number != None:
                        if mi==None or mi > value_as_number:
                            mi = value_as_number
                        if ma==None or ma < value_as_number:
                            ma = value_as_number
                        c = c+1
                        s = s+value_as_number
                    min[measurement] = mi
                    max[measurement] = ma
                    count[measurement] = c
                    sum[measurement] = s
                    #unit = d["unit"]
                    #print "%s,%s"%(str(value),str(value_as_number))
                    #range_low = d["range_low"]
                    #range_high = d["range_high"]    
        for k in count.keys():
            if count[k] > 0:
                 m[k] = { "sum":sum[k], "count":count[k], "mean":sum[k]/count[k], "min":min[k], "max":max[k] }
        patient["measurements"] = m
        print p
        print m
    return measurements

def tabulate_medications(patients,medication_map={}):
    raw_medications = {}

    for p in patients.keys():
        patient = patients[p]
        for e in patient["events"].keys():
            for d in patient["events"][e]:
                if d["type"]=="drug_start":
                    try:
                        raw_medications[d["drug"]]=raw_medications[d["drug"]]+1
                    except KeyError:
                        raw_medications[d["drug"]]=1

    medications = {}

    for c in raw_medications.keys():
        try:
            cc = medication_map[c]
        except KeyError:
            cc = c
        try:
            medications[cc] = medications[cc]+raw_medications[cc]
        except KeyError:
            medications[cc] = raw_medications[cc]

    x = medications.keys()
    x.sort(key=lambda q: 10000-medications[q])

    for z in x:
        print("%d: %s")%(medications[z],z)
    return medications

def tabulate_attribute(patients,attr):
    tabulation = {}
    for p in patients.keys():
        patient = patients[p]

        try:
            tabulation[attr] = tabulation[attr]+1
        except KeyError:
            tabulation[attr] = 1
    print attr
    print(len(tabulation.keys()))
    for l in tabulation.keys():
        print("    %s %d"%(l,tabulation[l]))
    return tabulation

def tabulate_characteristics(patients):
    return [tabulate_attribute(patients,"sex"),
            tabulate_attribute(patients,"race"),
            tabulate_attribute(patients,"died"),
            tabulate_attribute(patients,"ethnicity"),
            tabulate_attribute(patients,"State"),
            tabulate_attribute(patients,"Zip")]

def build_header(procedures,measurements,medications,conditions,include_procedure=[],include_measurement=[],include_medication=[],include_diagnosis=[]):
    header = ["sex","dob","race","ethnicity","state","zip","died"]

    #prr = procedure_tabulation.keys()
    #prr.sort(key=lambda x:10000-procedure_tabulation[x])

    #if len(prr)>30:
    #    prr = prr[0:30]
    for p in procedures.keys():
        if include_procedure==[] or p in include_procedure:
            header.append("procedure_"+p)

    for l in measurements.keys():
        if include_measurement==[] or l in include_measurement:
            header.append("lab_"+str(l))

    for p in medications.keys():
        if include_medication==[] or p in include_medication:
            header.append("prescription_"+str(p))

    for c in conditions.keys():
        if include_diagnosis==[] or c in include_diagnosis:
            header.append("condition_"+str(c))
    return header

def build_dataFrame(patients,procedures,measurements,medications,conditions,include_procedure=[],include_measurement=[],include_medication=[],include_diagnosis=[],procedure_map={},medication_map={},condition_map={}):
    rows = []
    for p in patients.keys():
        r = patients[p]
        row = {"dob":r["dob"],"sex":r["sex"],"race":r["race"],"ethnicity":r["ethnicity"],"state":r["State"],"died":r["died"],"zip":r["Zip"] }
        print(p)
        for p in procedures.keys():
            if include_procedure==[] or p in include_procedure:
                if patient_had_procedure(r,[p],procedure_map):
                    row["procedure_"+p] = "Yes"
                else:
                    row["procedure_"+p] = "No"
        for m in medications.keys():
            if include_medication==[] or m in include_medication:
                if patient_had_medication(r,[m],medication_map):
                    row["prescription_"+m] = "Yes"
                else:
                    row["prescription_"+m] = "No"
        for c in conditions.keys():
            if include_diagnosis==[] or c in include_diagnosis:
                if patient_has_condition(r,[c],condition_map):
                    row["condition_"+c] = "Yes"
                else:
                    row["condition_"+c] = "No"
        for m in measurements.keys():
            if include_measurement==[] or m in include_measurement:
                try:
                    x = r["measurements"][m]["mean"]
                    print m
                    print x
                    row["lab_"+m] = r["measurements"][m]["mean"]
                except KeyError:
                    row["lab_"+m] = None
        
        rows.append(row)
    df = pandas.DataFrame(rows)
    return df

def allNone(x):
    for r in rows:
        if r[x]!=None and r[x]!="None":
            return False
    return True

def build_features(header,outcomes=["died"],profile_directives={}):
    features = {}

    features1 = []
    features2 = []

    for x in header:
        if x=="age":
            features2.append("age")
            features["age"] = { "TYPE":"INT", "NAME":"age", "VALUE_CUT":[1,5,10,20,30,40,50,60,70,80,90] }
        elif len(x)>4 and x[0:4]=="lab_":
            try:
                p = profile_directives[x]
            except KeyError:
                features[x] = { "TYPE":"INT", "NAME":x, "STD_CUT":[] }
        else:
            #print "Feature"
            #print x
            if len(x)>8 and x[0:9]=="procedure":
                features1.append(x)
            #elif len(x)>8 and x[0:18]=="prescriptionDelta_":
            #    features1.append(x)
            elif x[0:4]=="lab_" or x[0:9]=="labDelta_":
                if x[4:] in filtered_labs or x[9:] in filtered_labs:
                    features2.append(x)
            features[x] = { "TYPE":"ENUM", "NAME":x }

    for o in outcomes:
        for x in header:
            if x != o:
                features[o+"_x_"+x] = { "TYPE":"CROSSTAB", "CONSTITUENTS":[o,x] }
    return features

def allBins(l):
    lab = labInfo[l]
    bins = []
    for b in lab["bins"]:
        bins.append(b)
    if lab["type"]!="bins":
        try:
            x = custom_bins[l]
            q = len(l)
            z = 0
            while z < q+1:
                b.append("_bin"+str(z))
                z = z+1
        except KeyError:
            b.append("_sdm2")
            b.append("_sdm1")
            b.append("_sdmh")
            b.append("_mid")
            b.append("_sdph")
            b.append("_sdp1")
            b.append("_sdp2")

def getBin(l,value,valueNum):
    if valueNum==None:
        try:
            value = value_mappings[l][value]
            valueNum = value
        except KeyError:
            pass
    if valueNum==None:
        return value
    else:
        try:
            x = custom_bins[l]
            i = 0
            while i < len(x) and valueNum < x[i]:
                i = i+1
            return "_bin"+str(i)
        except KeyError:
            mean = labInfo[l]["mean"]
            stdev = labInfo[l]["stdev"]
            if stdev==None:
                return value
            elif mean==None:
                return value
            elif valueNum < mean - stdev*2:
                return "_sdm2"
            elif valueNum < mean - stdev:
                return "_stm1"
            elif valueNum < mean - stdev*0.5:
                return "_stmh"
            elif valueNum < mean + stdev*0.5:
                return "_mid"
            elif valueNum < mean + stdev:
                return "_sdph"
            elif valueNum < mean + stdev*2:
                return "_sdp1"
            else:
                return "_stp2"

def get_crosstab_bins(profile):
    #print("get_crosstab_bins")
    #print(profile)
    try:
        return profile["fields"]
    except KeyError:
        pass
    try:
        x = profile["valueBins"]
        x.insert(0,profile["min"])
        return x
    except KeyError:
        pass
    try:
        x = profile["percentCutValues"]
        x.insert(0,profile["min"])
        return x
    except KeyError:
        pass
    try:
        x = profile["stdCutValues"]
        x.insert(0,profile["min"])
        return x
    except KeyError:
        pass

    return []

def get_bin_number(profile, value):
    if value==None:
        return -1
    try:
        f = profile["fields"]
        n = 0
        for x in f:
            if str(value)==str(x):
                return n
            n = n+1
        return -1
    except KeyError:
        pass
    try:
        x = profile["valueBins"]
        n = 0
        try:
            value = float(value)
        except ValueError:
            value = 0
        if math.isnan(value):
            return -1
        while n < len(x) and float(value) >= float(x[n]):
            n = n+1
        return n
    except KeyError:
        pass
    try:
        x = profile["percentCutValues"]
        n = 0
        while n < len(x) and value >= x[n]:
            n = n+1
        return n
    except KeyError:
        pass
    try:
        x = profile["stdCutValues"]
        n = 0
        try:
            value = float(value)
        except ValueError:
            value = 0
        if math.isnan(value):
            return -1
        while n < len(x) and value >= x[n]:
            n = n+1
        return n
    except KeyError:
        pass

    return -1

def get_first_crosstab_index(binlist):
    x = []
    for q in binlist:
        x.append(0)
    return x

def get_next_crosstab_index(binlist,ind):
    n = 0
    while (n < len(binlist)):
        l = ind[n]
        l = l+1
        ind[n] = l
        if l==len(binlist[n]):
            l = 0
            ind[n] = l
        else:
            return ind
        n = n+1
    return ind

def all_zeros(ind):
    for i in ind:
        if i > 0:
            return False
    return True

def get_crosstab_value(matrix, ind):
    for x in ind:
        if x < 0:
            return 0
        if len(matrix) <= x:
            return 0
        matrix = matrix[x]
    if matrix==[]:
        return 0
    return matrix

def set_crosstab_value(matrix, ind, value):
    if ind==[]:
        return value
    if len(ind) > 1:
        x = matrix
        while len(x) <= ind[0]:
            x.append([])
        x[ind[0]] = set_crosstab_value(x[ind[0]], ind[1:], value)
        return x
    else:
        x = matrix
        while len(x) <= ind[0]:
            x.append(0)
        x[ind[0]] = set_crosstab_value(x[ind[0]], ind[1:], value)
        return x

def count_data(d):
    c = 0
    for x in d.keys():
        if not(math.isnan(d[x])):
            c = c+1
    return c

def process_pandas_object(f, features):
    d = f.to_dict()
    profile = {}
    context = { #"version":"1.0",
                "schema":"http://clinicalProfile.ncats.io/",
                "fields":{"@id":"schema:enumValues", "@container":"@list"},
                "counts":{"@id":"schema:binCountValue", "@container":"@list"},
                "valueBins":{"@id":"schema:valueBin", "@container":"@list"},
                "valueBinCounts":{"@id":"schema:valueBinCount", "@container":"@list"},
                "percentCutCounts":{"@id":"schema:percentCutCount", "@container":"@list"},
                "percentCutValues":{"@id":"schema:percentCutValue", "@container":"@list"},
                "percentCuts":{"@id":"schema:percentCut", "@container":"@list"},
                "constituents":{"@id":"schema:field", "@container":"@list"},
                "type":"schema:fieldType",
                "min":"schema:minValue",
                "max":"schema:maxValue",
                "count":"schema:countValue",
                "mean":"schema:meanValue",
                "sd":"schema:sdValue",
                "percents":{"@id":"schema:percent", "@container":"@list"},
              }
    #print d.keys()
    for field in features.keys():
        if features[field]["TYPE"]=="ENUM":
            context[field] = "schema:enumEntry"
            data = d[features[field]["NAME"]]
            fields = []
            counts = {}
            total = 0
            try:
                valueMap = features[field]["valueMap"]
            except KeyError:
                valueMap = {}
            for x in data.keys():
                total = total+1
                v = data[x]
                try:
                    v = valueMap[v]
                except KeyError:
                    pass
                v = str(v)
                if not(v in fields):
                    fields.append(v)
                try:
                    counts[v] = counts[v]+1
                except KeyError:
                    counts[v] = 1
            percent = {}
            for x in counts.keys():
                percent[x] = counts[x]*1.0/total
            profile[field] = { "type":"ENUM", "fields":fields, "counts":counts, "percents":percent }
            try:
                profile[field]["meta"] = features[field]["META"]
            except KeyError:
                pass
        if features[field]["TYPE"]=="INT":
            context[field] = "schema:intEntry"
            data = d[features[field]["NAME"]]
            total = 0.0
            count = 0.0
            b = []
            #print field
            try:
                valueMap = features[field]["valueMap"]
            except KeyError:
                valueMap = {}
            print(field)
            minv = None
            maxv = None
            #print "a "+str(count_data(data))
            for x in data.keys():
                v = data[x]
                #print "Value"
                #print v
                try:
                    v = valueMap[v]
                except KeyError:
                    pass
                #print v
                try:
                    try:
                        v = float(v)
                    except ValueError:
                        #print v
                        v = float("nan")
                    if (not(math.isnan(v))):
                        #print x
                        #print v
                        count = count+1
                        #print v
                        #print v
                        total = total+v
                        try:
                            cuts = features[field]["VALUE_CUT"]
                            #print cuts
                            if (b==[]):
                                x = len(cuts)
                                while x > -1:
                                    x = x - 1
                                    b.append(0)
                            if not(math.isnan(b)):
                                n = 0
                                while n < len(cuts) and v >= cuts[n]:
                                    n = n+1
                                b[n] = b[n] + 1
                        except KeyError:
                            pass
                        try:
                            if minv==None:
                                minv = v
                            elif v < minv:
                                minv = v
                        except NameError:
                            minv = v
                        try:
                            if maxv==None:
                                maxv = v
                            elif v > maxv:
                                maxv = v
                        except NameError:
                            maxv = v
                except TypeError:
                    pass
                #print count
            #print "b "+str(count_data(data))
            if count > 0:
                mean = total / count
                sd_total = 0.0
                for x in data.keys():
                    try:
                        v = data[x]
                        try:
                            v = valueMap[v]
                        except KeyError:
                            pass
                        try:
                            v = float(v)
                        except ValueError:
                            #print v
                            v = 0
                        if (not(math.isnan(v))):
                            sd_total = sd_total + (v - mean) * (v - mean)
                    except TypeError:
                        pass
                sd = (sd_total / count) ** 0.5
                pp = { "type":"INT", "count":count, "mean":mean, "min":minv,
                                 "max":maxv, "sd":sd }
            else:
                pp = { "type":"INT", "count":0, "mean":None, "min":None,
                                 "max":None, "sd":None }
            #print "c "+str(count_data(data))
            try:
                pp["meta"] = features[field]["META"]
            except KeyError:
                pass
            try:
                pp["valueBins"] = features[field]["VALUE_CUT"]
                pp["valueBinCounts"] = b
            except KeyError:
                pass
            try:
                p = features[field]["PERCENT_CUT"]
                l = []
                for x in data.keys():
                    v = data[x]
                    try:
                        try:
                            v = valueMap[v]
                        except KeyError:
                            pass
                        try:
                            v = float(v)
                        except ValueError:
                            #print v
                            v = 0
                        if not(math.isnan(v)):
                            l.append(v)
                    except TypeError:
                        pass
                l.sort()
                #print field
                #for x in l:
                    #print x
                c = []
                t = 0
                for x in p:
                    t = t + x
                    #print t
                    pos = t * len(l) / 100.0
                    #print pos
                    p1 = int(pos)
                    if p1==pos:
                        p2 = p1
                    else:
                        p2 = p1+1
                    if p1 >= len(l):
                        v1 = l[len(l)-1]+1
                    else:
                        v1 = l[p1]
                    if p2 >= len(l):
                        v2 = l[len(l)-1]+1
                    else:
                        v2 = l[p2]
                        if v2==v1:
                            while p2 < len(l)-1 and l[p2]==v1:
                                p2 = p2 + 1
                                v2 = l[p2]
                            if l[p2]==v1:
                                v2 = v1+1
                    #print v1
                    #print v2
                    #print p1
                    #print p2
                    c.append(v1+(v2-v1)*0.5)
	            #print c
                b = []
                x = len(c)
                #print "BINNING"
                #print c
                while x > -1:
                     x = x - 1
                     b.append(0)
                for x in data.keys():
                    try:
                        v = data[x]
                        try:
                            v = valueMap[v]
                        except KeyError:
                            pass
                        try:
                            v = float(v)
                        except ValueError:
                            #print v
                            v = 0
                        #print v
                        if not(math.isnan(v)):
                            n = 0
                            while n < len(c) and v >= c[n]:
                                n = n+1
                            b[n] = b[n] + 1
                    except TypeError:
                        pass
                pp["percentCuts"] = p
                pp["percentCutValues"] = c
                pp["percentCutCounts"] = b
            except KeyError:
                pass
            #print "d "+str(count_data(data))
            try:
                p = features[field]["STD_CUT"]
                if p==[] or not(type(p) is list):
                    p = [-2,-1,-0.5,0.5,1,2]
                c = []
                if count<2:
                    for x in p:
                        if pp["min"]==None:
                            c.append(len(c))
                        else:
                            c.append(pp["min"]+1+len(c))
                else:
                    for x in p:
                        c.append(pp["mean"]+pp["sd"]*x)
                b = []
                x = len(c)
                while x > -1:
                     x = x - 1
                     b.append(0)
                print pp
                #print field
                #print data
                #print "e "+str(count_data(data))
                for x in data.keys():
                    try:
                        v = data[x]
                        #if not(math.isnan(v)):
                            #print(x)
                        try:
                            v = valueMap[v]
                        except KeyError:
                            pass
                        #if not(math.isnan(v)):
                            #print("Here1")
                        try:
                            v = float(v)
                        except ValueError:
                            #print v
                            v = float("nan")
                        #print v
                        #if not(math.isnan(v)):
                            #print("Here2")
                        if not(math.isnan(v)):
                            #print "%d - %f"%(x,v)
                            n = 0
                            while n < len(c) and v >= c[n]:
                                n = n+1
                            #print n
                            #print len(b)
                            b[n] = b[n] + 1
                    except TypeError:
                        pass
                #print c
                #print b
                pp["stdCutValues"] = c
                pp["stdCutCounts"] = b
            except KeyError:
                pass
            print(pp)
            profile[field] = pp
    for field in features.keys():
        if features[field]["TYPE"]=="CROSSTAB":
            context[field] = "schema:crosstabEntry"
            print field
            constituents = features[field]["CONSTITUENTS"]
            data = []
            bins = []
            probabilities = []
            has_empty_constituent = False
            for c in constituents:
                data.append(d[features[c]["NAME"]])
                x = get_crosstab_bins(profile[c])
                if len(x)==0:
                    has_empty_constituent = True
                bins.append(x)
                prob = []
                total = 0.0
                try:
                    counts = profile[c]["counts"]
                    fields = profile[c]["fields"]
                except KeyError:
                    try:
                        counts = profile[c]["valueBinCounts"]
                    except KeyError:
                        try:
                            counts = profile[c]["percentCutCounts"]
                        except KeyError:
                            #print c
                            #print profile[c]
                            #print features[c]
                            counts = profile[c]["stdCutCounts"]
                    fields = []
                    i = 0
                    while i < len(counts):
                        fields.append(i)
                        i = i + 1
                print counts
                print fields
                for f in fields:
                    cc = counts[f]
                    total = total+cc
                for f in fields:
                    if total==0:
                        prob.append(1.0/len(fields))
                    else:
                        prob.append(counts[f] / total)
                probabilities.append(prob)
            if has_empty_constituent:
                crosstab = { "type":"CROSSTAB", "tabulations":{}, "constituents":constituents, "mutual_information":0, "mutual_first_info":0, "relevant_combinations":0 }
                try:
                    crosstab["meta"] = features[field]["META"]
                except KeyError:
                    pass
            else:
                print(field)
                print(bins)
                try:
                    excludes = features[field]["EXCLUDE"]
                except KeyError:
                    excludes = {}
                skip_over = []
                x = 0
                while x < len(bins):
                    y = 0
                    c = constituents[x]
                    while y < len(bins[x]):
                        try:
                            #print excludes
                            if bins[x][y] in excludes[c]:
                                skip_over.append((x,y))
                        except KeyError:
                            pass
                        y = y+1
                    x = x+1
                if len(constituents)>2:
                    print(field)
                    print(skip_over)
                #print probabilities
                counts = []
                onetabs = []
                exclude_tabs = []
                exclude_probs = []
                total_one = 0.0
                p_one = []
                x = 0
                while x < len(probabilities[0]):
                    p_one.append(0.0)
                    x = x+1
                for x in data[0].keys():
                    #print x
                    n = 0
                    value = []
                    while n < len(constituents):
                        try:
                            valueMap = features[constituents[n]]["valueMap"]
                        except KeyError:
                            valueMap = {}
                        v = data[n][x]
                        try:
                            v = valueMap[v]
                        except KeyError:
                            pass
                        value.append(v)
                        n = n+1
                    ind = []
                    n = 0
                    while n < len(constituents):
                        q = get_bin_number(profile[constituents[n]],value[n])
                        if q==-1:
                            ind = None
                            break
                        ind.append(q)
                        n = n+1
                    #print counts
                    #print ind
                    if ind != None:
                        v = get_crosstab_value(counts, ind)
                        #print v
                        v = v+1
                        #print counts
                        counts = set_crosstab_value(counts, ind, v)
                        doit = True
                        q = 0
                        while q < len(ind):
                            if (q,ind[q]) in skip_over:
                                doit = False
                            q = q+1
                        if doit:
                            v = get_crosstab_value(onetabs,ind[1:])
                            v = v+1
                            onetabs = set_crosstab_value(onetabs,ind[1:],v)
                            v = get_crosstab_value(exclude_tabs,ind)
                            v = v+1
                            exclude_tabs = set_crosstab_value(exclude_tabs,ind,v)
                            n = 0
                            while n < len(ind):
                                v = get_crosstab_value(exclude_probs,[n,ind[n]])
                                v = v+1.0
                                exclude_probs = set_crosstab_value(exclude_probs,[n,ind[n]],v)
                                n = n+1
                            total_one = total_one+1
                            p_one[ind[0]] = p_one[ind[0]]+1.0
                x = 0
                while x < len(probabilities[0]) and total_one > 0:
                    p_one[x] = p_one[x] / total_one
                    x = x + 1
                if len(constituents)>2:
                    print(total)
                    print("total_one %f"%total_one)
               # print counts
               # combine cross tabulations
                list = []
                ind = get_first_crosstab_index(bins)
                cont = True
                minfo = 0.0
                moinfo = 0.0
                while cont:
                    fields = {}
                    i = 0
                    pd = 1.0
                    while i < len(constituents):
                        print(ind)
                        print constituents
                        print bins
                        print i
                        fields[constituents[i]] = bins[i][ind[i]]
                        print ind[i]
                        if ind[i] >= len(probabilities[i]):
                            pd = 0
                        else:
                            pd = pd * probabilities[i][ind[i]]
                        i = i+1
                    v = get_crosstab_value(counts,ind)
                    if total==0:
                        pn = 0
                    else:
                        pn = v / total
                    if pd > 0 and pn > 0:
                        #print pd
                        #print pn
                        #print minfo
                        minfo = minfo + pn * math.log(pn / pd)
                    doit = True
                    q = 0
                    while q < len(ind):
                        if (q,ind[q]) in skip_over:
                            doit = False
                        q = q+1
                    if doit and total_one>0:
                        if ind[0] < len(exclude_probs[0]):
                            pdp = exclude_probs[0][ind[0]] / total_one
                        else:
                            pdp = 0
                        i = 0
                        if total_one == 59:
                            print(ind)
                            print(get_crosstab_value(onetabs,ind[1:]))
                        pdp = pdp * get_crosstab_value(onetabs,ind[1:])/total_one
                    #while i < len(constituents):
                    #    if total_one == 59:
                    #        print "i = %d"%i
                    #    if ind[i]>=len(exclude_probs[i]) and total_one > 0:
                    #        pr = 0.0
                    #    else:
                    #        pr = exclude_probs[i][ind[i]] / total_one
                        #sub = 0.0
                        #for x in skip_over:
                        #    if x[0]==i:
                        #        sub = sub+probabilities[i][x[1]]
                        #if total_one == 59:
                        #    print pr
                            #print sub
                            #print pr / (1.0-sub)
                        #pr = pr / (1.0 - sub)
                    #    pdp = pdp * pr
                    #    i = i+1
                    #if ind[0]>= len(probabilities[0]):
                    #    pnp = 0
                    #else:
                    #    pnp = p_one[ind[0]]
                    #pnp = pnp * (get_crosstab_value(onetabs,ind[1:])/total_one)
                        pnp = get_crosstab_value(counts,ind)/total_one
                        if total_one == 59:
                            #print(ind)
                            #print(pnp)
                            #print(pdp)
                            print(get_crosstab_value(onetabs,ind[1:]))
                        if pdp > 0 and pnp > 0:
                            moinfo = moinfo + pnp * math.log(pnp / pdp)
                    print("Entry")
                    print(fields)
                    print(v)
                    list.append({"fields":fields, "count":v})
                    ind = get_next_crosstab_index(bins,ind)
                    if all_zeros(ind):
                        cont = False
                if len(constituents)>2:
                    print(moinfo)
                    print(minfo)
                crosstab = { "type":"CROSSTAB", "tabulations":list, "constituents":constituents, "mutual_information":minfo, "mutual_first_info":moinfo, "relevant_combinations":total_one }
                try:
                    crosstab["meta"] = features[field]["META"]
                except KeyError:
                    pass
            profile[field] = crosstab

    profile["@context"] = context
    profile["@id"] = "http://clinicalProfile.ncats.io/cp"
    profile["@type"] = "http://clinicalProfile/cp_type"
    return profile

def generate(patients,profile_directives):
    profile_directives = {}

    try:
        outcomes = profile_directives["outcomes"]
    except KeyError:
        outcomes = ["died"]

    try:
        condition_map = profile_directives["condition_map"]
    except KeyError:
        condition_map = {}

    try:
        condition_max = profile_directives["condition_max"]
    except KeyError:
        condition_max = None

    try:
        condition_min_count = profile_directives["condition_min_count"]
    except KeyError:
        condition_min_count = None

    try:
        procedure_map = profile_directives["procedure_map"]
    except KeyError:
        procedure_map = {}
    
    try:
        medication_map = profile_directives["medication_map"]
    except KeyError:
        medication_map = {}
    try:
        measurement_map = profile_directives["measurement_map"]
    except KeyError:
        measurement_map = {}
    
    try:
        procedure_min_count = profile_directives["procedure_min_count"]
    except KeyError:
        procedure_min_count = None

    try:
        diagnosis_filter_list = profile_directives["diagnosis_filter_list"]
    except KeyError:
        diagnosis_filter_list = {}
    
    try:
        medication_filter_list = profile_directives["medication_filter_list"]
    except KeyError:
        medication_filter_list = {}
    
    try:
        procedure_filter_list = profile_directives["procedure_filter_list"]
    except KeyError:
        procedure_filter_list = {}

    try:
        core_procedure = profile_directives["core_procedure"]
    except KeyError:
        core_procedure = []

    try:
        core_diagnosis = profile_directives["core_diagnosis"]
    except KeyError:
        core_diagnosis = []
    
    try:
        core_medication = profile_directives["core_medication"]
    except KeyError:
        core_medication = {}

    try:
        core_measurement = profile_directives["core_measurement"]
    except KeyError:
        core_measurement = {}

    try:
        include_procedure = profile_directives["include_procedure"]
    except KeyError:
        include_procedure = []

    try:
        include_diagnosis = profile_directives["include_diagnosis"]
    except KeyError:
        include_diagnosis = []
    
    try:
        include_medication = profile_directives["include_medication"]
    except KeyError:
        include_medication = []

    try:
        include_measurement = profile_directives["include_measurement"]
    except KeyError:
        include_measurement = []

    patients = filter_patients(patients,diagnosis_filter_list,procedure_filter_list,medication_filter_list,condition_map,procedure_map,medication_map)

    conditions = tabulate_conditions(patients,condition_map)
    procedures = tabulate_procedures(patients,procedure_map)
    measurements = tabulate_measurements(patients,measurement_map)
    medications = tabulate_medications(patients,medication_map)

    tabulations = tabulate_characteristics(patients)
    sex_tabulation = tabulations[0]
    race_tabulation = tabulations[1]
    died_tabulation = tabulations[2]
    ethnicity_tabulation = tabulations[3]
    state_tabulation = tabulations[4]
    zip_tabulation = tabulations[5]

    header = build_header(procedures,measurements,medications,conditions,include_procedure,include_measurement,include_medication,include_diagnosis)
    df = build_dataFrame(patients,procedures,measurements,medications,conditions,include_procedure,include_measurement,include_medication,include_diagnosis,procedure_map,medication_map,condition_map)
    features = build_features(header,outcomes=["died"],profile_directives=profile_directives)

    return process_pandas_object(df, features)


