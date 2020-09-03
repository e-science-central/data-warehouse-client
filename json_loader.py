import data_warehouse
import json
import sys

def load_json_file(fname:str):
    """
    Load a json file
    :param fname: the filename of the json file
    :return the json file represented as a Python dictionary
    """
    try:
        with open(fname, 'r') as jIn:
            j = json.load(jIn)
            return j
    except Exception as e:
        sys.exit("Unable to load the json file! Exiting.\n" + str(e))

def mk_01(s:str):
    """
    Transform a boolean represented as a 'T' or 'F' string into 0 for False or 1 for True
    :param s: 'T' or 'F'
    :return integer 0 for False or 1 for True
    """
    if s=='N':
        return 0
    else:
        return 1

def mk_optional_string(data,jfield):
    """
    Returns either the string represented in a json structure, or an empty string if it's not present
    :param date:   json that may contian the jfield
    :param jfield: the name of the field
    :return if the field exists then it's returned; otherwise an empty string is returned
    """
    val = data.get(jfield)
    if val == None:
        return ""
    else:
        return val

def mk_category(cat_name,cat_list):
    """
    Returns the position of a category in a list of category names.
    (N.B. only works if categorids in the category table run consecutively from 0)
    :param cat_name: the category name from the category table
    :param cat_list: a list of all categories, in order of their categoryid in the category table
    :return the categoryid of the category
    """
    return cat_list.index(cat_name)

def split_enum(jfields,typeids,valuelist):
    """
    ENUMS (Sets of values) are not represented directly in the warehouse. Instead they are represented as one boolean
    measurementtype per value. This function takes a json list of values and creates the list of measurements from it -
    one for each type.
    :param jfields: the list of possible values in the ENUM (in the order in which they are to be added into the
                    measurement types
    :param typeids: the list of typeids of the measurements into which the booleans are to be stored
    :param valuelist: the list of values that are to be stored as measurements
    :return The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    res = []
    for (typeid,value) in zip(typeids,valuelist):
        res = res + [(typeid,4,int(value in jfields))] # the 4 is because the type is boolean
    return res

def get_participantid(studyid,js):
    """
    maps from the participantid that is local to the study, to the unique id stored with measurements in the warehouse
    :poram studyid: the study id
    :param js: the json form
    :return The id of the participant
    """
    localid = js['metadata']['_userId']
    q = " SELECT id FROM participant " \
        "WHERE participant.study       = "  + str(studyid) + \
        "AND participant.participantid = '" + localid + "';"
    res =  data_warehouse.returnQueryResult(q)
    return res[0]

def mk_e_screening_chf(js):
    """
    transforms a e-screening-chf json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return [(191, 4, mk_01(data['metre'])),
            (192, 4, mk_01(data['gold'])),
            (193, 4, mk_01(data['ageval'])),
            (194, 4, mk_01(data['impaired'])),
            (195, 4, mk_01(data['willing'])),
            (196, 4, mk_01(data['eligible'])),
            (197, 4, mk_01(data['available'])),
            (198, 4, mk_01(data['diagnosis'])),
            (199, 4, mk_01(data['history'])),
            (200, 4, mk_01(data['literate'])),
            (201, 4, mk_01(data['shoe']))]

def mk_walking_aids_group(js):
    """
    transforms a j-walking-aids.json form into the triples used by insertMeasurementGroup to
        store each measurement that is in the form
    :param js: the json form
    :return: The list of (typeid,valType,value) triples that are used by insertMeasurementGroup to add the measurements
    """
    data = js['data']
    return  split_enum( data['allaids'],[14,15,16,17,18,19],
                        ['None','One cane/crutch','Two crutches','Walker','Rollator','Other']) +\
            [(20, 2, mk_optional_string(data,'Other')),
             (21, 5, mk_category(data['indoor'],['None','One cane/crutch','Two crutches','Walker','Rollator','Other'])),
             (22 ,5, mk_category(data['indoorfreq'],['Regularly', 'Occasionally'])),
             (23 ,5, mk_category(data['outdoor'],['None','One cane/crutch','Two crutches','Walker','Rollator','Other'])),
             (24 ,5, mk_category(data['outdoorfreq'],['Regularly', 'Occasionally']))]

#Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")

print("\n Test of loading database from e-screening-chf json file\n")
js = load_json_file('input\e-screening-chf.json')
participantid = get_participantid(4,js)
# insert new instance in the warehouse
instanceid = data_warehouse.insertMeasurementGroup(4,24,mk_e_screening_chf(js),participant=participantid)

newdata = data_warehouse.getMeasurements(groupInstance=instanceid)
dataInTabularForm = data_warehouse.formMeasurementGroup(newdata)
data_warehouse.printMeasurementGroupInstances(dataInTabularForm,24)

print("\n Test of loading database from j-walking-aids json file\n")
js = load_json_file('input\j-walking-aids.json')
participantid = get_participantid(4,js)

# insert new instance in the warehouse
instanceid = data_warehouse.insertMeasurementGroup(4,4,mk_walking_aids_group(js),participant=participantid)

newdata = data_warehouse.getMeasurements(groupInstance=instanceid)
dataInTabularForm = data_warehouse.formMeasurementGroup(newdata)
data_warehouse.printMeasurementGroupInstances(dataInTabularForm,4)

