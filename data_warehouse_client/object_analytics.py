# Copyright 2020 Newcastle University.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def mk_get_measurement_by_type_sql(study, measurement_group, measurement_type, name):
    """
    Helper function used to extract the members of an instancegroup
    :param study: study id
    :param measurement_group: measurement group id
    :param measurement_type: measurement type
    :param name: name for the table
    :return: sql to extract the members of an instancegroup
    """
    q  = " (SELECT * FROM measurement WHERE study = " + str(study)
    q += " AND measurementgroup = " + str(measurement_group)
    q += " AND measurementtype  = " + str(measurement_type) + ") AS " + name + " "
    return q


def mk_object_table_sql(study):
    """
    Create SQL to turn an object group instance into a table
    :param study: study id
    :return: SQL to turn an object group instance into a table
    """
    q =  " (SELECT a.groupinstance AS instance, a.time AS time, a.valinteger AS frame, b.valinteger AS oid, "
    q += " c.valinteger AS otype, d.valreal AS left, e.valreal AS top, f.valreal AS right, g.valreal AS bottom "
    q += " FROM "
    q +=                 mk_get_measurement_by_type_sql(study, 1, 1, "a")
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 2, "b") + " ON (a.groupinstance = b.groupinstance) "
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 3, "c") + " ON (a.groupinstance = c.groupinstance) "
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 4, "d") + " ON (a.groupinstance = d.groupinstance) "
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 5, "e") + " ON (a.groupinstance = e.groupinstance) "
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 6, "f") + " ON (a.groupinstance = f.groupinstance) "
    q += "INNER JOIN " + mk_get_measurement_by_type_sql(study, 1, 7, "g") + " ON (a.groupinstance = g.groupinstance) "
    q += ") "
    return q


def head_count_sql(study):
    """
    creates sql to calculate the number of objects in each frame
    :param study:
    :return: sql to calculate the number of objects in each frame
    """
    return "SELECT frame, count(*) FROM " + mk_object_table_sql(study) + " AS object GROUP BY frame ORDER BY frame;"


def social_distancing_violations_sql(study, dist_fn, threshold):
    """
    Returns sql to compute the pairs of objects that are within a threshold distance of each other
    :param study: study id
    :param dist_fn: a funtion to calculate the distance
    :param threshold:
    :return: sql to return pairs of objects that are within a threshold of each other
    """
    q  = " WITH objtable AS (" + mk_object_table_sql(study) + ")"
    q += " SELECT a.time AS time, a.frame AS frame, a.oid AS aoid, a.otype AS atype,"
    q += "        b.oid AS boid,  b.otype AS botype, "
    q +=          dist_fn + " AS dist "
    q += " FROM objtable AS a INNER JOIN objtable AS b "
    q += " ON (a.frame = b.frame) and (a.oid < b.oid) "

    oq =  " SELECT frame, time, aoid, boid, dist "
    oq += " FROM (" + q + ") AS opair"
    oq += " WHERE dist < " + str(threshold) + " ORDER BY frame;"
    return oq


def pythagorus():
    """
    Calculate the distance between two objects represented by their bounding boxes
    :return: an sql expression to compute the distance between the centres of the objects
    """
    return "SQRT(POW((a.top+a.bottom)/2-(b.top+b.bottom)/2,2) + POW((a.top+a.bottom)/2-(b.top+b.bottom)/2,2))"

