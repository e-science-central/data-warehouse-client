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


def trunc_options():
    """
    A list of all trunc options on dates supported by postgresql
    :return: a list of all trunc options on dates supported by postgresql
    """
    truncs = ['millennium', 'century', 'decade', 'year', 'quarter', 'month', 'week', 'day',
              'hour', 'minute', 'second', 'milliseconds', 'microseconds']
    return truncs


def mk_axis_query(study, mg, acc_type, axis_name):
    """
    generate the sql used in enmo calculations for one accelerometer axis
    :param study: study id
    :param mg: message group
    :param acc_type: the message type for the accelerometery data
    :param axis_name: the name of the axis (x, y or z)
    :return: the sql to calculate the square of the value in one axis of the accelerometer
    """
    q = " (SELECT time, groupinstance, valreal*valreal AS sqr" + \
        "  FROM   measurement" + \
        "  WHERE  measurementtype = " + str(acc_type) + " AND " + \
        "         study = " + str(study) + " AND " + \
        "         measurementgroup =  " + str(mg) + ") AS " + axis_name
    return q


def mk_enmo_aggregation_query(study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation):
    """
    Create a query to calculate and aggregate enmo from accelerometry
    :param study: study id
    :param mg: message group
    :param x_acc_type: the message type for the accelerometery x axis
    :param y_acc_type: the message type for the accelerometery y axis
    :param z_acc_type: the message type for the accelerometery z axis
    :param aggregation: the type of time-based aggregation
    :return: the query
    """

    if aggregation in trunc_options():  # valid aggreagation in postgresql
        qx = mk_axis_query(study, mg, x_acc_type, "x")
        qy = mk_axis_query(study, mg, y_acc_type, "y")
        qz = mk_axis_query(study, mg, z_acc_type, "z")

        q_combine_axes = "(SELECT x.time AS timestamp, ABS(SQRT(x.sqr + y.sqr + z.sqr) - 1) AS enmo " + \
                         " FROM " + qx + " INNER JOIN " + qy + " ON (x.groupinstance=y.groupinstance) " + \
                         "                 INNER JOIN " + qz + " ON (x.groupinstance=z.groupinstance) ) AS enmovalues "

        q_main = " SELECT date_trunc(\'" + aggregation + "\', timestamp) AS timestamp, AVG(enmo) as enmo " + \
                 "  FROM " + q_combine_axes + \
                 "  GROUP BY date_trunc(\'" + aggregation + "\', timestamp)" + \
                 "  ORDER BY timestamp "
        return (True, q_main)
    else:
        print(f'{aggregation} is not a valid aggregation option.')
        return (False, "")


def enmo_aggregations(dw, study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation):
    """
    Calculate and aggregate enmo from accelerometry
    :param dw: a data warehouse handle
    :param study: study id
    :param mg: message group
    :param x_acc_type: the message type for the accelerometery x axis
    :param y_acc_type: the message type for the accelerometery y axis
    :param z_acc_type: the message type for the accelerometery z axis
    :param aggregation: the type of time-based aggregation
    :return: the aggregated enmo data in the form (timestamp, enmo)
    """
    (valid, q) = mk_enmo_aggregation_query(study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation)
    if valid:
        res = dw.returnQueryResult(q + ";")
        return res
    else:
        return []


#  resting / sedentary, low, moderate, vigorous
#    physical
#    activity

def activity_classification(dw, study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation):
    """
    Calculate the breakdown in activity based on aggregated enmo values
    :param dw: a data warehouse handle
    :param study: study id
    :param mg: message group
    :param x_acc_type: the message type for the accelerometery x axis
    :param y_acc_type: the message type for the accelerometery y axis
    :param z_acc_type: the message type for the accelerometery z axis
    :param aggregation: the type of time-based aggregation
    :return: the breakdown of activity in the form [(activity_level, count of values at that level)]
    """
    #  https://ubiq.co/database-blog/create-histogram-postgresql/
    (valid, enmo_aggregation) = mk_enmo_aggregation_query(study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation)
    if valid:
        q = "SELECT '1. Sedentary' as activity, COUNT(enmo) AS count FROM " + "(" + enmo_aggregation + ") AS s " + \
            "WHERE enmo BETWEEN 0.0 and 0.25 " + \
            "UNION(" + \
            "SELECT '2. Low' as activity, COUNT(enmo) AS count FROM " + "(" + enmo_aggregation + ") AS l " + \
            "WHERE enmo BETWEEN 0.25 and 0.5 ) " + \
            "UNION(" + \
            "SELECT '3. Moderate' as activity, COUNT(enmo) AS count FROM " + "(" + enmo_aggregation + ") AS m " + \
            "WHERE enmo BETWEEN 0.5 and 0.75 ) " + \
            "UNION(" + \
            "SELECT '4. Vigorous' as activity, COUNT(enmo) AS count FROM " + "(" + enmo_aggregation + ") AS v " + \
            "WHERE enmo BETWEEN 0.75 and 1.0 ) " + \
            "ORDER BY activity;"
        res = dw.returnQueryResult(q)
        return res
    else:
        return []
