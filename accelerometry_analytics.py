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


def enmo_aggregations(dw, study, mg, x_acc_type, y_acc_type, z_acc_type, aggregation):
    """

    :param dw: a data warehouse handle
    :param study: study id
    :param mg: message group
    :param x_acc_type: the message type for the accelerometery x axis
    :param y_acc_type: the message type for the accelerometery y axis
    :param z_acc_type: the message type for the accelerometery z axis
    :param aggregation: the type of time-based aggregation
    :return: the aggregated enmo data in the form (timestamp, enmo)
    """
    test = False
    if aggregation in trunc_options():
        qx = mk_axis_query(study, mg, x_acc_type, "x")
        qy = mk_axis_query(study, mg, y_acc_type, "y")
        qz = mk_axis_query(study, mg, z_acc_type, "z")
        if test:
            qx_res = dw.returnQueryResult("SELECT * FROM " + qx)
            print(*qx_res, sep='\n')

        q_combine_axes = "(SELECT x.time AS timestamp, ABS(SQRT(x.sqr + y.sqr + z.sqr) - 1) AS enmo " + \
                         " FROM " + qx + " INNER JOIN " + qy + " ON (x.groupinstance=y.groupinstance) " + \
                         "                 INNER JOIN " + qz + " ON (x.groupinstance=z.groupinstance) ) AS enmovalues "

        q_main = " SELECT date_trunc(\'" + aggregation + "\', timestamp) AS timestamp, AVG(enmo) as enmo " +\
                 "  FROM " + q_combine_axes + \
                 "  GROUP BY date_trunc(\'" + aggregation + "\', timestamp)" + \
                 "  ORDER BY timestamp; "

        res = dw.returnQueryResult(q_main)
        return res
    else:
        print(f'{aggregation} is not a valid aggregation option.')
        return []
