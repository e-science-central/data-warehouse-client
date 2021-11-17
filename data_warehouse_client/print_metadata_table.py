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

from data_warehouse_client import file_utils
from tabulate import tabulate  # https://github.com/astanin/python-tabulate
import datetime


def valuetype_to_name():
    return {0: 'Integer', 1: 'Real', 2: 'Text', 3: 'Date Time', 4: 'Boolean',
            5: 'Nominal', 6: 'Ordinal', 7: 'Bounded Integer', 8: 'Bounded Real'}


def mk_txt_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".txt"


def print_metadata_tables_to_file(dw, study_id):

    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    fname = mk_txt_report_file_name(file_dir, "metadata-table-", time_fname_str)

    metadata = create_measurement_group_info(dw, study_id)
    headers = ['Measurement Type', 'Id', 'Value Type', 'Optional?', 'Units', 'Min Value', 'Max Value', 'Categories']

    with open(fname, "w", encoding="utf-8") as f:
        print(f'Measurement Groups for Study {study_id}', file=f)
        for mg_id, mg_data in metadata.items():
            mg_info = mg_data['message_types']
            name = mg_data['name']
            rows = []
            for mt_id, mt_info in mg_info.items():
                cats = '\n'.join(f'{v}:{k}' for k, v in mt_info['categories'].items())
                lower_bound = ''
                upper_bound = ''
                if mt_info['intbounds'] != {}:
                    lower_bound = mt_info['intbounds']['minval']
                    upper_bound = mt_info['intbounds']['maxval']
                elif mt_info['realbounds'] != {}:
                    lower_bound = mt_info['realbounds']['minval']
                    upper_bound = mt_info['realbounds']['maxval']
                if mt_info['units'] is None:
                    units_prnt = ""
                else:
                    units_prnt = mt_info['units']
                row = [mt_info['name'], mt_id, valuetype_to_name()[mt_info['valtype']],
                       mt_info['optional'], units_prnt, lower_bound, upper_bound, cats]
                rows = rows + [row]
            print(f'\nMeasurement Group {name} (id = {mg_id})\n', file=f)
            print(tabulate(rows, headers=headers, tablefmt="grid"), file=f)


def print_metadata_tables(dw, study_id):
    """
    print study metadata
    """
    metadata = create_measurement_group_info(dw, study_id)
    print(f'Measurement Groups for Study {study_id}')
    headers = ['Measurement Type', 'Id', 'Value Type', 'Optional?', 'Units', 'Min Value', 'Max Value', 'Categories']
    for mg_id, mg_data in metadata.items():
        mg_info = mg_data['message_types']
        name = mg_data['name']
        rows = []
        for mt_id, mt_info in mg_info.items():
            cats = '\n'.join(f'{v}:{k}' for k, v in mt_info['categories'].items())
            lower_bound = ''
            upper_bound = ''
            if mt_info['intbounds'] != {}:
                lower_bound = mt_info['intbounds']['minval']
                upper_bound = mt_info['intbounds']['maxval']
            elif mt_info['realbounds'] != {}:
                lower_bound = mt_info['realbounds']['minval']
                upper_bound = mt_info['realbounds']['maxval']
            if mt_info['units'] is None:
                units_prnt = ""
            else:
                units_prnt = mt_info['units']
            row = [mt_info['name'], mt_id, valuetype_to_name()[mt_info['valtype']],
                   mt_info['optional'], units_prnt, lower_bound, upper_bound, cats]
            rows = rows + [row]
        print(f'\nMeasurement Group {name} (id = {mg_id})\n')
        print(tabulate(rows, headers=headers, tablefmt="grid"))


def mk_optional(opt):
    """
    turns the value in the optional column into a boolean
    :param opt: either true, false or None
    :return: True (if opt == true) or False (if Null, or false)
    """
    if opt != True:
        return False
    else:
        return True


def create_measurement_group_info(dw, study):
    """
    Creates a dictionary with each entry a measurement group
    :param dw: data warehouse handle
    :param study: study id
    :return:
    """
    q1 = file_utils.process_sql_template("get_measurement_group_info.sql", {"study": study})
    r1 = dw.return_query_result(q1)  # return a list of (measurementgroup, measurementtype, name, valtype, optional)

    q2 = file_utils.process_sql_template("get_categories_in_study.sql", {"study": study})
    r2 = dw.return_query_result(q2)

    cats = {}
    for mt in set([row[0] for row in r2]):
        cats[mt] = dict(map(lambda t: (t[2], t[1]),  filter(lambda r: r[0] == mt, r2)))

    q3 = file_utils.process_sql_template("get_boundsint_in_study.sql", {"study": study})
    r3 = dw.return_query_result(q3)
    int_bounds = {}
    for [mt_id, minval, maxval] in r3:
        int_bounds[mt_id] = {'minval': minval, 'maxval': maxval}

    q4 = file_utils.process_sql_template("get_boundsreal_in_study.sql", {"study": study})
    r4 = dw.return_query_result(q4)
    real_bounds = {}
    for [mt_id, minval, maxval] in r4:
        real_bounds[mt_id] = {'minval': minval, 'maxval': maxval}

    q5 = file_utils.process_sql_template("get_measurement_groups_in_study.sql", {"study": study})
    r5 = dw.return_query_result(q5)
    mg_names = {}
    for [mg_id, mg_name] in r5:
        mg_names[mg_id] = mg_name

    q6 = file_utils.process_sql_template("get_units_in_study.sql", {"study": study})
    r6 = dw.return_query_result(q6)
    unit_name = {}
    for [mt_id, unit_description] in r6:
        unit_name[mt_id] = unit_description

    mg_info = {}
    for mg in set([row[0] for row in r1]):
        mt_info_all = {}
        for (group, ms_type, name, valtype, optional, units) in list(filter(lambda r: r[0] == mg, r1)):
            cat = cats.get(ms_type)
            if cat is None:
                cat_val = {}
            else:
                cat_val = cat
            intbounds = int_bounds.get(ms_type)
            if intbounds is None:
                int_bounds_val = {}
            else:
                int_bounds_val = intbounds
            realbounds = real_bounds.get(ms_type)
            if realbounds is None:
                real_bounds_val = {}
            else:
                real_bounds_val = realbounds
            unitname = unit_name.get(ms_type)
            if unitname is None:
                units_val = None
            else:
                units_val = unitname
            mt_info = {'name': name, 'valtype': valtype, 'optional': mk_optional(optional), 'units': units_val,
                       'categories': cat_val, 'intbounds': int_bounds_val, 'realbounds': real_bounds_val}
            mt_info_all[ms_type] = mt_info
        mg_info[mg] = {'name': mg_names[mg], 'message_types': mt_info_all}
    return mg_info
