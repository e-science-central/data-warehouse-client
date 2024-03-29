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

import matplotlib.pyplot as pyplot
from matplotlib.backends.backend_pdf import PdfPages
import datetime
import numpy as np

from data_warehouse_client import print_metadata_table


def plot_measurements(dw, rows, study, measurement_type_id, plot_file):
    """
    Plot the value of a measurement over time.
    :param dw: data warehouse handle
    :param rows: a list of measurements generated by the other client functions. Each measurement is in the form:
                    id,time,study,participant,measurementType,
                    typeName,measurementGroup,groupInstance,trial,valType,value
    :param study: study id
    :param measurement_type_id: the measurement type of the measurements to be plotted
    :param plot_file: the name of the file into which the plot will be written
    """
    # https://matplotlib.org/api/pyplot_api.html
    if len(rows) > 0:
        trans = [list(i) for i in zip(*rows)]  # transpose the list of lists
        x = trans[1]  # the data and time
        y = trans[10]  # the measurement value

        mt_info = dw.get_measurement_type_info(study, measurement_type_id)
        units = mt_info[0][3]  # get the units name
        pyplot.title(rows[0][5])
        pyplot.xlabel("Time")  # Set the x-axis label
        pyplot.ylabel(units)  # Set the y-axis label to be the units of the measurement type
        pyplot.plot(x, y)
        pyplot.savefig(plot_file)
        pyplot.close()
    else:
        print("No values to plot\n")


def mk_pdf_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + "-" + time_string + ".pdf"


def plot_distributions(dw, study):
    """
    Plot the distribution of all measurent types in a study
    :param dw: data warehouse handle
    :param study: study id
    :return:
    """
    metadata = print_metadata_table.create_measurement_group_info(dw, study)
    measurement_value_index = 10
    file_dir = "reports/"
    timestamp = datetime.datetime.now()  # use the current date and time if none is specified
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')
    fname = mk_pdf_report_file_name(file_dir, "measurement-distributions", time_fname_str)
    pdf = PdfPages(fname)

    # print(f'Measurement Data Distributions for Study {study}\n', file=f)
    for mg_id, mg_data in metadata.items():
        mg_info = mg_data['message_types']
        mg_name = mg_data['name']
        # print(f'\nMeasurement Group {name} (id = {mg_id})\n', file=f)
        for mt_id, mt_info in mg_info.items():
            mt_name = mt_info['name']
            # print(f'\nMeasurement Type {mt_name} (id = {mt_id})\n', file=f)
            ms = dw.get_measurements(study, measurement_group=mg_id, measurement_type=mt_id)  # get all measurements
            if len(ms) > 0:
                trans = [list(i) for i in zip(*ms)]  # transpose the list of lists
                values = trans[measurement_value_index]  # the measurement value
                values = list(filter(lambda val: val is not None, values))  # remove missing values
                # plot distribution based on value type
                value_type = print_metadata_table.valuetype_to_name()[mt_info['valtype']]
                if value_type in ['Integer', 'Real', 'Bounded Integer', 'Bounded Real']:
                    fig = pyplot.figure()
                    pyplot.boxplot([values], patch_artist=True,
                                   labels=[f'{mt_name}'])
                    pyplot.title(f'{mg_name} ({mg_id}) / {mt_name} ({mt_id})')
                    pyplot.close()
                    pdf.savefig(fig)
                elif value_type in ['Nominal', 'Ordinal']:
                    categories = mt_info['categories']  # get the category dictionary k = name, val = id
                    n_cat = len(categories)  # calculate the number of categories
                    cat_count = np.zeros((n_cat,), dtype=int)  # initialise a vector for each category
                    cat_index = {}
                    index = 0
                    for c in categories:  # create a mapping from the category name to the index into the vector
                        cat_index[c] = index
                        index += 1
                    #  print(f'\n {mg_id} / {mt_id}: {cat_index} \n')
                    for v in values:   # count how many values in each category
                        #  print(*values, sep='\n')
                        cat_count[cat_index[v]] += 1
                    cat_names = list(categories.keys())
                    cat_counts = list(cat_count)
                    fig = pyplot.figure()
                    pyplot.bar(cat_names, cat_counts)
                    pyplot.title(f'{mg_name} ({mg_id}) / {mt_name} ({mt_id})')
                    pyplot.close()
                    pdf.savefig(fig)
                elif value_type in ['Boolean']:
                    t_count = 0
                    f_count = 0
                    for v in values:
                        if v == 'T':
                            t_count += 1
                        else:
                            f_count += 1
                    fig = pyplot.figure()
                    pyplot.bar(['False', 'True'], [f_count, t_count])
                    pyplot.title(f'{mg_name} ({mg_id}) / {mt_name} ({mt_id})')
                    pyplot.close()
                    pdf.savefig(fig)
#              else:
                    # print('No Measurements Recorded', file=f)
    pdf.close()
