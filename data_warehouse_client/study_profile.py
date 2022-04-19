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


import pandas as pd
import pandas_profiling as pp
import datetime
from data_warehouse_client import data_warehouse


def mk_html_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".html"


def profile_all_measurement_groups(dw, study, report_dir):
    """
    Write a Profile file for each measurement group in a study
    :param dw: data warehouse handle
    :param study: study id
    :param report_dir: the directory in which the profiles will be written
    """

    timestamp = datetime.datetime.now()                        # use the current date and time in the filenames
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')   # format the date time string used in the filename
    measurement_groups = dw.get_all_measurement_groups(study)  # get all the measurement groups in the study

    for [mg_id, mg_name] in measurement_groups:                # for each measurement group
        (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])   # extract header and all instances
        df = pd.DataFrame(instances, columns=header)                                 # create a pandas data frame
        profile = pp.ProfileReport(df, title=mg_name + " Pandas Profiling Report")   # create a profile report
        f_name = mk_html_report_file_name(report_dir, "Measurement-Group-Profile-" + mg_name + "-", time_fname_str)
        profile.to_file(f_name)  # store the report in  a file

