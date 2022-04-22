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


def mk_html_report_file_name(f_dir, report_name, time_string):
    return f_dir + report_name + time_string + ".html"


def profile_all_measurement_groups(dw, study, report_dir, select_participants=False, participants=[],
                                   select_trials=False, trials=[], hide_trial_column=False, filename_prefix=''):
    """
    Write a Profile file for each measurement group in a study
    :param dw: data warehouse handle
    :param study: study id
    :param report_dir: the directory in which the profiles will be written
    :param select_participants: select a subset of participants to be included in the profile
    :param participants: list of participants to be included in the profile if select_participants is true
    :param select_trials: select a subset of trials to be included in the profile
    :param trials: list of trials to be included in the profile if select_trials is true
    :param hide_trial_column: don't include the Trial column (useful for studies where trial is not used)
    :param filename_prefix: optional string to add to front of filename
    """
    timestamp = datetime.datetime.now()                        # use the current date and time in the filenames
    time_fname_str = timestamp.strftime('%Y-%m-%dh%Hm%Ms%S')   # format the date time string used in the filename
    measurement_groups = dw.get_all_measurement_groups(study)  # get all the measurement groups in the study

    for [mg_id, mg_name] in measurement_groups:                # for each measurement group
        (header, instances) = dw.get_measurement_group_instances(study, mg_id, [])   # extract header and all instances
        df_full = pd.DataFrame(instances, columns=header)                            # create a pandas data frame
        if select_participants:   # select participants
            df_participants = df_full.loc[df_full['Participant'].isin(participants)]
        else:
            df_participants = df_full   # include all participants
        if select_trials:  # select trials
            df = df_participants.loc[df_participants['Trial'].isin(trials)]
        else:
            df = df_participants   # include all trials
        # drop columns that are not needed in the profile
        if hide_trial_column:
            std_columns_to_drop = ['Trial', 'Instance', 'Study', 'Measurement Group', 'Participant']
        else:
            std_columns_to_drop = ['Instance', 'Study', 'Measurement Group', 'Participant']
        if 'uniqueId' in header:
            columns_to_drop = ['uniqueId'] + std_columns_to_drop
        else:
            if 'uniqueid' in header:
                columns_to_drop = ['uniqueid'] + std_columns_to_drop
            else:
                columns_to_drop = std_columns_to_drop
        df_main_columns = df.drop(columns=columns_to_drop)
        # create a profile report
        profile = pp.ProfileReport(df_main_columns, title="Profiling Report for " + mg_name, progress_bar=False)
        f_name = mk_html_report_file_name(report_dir,
                                          filename_prefix + "Measurement-Group-Profile-" + mg_name + "-",
                                          time_fname_str)
        profile.to_file(f_name)  # store the report in  a file
