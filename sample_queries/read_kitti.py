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


import csv

import data_warehouse

# Create a connection to the data warehouse
data_warehouse = data_warehouse.DataWarehouse("db-credentials.json", "datawarehouse")


def object_types():
    """
    :return: the types of objets found in a kitti file, and their corresponding category id
    """
    return {'pedestrian': 0, 'car': 1}


def map_to_category_number(category_string, category_mapper):
    """
    map a category name to its id
    :param category_string: the string holding the category name
    :param category_mapper: a dictionary that maps the category name to the id
    :return: the category id
    """
    return category_mapper[category_string]


def read_kitti_file(file_name):
    """
    read KITTI file
    format is https://github.com/NVIDIA/DIGITS/blob/v4.0.0-rc.3/digits/extensions/data/objectDetection/README.md
    :param file_name:
    :return: list of object occurrences. Each is a tuple:
                (frameId,objectId,objectType,bboxLeft,bboxTop,bboxRight,bboxBottom)
    """
    fi = open(file_name, 'r')
    print("KITTI File found")
    object_occurrences = csv.reader(fi, delimiter=' ')
    res = []
    for field in object_occurrences:
        res = [(field[0], field[1], field[2], field[6], field[7], field[8], field[9])] + res
        # (frameId,oId,oType,bboxLeft,bboxTop,bboxRight,bboxBottom)
    return res


study = 100
source_id = 1
measurement_group = 1

frame_id_index = 0
object_id_index = 1
object_type_index = 2
bounding_box_Left_index = 3
bounding_box_Top_index = 4
bounding_box_Right_index = 5
bounding_box_Bottom_index = 6

r = read_kitti_file('input\KITTI-13.txt')
for cl in r:
    print(cl)
    data_warehouse.insertMeasurementGroup(study, measurement_group,
                                          [(1, 0, cl[frame_id_index]),
                                           (2, 0, cl[object_id_index]),
                                           (3, 5, map_to_category_number(cl[object_type_index], object_types())),
                                           (4, 1, cl[bounding_box_Left_index]),
                                           (5, 1, cl[bounding_box_Top_index]),
                                           (6, 1, cl[bounding_box_Right_index]),
                                           (7, 1, cl[bounding_box_Bottom_index])],
                                          source=source_id)
print(len(r), " objects read into the data warehouse")
