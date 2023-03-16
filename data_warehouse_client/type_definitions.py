# Copyright 2023 Newcastle University.
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

from typing import Union, Tuple, List, Dict, Optional, Callable
from datetime import datetime

MeasurementType = int
ValType = int
DateTime = datetime
Value = Union[int, float, str, DateTime]
ValueTriple = Tuple[MeasurementType, ValType, Value]

Source = int
Participant = int
Trial = int
MeasurementGroup = int
MeasurementGroupInstance = int
Study = int

LoadHelperResult = Tuple[bool, List[ValueTriple], str]
LoaderResult = Tuple[List[Tuple[MeasurementGroup, List[LoadHelperResult]]],
                     Optional[DateTime], Optional[Trial], Optional[Participant], Optional[Source]]

DataToLoad = Dict[str, Union[Value, List['DataToLoad'], List[str]]]

FieldValue = Union[Value, List['DataToLoad'], List[str]]

IntBounds = Dict[MeasurementType, Dict[str, int]]
RealBounds = Dict[MeasurementType, Dict[str, float]]
DateTimeBounds = Dict[MeasurementType, Dict[str, DateTime]]
CategoryIds = Dict[MeasurementType, List[int]]
CategoryValues = Dict[MeasurementType, Dict[str, int]]

Bounds = Tuple[IntBounds, RealBounds, DateTimeBounds, CategoryIds, CategoryValues]

Loader = Callable[[DataToLoad, Bounds], LoaderResult]
