#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import unittest
from enum import Enum

from airflow.utils.weekday import WeekDay


class TestWeekDay(unittest.TestCase):
    def test_weekday_enum_length(self):
        assert len(WeekDay) == 7

    def test_weekday_name_value(self):
        weekdays = "MONDAY TUESDAY WEDNESDAY THURSDAY FRIDAY SATURDAY SUNDAY"
        weekdays = weekdays.split()
        for i, weekday in enumerate(weekdays, start=1):
            weekday_enum = WeekDay(i)
            assert weekday_enum == i
            assert int(weekday_enum) == i
            assert weekday_enum.name == weekday
            assert weekday_enum in WeekDay
            assert 0 < weekday_enum < 8
            assert isinstance(weekday_enum, WeekDay)
            assert isinstance(weekday_enum, int)
            assert isinstance(weekday_enum, Enum)
