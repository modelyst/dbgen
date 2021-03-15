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

from hypothesis.strategies import SearchStrategy, builds, from_type, just, one_of

from dbgen.core.expr.sqltypes import JSON, JSONB, Boolean, Date, Decimal, Double, Int, Text, Timestamp


def DecimalStrat() -> SearchStrategy:
    return from_type(Decimal)


def BooleanStrat() -> SearchStrategy:
    return from_type(Boolean)


def IntStrat() -> SearchStrategy:
    return builds(Int, kind=one_of(just('small'), just('medium'), just('big')))


def TextStrat() -> SearchStrategy:
    return from_type(Text)


def DateStrat() -> SearchStrategy:
    return from_type(Date)


def TimestampStrat() -> SearchStrategy:
    return from_type(Timestamp)


def DoubleStrat() -> SearchStrategy:
    return from_type(Double)


def JSONStrat() -> SearchStrategy:
    return from_type(JSON)


def JSONBStrat() -> SearchStrategy:
    return from_type(JSONB)


sql_type_strats = [
    DecimalStrat(),
    BooleanStrat(),
    IntStrat(),
    TextStrat(),
    DateStrat(),
    TimestampStrat(),
    DoubleStrat(),
    JSONStrat(),
    JSONBStrat(),
]


def SQLTypeStrat() -> SearchStrategy:
    return one_of(*sql_type_strats)
