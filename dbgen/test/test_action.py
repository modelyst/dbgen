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

"""Module for testing the load object"""
# External Modules
import unittest

import dbgen.utils.exceptions as exceptions

# Internal Modules
from dbgen import Attr, Const, Model, Obj, Rel


class TestLoad(unittest.TestCase):
    """Test the load object"""

    def setUp(self) -> None:
        self.model = Model("test_model")
        parent_obj = Obj(
            "parent",
            attrs=[Attr("test_id_col", identifying=True)],
        )
        test_object = Obj(
            "child",
            attrs=[Attr("test_id_col", identifying=True), Attr("test_col")],
            fks=[Rel("parent", identifying=True)],
        )
        self.model.add([parent_obj, test_object])

    def test_simple_load_creation(self) -> None:
        """Creates an example load from an Obj with no expected errors"""
        test_object = self.model.get("child")
        test_object(parent=Const(None), test_id_col=Const(1), test_col=Const(None))

    def test_load_exceptions(self) -> None:
        """Creates an example load from an Obj and tests for the error messaging"""
        test_object = self.model.get("child")
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object()
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object(test_col=Const(None))
        with self.assertRaises(exceptions.DBgenMissingInfo):
            test_object(test_id_col=Const(1), test_col=Const(None))
        with self.assertRaises(exceptions.DBgenInvalidArgument):
            test_object(parent=Const(None), test_id_col=1, test_col=Const(None))
