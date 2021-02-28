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

"""hypothesis testing setup"""
import os

from hypothesis import Verbosity, settings

# Register three profiles for varying degrees of testing
settings.register_profile("ci", max_examples=1000)
settings.register_profile("dev", max_examples=100)
settings.register_profile("debug", max_examples=30, verbosity=Verbosity.verbose)
# Load the profile from the environmental variable
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))
