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

"""utilities for hypothesis testing"""
from string import ascii_letters, ascii_lowercase, digits, punctuation

from hypothesis.strategies import booleans, integers, none, one_of, text

password_alpha = digits + ascii_letters + punctuation


anystrat = one_of(text(), booleans(), text(), integers(), none())
nonempty = text(min_size=1)
nonempty_limited = text(min_size=1, max_size=3)
letters = text(min_size=1, alphabet=ascii_lowercase)
letters_complex = text(min_size=1, alphabet=password_alpha)
