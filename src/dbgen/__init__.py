#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Welcome to dbgen"""

from dbgen.version import git_version, version

__author__ = "Modelyst LLC"
__email__ = "info@modelyst.io"
__maintainer__ = "Michael Statt"
__maintainer_email__ = "michael.statt@modelyst.io"
__version__ = version
__gitversion__ = git_version
# Build Logo for easy CLI version checking
git_ver_str = f"GITVERSION: {__gitversion__}\n" if __gitversion__ else ""
LOGO = f"""
-------------------------------
    ____  ____
   / __ \\/ __ )____ ____  ____
  / / / / __  / __ `/ _ \\/ __ \\
 / /_/ / /_/ / /_/ /  __/ / / /
/_____/_____/\\__, /\\___/_/ /_/
            /____/
-------------------------------
VERSION: {__version__}
{git_ver_str}-------------------------------
"""

# Imports
from dbgen.core.expr.expr import (
    ABS,
    AND,
    ARRAY,
    ARRAY_AGG,
    AVG,
    BINARY,
    CASE,
    COALESCE,
    CONCAT,
    CONVERT,
    COUNT,
    EQ,
    GE,
    GROUP_CONCAT,
    GT,
    IF_ELSE,
    IN,
    JSON_EXTRACT,
    LE,
    LEFT,
    LEN,
    LIKE,
    LT,
    MAX,
    MIN,
    NE,
    NOT,
    NULL,
    OR,
    POSITION,
    R2,
    REGEXP,
    REPLACE,
    RIGHT,
    STD,
    SUBSELECT,
    SUM,
    Expr,
    Literal,
    One,
    Sum,
    Tup,
    Zero,
    false,
    toDecimal,
    true,
)
from dbgen.core.expr.pathattr import PathAttr
from dbgen.core.expr.sqltypes import (
    JSON,
    JSONB,
    Boolean,
    Date,
    Decimal,
    Double,
    Int,
    SQLType,
    Text,
    Timestamp,
    Varchar,
)
from dbgen.core.fromclause import Path as JPath
from dbgen.core.func import Env, Func, Import
from dbgen.core.funclike import Arg, Const, PyBlock
from dbgen.core.gen import Generator
from dbgen.core.gen import Generator as Gen
from dbgen.core.misc import ConnectInfo
from dbgen.core.model.model import Model
from dbgen.core.query import Query
from dbgen.core.schema import Attr, Entity, Path, PathEQ, QView, RawView
from dbgen.core.schema import UserRel as Rel
from dbgen.utils.lists import concat_map, flatten, merge_dicts, nub
from dbgen.utils.parsing import parser

############################
from dbgen.utils.sql import sqlexecute, sqlselect
