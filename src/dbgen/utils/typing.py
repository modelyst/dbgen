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

"""Store useful python type hints for use in project."""
from typing import Any, Callable, Dict, Optional, Type, Union
from uuid import UUID

import sqlalchemy.types as sa_types

COLUMN_TYPE = Union[Type[sa_types.TypeEngine], sa_types.TypeEngine]
NoArgAnyCallable = Callable[[], Any]
IDType = Optional[UUID]
NAMESPACE_TYPE = Dict[str, Dict[str, Any]]
