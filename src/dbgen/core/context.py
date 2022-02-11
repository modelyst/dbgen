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

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Dict

from pydantic import BaseModel

if TYPE_CHECKING:
    from contextvars import Token  # pragma: no cover


class BaseContext(BaseModel):
    context_dict: Dict[str, Any]
    __context__: ContextVar
    _token: 'Token'

    def __enter__(self):
        token = self.__context__.set(self.context_dict)
        object.__setattr__(self, '_token', token)
        return self.context_dict

    def __exit__(self, *_):
        self.reset()

    def reset(self):
        self.__context__.reset(getattr(self, '_token'))

    @classmethod
    def get(cls):
        return cls.__context__.get(None)


class ModelContext(BaseContext):
    __context__: ContextVar = ContextVar('model')


class ETLStepContext(BaseContext):
    __context__: ContextVar = ContextVar('etl_step')
