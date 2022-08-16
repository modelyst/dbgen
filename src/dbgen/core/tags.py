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

import contextlib

from dbgen.core.context import TagsContext


@contextlib.contextmanager
def tags(*new_tags: str):
    # Get any tags set in outer contexts
    upper_context = TagsContext.get()
    upper_tags = upper_context.get('tags', []) if upper_context else []
    # Concatenate the tags together and set new lower context
    lower_tags = [*upper_tags, *new_tags]
    lower_context = TagsContext(context_dict={'tags': lower_tags})
    yield lower_context.__enter__()
    # Clean up the context to reset to the outer context
    lower_context.__exit__()
