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

from typing import Any
from typing import Generator as GenType
from typing import List, Mapping, Optional

from sqlalchemy.future import Engine
from sqlmodel import Session, func, select

from dbgen.core.metadata import ETLStepEntity, ETLStepRunEntity, RunEntity


def get_runs(
    run_id: Optional[int],
    meta_engine: "Engine",
    all_runs: bool = True,
    accepted_statuses: List[str] = None,
    last: Optional[int] = 1,
) -> GenType[Mapping[str, Any], None, None]:
    statement = select(ETLStepEntity.name, *ETLStepRunEntity.__table__.c).join_from(ETLStepRunEntity, ETLStepEntity).order_by(ETLStepRunEntity.run_id.desc(), ETLStepRunEntity.ordering)  # type: ignore
    if accepted_statuses:
        statement = statement.where(ETLStepRunEntity.status.in_(accepted_statuses))  # type: ignore
    if not all_runs:
        statement = statement.where(
            ETLStepRunEntity.run_id > select(func.max(RunEntity.id)).scalar_subquery() - last  # type: ignore
        )
    with Session(meta_engine) as session:
        result = session.exec(statement).mappings()
        yield from result
