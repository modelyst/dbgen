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

import traceback
from bdb import BdbQuit
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple
from uuid import UUID

from pydantic import PrivateAttr
from pydasher import hasher

from dbgen._enum import RunStatus
from dbgen.core.base import Base, encoders
from dbgen.core.etl_step import ETLStep
from dbgen.core.model_settings import BaseModelSettings
from dbgen.core.node.extract import Extract
from dbgen.core.run.utilities import RunConfig


class TestRunResults(Base):
    number_of_extracted_rows: int = 0
    status: RunStatus = RunStatus.initialized


class ETLStepTestRunner(Base):
    _etl_step: ETLStep = PrivateAttr(None)

    def test(
        self,
        etl_step: ETLStep,
        test_rows: Optional[Sequence[Any]] = None,
        settings: Optional[BaseModelSettings] = None,
    ):
        self._etl_step = etl_step
        run_config: RunConfig = RunConfig(settings=settings) if settings else RunConfig()
        extract = etl_step.extract
        results = TestRunResults()
        extract._set_run_config(run_config)
        with extract:
            # Start while loop to iterate through the nodes
            self._logger.debug('Looping through extracted rows...')

            for batch_ind, batch in enumerate(self.batchify(extract, 1)):
                (
                    _,
                    rows_to_load,
                    rows_processed,
                    inputs_skipped,
                    output,
                    exc,
                ) = self.transform_batch(batch, run_config)
                # Check if transforms or loads raised an error
                self._logger.info(f'Batch {batch_ind} processed')
                self._logger.info(f'Skipped {inputs_skipped} rows int batch')
                self._logger.info(f'Processed {rows_processed} rows int batch')
                self._logger.info(f'Namespace: {output}')
                if exc:
                    msg = f"Error when running etl_step {self._etl_step.name}"
                    self._logger.error(msg)
                    results.status = RunStatus.failed
                    return results
                self._fake_load(etl_step, rows_to_load)
                results.number_of_extracted_rows += len(batch)
                results.status = RunStatus.completed
        return results

    def _fake_load(self, etl_step: ETLStep, rows_to_load: Dict[str, Dict[UUID, Any]]):
        for load in etl_step._sorted_loads():
            self._logger.debug(f'Loading into {load}')
            rows = rows_to_load[load.hash]
            for row in rows:
                self._logger.info(f'Attempting to load row: {row}')

    def transform_batch(self, batch: List[Tuple[UUID, Dict[str, Dict[str, Any]]]], run_config: 'RunConfig'):
        """Transform a batch of extracted namespaces."""
        # initialize the master dict for the rows that will need to be loaded after
        rows_to_load: Dict[str, Dict[UUID, dict]] = {node.hash: {} for node in self._etl_step.loads}
        processed_hashes = []
        inputs_skipped = 0
        for input_hash, row in batch:
            try:
                output, skipped = self._etl_step._transform(row, rows_to_load, run_config)
                if not skipped:
                    processed_hashes.append(input_hash)
                else:
                    inputs_skipped += 1
            except (KeyboardInterrupt, SystemExit, BdbQuit):
                raise
            except BaseException:
                return None, None, None, inputs_skipped, None, traceback.format_exc()
        return processed_hashes, rows_to_load, len(batch), inputs_skipped, output, None

    def batchify(
        self,
        extract: Extract,
        batch_size: int,
        input_rows: List[dict] = None,
    ) -> Generator[Any, None, None]:
        # initialize the batch and counts
        batch = []
        generator = extract.extract() if not input_rows else input_rows
        # Loop the the rows in the extract function
        for row in generator:
            # Check the hash of the inputs against the metadatabase
            processed_row = extract.process_row(row)
            input_hash = self._get_hash(processed_row, self._etl_step.uuid)
            batch.append((input_hash, {extract.hash: processed_row}))

            # If batch size is reached advance bar and yield
            if len(batch) == batch_size:
                yield batch
                batch = []

    def _get_hash(self, extracted_dict: Dict[str, Any], etl_step_uuid: UUID) -> UUID:
        # Convert Row to a dictionary so we can hash it for repeat-checking
        input_hash = UUID(hasher((etl_step_uuid, extracted_dict), encoders=encoders))
        return input_hash
