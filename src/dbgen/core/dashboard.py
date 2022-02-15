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

from contextlib import contextmanager
from enum import Enum
from functools import partial
from typing import Callable, Dict

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table


class BarNames(str, Enum):
    EXTRACTED = 'Extracted'
    LAUNCHED = 'Launched'
    TRANSFORMED = 'Transformed'
    LOADED = 'Loaded'
    OVERALL = ''


class Dashboard:
    bars: Dict[BarNames, Callable]

    def __init__(self, console: Console = None, enable: bool = True) -> None:
        self.console = console
        self.enable = enable
        self.bars = {}
        self.refresh_per_second = 8

    def make_table(self, progress):
        if not self.enable:
            return
        progress_table = Table.grid(expand=True)
        progress_table.add_row(
            Panel(self.overall_progress, title="Overall Progress", border_style="green", padding=(2, 2)),
            Panel(progress, title="[b]ETL Step", border_style="red", padding=(1, 2)),
        )

        return progress_table

    @property
    def etl_step_progress(self):
        return Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            "[progress.completed]{task.completed:>3.0f} rows",
            refresh_per_second=self.refresh_per_second,
            transient=True,
        )

    @contextmanager
    def show(self, total: int):
        self.overall_length = total
        self.overall_progress = Progress(TimeElapsedColumn(), BarColumn(), TextColumn("{task.description}"))
        generator_bar = self.overall_progress.add_task(BarNames.OVERALL, total=total)
        self.bars[BarNames.OVERALL] = partial(self.overall_progress.update, generator_bar)
        if not self.enable:
            yield self
            return
        with Live(
            self.make_table(self.etl_step_progress),
            console=self.console,
            refresh_per_second=self.refresh_per_second,
            transient=True,
        ) as live:
            self.live = live
            yield self

    def set_etl_name(self, etl_step_name: str, order: int = None):
        self.advance_bar(
            BarNames.OVERALL,
            advance=0,
            description=f"[bold #AAAAAA]{etl_step_name} ({order}/{self.overall_length})",
        )

    def finish(self):
        self.advance_bar(BarNames.OVERALL, advance=0, description='Finished!')

    def set_total(self, total):
        for bar in self.bars:
            if bar != BarNames.OVERALL:
                self.advance_bar(bar, advance=0, total=total, start=True)

    def add_etl_progress_bars(self, total: int = None):
        if not self.enable:
            return
        progress = self.etl_step_progress
        self.live.update(self.make_table(progress))
        for text in (BarNames.EXTRACTED, BarNames.TRANSFORMED, BarNames.LOADED):
            bar = progress.add_task(
                text,
                start=total is not None,
                total=total or 1000000,
            )
            self.bars[text] = partial(progress.update, bar)

    def advance_bar(self, bar_name: BarNames, advance: int = 1, description: str = None, **kwargs):
        if self.enable:
            self.bars[bar_name](advance=advance, description=description, **kwargs)
