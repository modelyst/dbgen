# import asyncio
# import logging
# from datetime import datetime
# from io import StringIO

# from rich.console import Console
# from rich.layout import Layout
# from rich.live import Live
# from rich.panel import Panel
# from rich.table import Table
# from rich.logging import RichHandler

# console = Console()


# class Header:
#     """Display header with clock."""

#     def __rich__(self) -> Panel:
#         grid = Table.grid(expand=True)
#         grid.add_column(justify="center", ratio=1)
#         grid.add_column(justify="right")
#         grid.add_row(
#             "[bold]DBgen[/bold]",
#             datetime.now().ctime().replace(":", "[blink]:[/]"),
#         )
#         return Panel(grid, style="white on purple")


# class Dashboard:
#     def __init__(self, logging_level=logging.INFO) -> None:
#         self.logging_level = logging_level
#         self.messages: asyncio.Queue[str] = asyncio.Queue()
#         self.layout = self.make_layout()
#         self.logger = logging.getLogger(__name__)
#         self.logger.setLevel(self.logging_level)

#     def generate_table(self):
#         table = Table.grid(expand=True)
#         try:
#             while True:
#                 row = self.messages.get_nowait()
#                 table.add_row(row)
#         except asyncio.QueueEmpty:
#             pass
#         return Panel(table)

#     def make_layout(self):
#         layout = Layout(name='root')
#         layout.split(Layout(Header(), name='header', size=3), Layout(name='main'))
#         layout['main'].split_row(Layout(name='side'), Layout(name='body', ratio=2, minimum_size=60))
#         layout['main']['side'].update(self.generate_table())
#         return layout

#     async def run(self, func):
#         return await asyncio.gather(self.show(func))

#     def start(self, func):
#         asyncio.run(self.run(func))

#     async def log(self):
#         logging_io = StringIO()
#         logging_console = Console(file=logging_io, width=int(console.width / 2.0), force_interactive=False)
#         self.logger.addHandler(RichHandler(console=logging_console))
#         while True:
#             self.layout['main']["side"].update(self.generate_table())
#             await asyncio.sleep(0.5)

#     async def show(self, func):
#         layout = self.make_layout()
#         loop = asyncio.get_event_loop()
#         logger = asyncio.create_task(self.log())
#         try:
#             with Live(layout, screen=True):
#                 output = func()
#         finally:
#             logger.cancel()
#             try:
#                 await logger
#             except asyncio.CancelledError:
#                 pass
#         return output
