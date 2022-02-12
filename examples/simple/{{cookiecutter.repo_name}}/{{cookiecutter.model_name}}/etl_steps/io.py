from dbgen import Constant, ETLStep, Model
from dbgen.core.node.common_extractors import FileExtractor

from ..constants import DATA_PATH
from ..schema import MyTable
from ..transforms.io import simple_io


def add_io_etl_steps(model: Model):
    with model:
        with ETLStep("dummy_etl_step"):
            MyTable.load(insert=True, label=Constant("test_label"))

        with ETLStep("read_file"):
            file_names = FileExtractor(directory=DATA_PATH, extension="txt").results()
            file_contents = simple_io(file_names).results()
            MyTable.load(insert=True, label=file_contents)
