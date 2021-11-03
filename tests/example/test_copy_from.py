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

from pathlib import Path

from sqlmodel import create_engine


def main():
    engine = create_engine('postgresql://michaelstatt@localhost/sqlalchemy')
    file_name = Path(__file__).parent / 'test.csv'
    test_conn = engine.raw_connection()

    try:
        with test_conn.cursor() as curs:
            with open(file_name) as f:
                curs.copy_from(f, 'test', null="None", columns=['name', 'tags'], sep='|')
                f.seek(0)
                print(f.read())
        test_conn.commit()
    finally:
        test_conn.close()


if __name__ == '__main__':
    main()
