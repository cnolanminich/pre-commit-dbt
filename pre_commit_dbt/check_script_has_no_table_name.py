import argparse
import re
from pathlib import Path
from typing import Generator
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
import sqlfluff

from pre_commit_dbt.utils import add_filenames_args

REGEX_PARENTHESIS = r"([\(\)])"  # pragma: no mutate


def add_space_to_parenthesis(sql: str) -> str:
    return re.sub(REGEX_PARENTHESIS, r" \1 ", sql)


def has_table_name(
    sql: str, filename: str, dotless: Optional[bool] = False
) -> Tuple[int, Set[str]]:
    status_code = 0
    sql_clean = add_space_to_parenthesis(sql)
    parsed_sql = sqlfluff.parse(sql_clean)
    table_names = parsed_sql.tree.get_table_references()
    table_names_lower = [table.lower() for table in table_names]
    table_names_lower = set(table_names_lower)
    return status_code, table_names_lower


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    parser.add_argument("--ignore-dotless-table", action="store_true")

    args = parser.parse_args(argv)
    status_code = 0

    for filename in args.filenames:
        sql = Path(filename).read_text()
        status_code_file, tables = has_table_name(
            sql, filename, args.ignore_dotless_table
        )
        if status_code_file:
            result = "\n- ".join(list(tables))  # pragma: no mutate
            print(
                f"{filename}: "
                f"does not use source() or ref() macros for tables:\n- {result}",
            )
            status_code = status_code_file

    return status_code


if __name__ == "__main__":
    exit(main())
