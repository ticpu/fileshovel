#!/usr/bin/env python3
import logging

from fileshovel.pgsql import PgLineInserter
from options import FileShovelOptions

log = logging.getLogger("fileshovel.main")


def main():
	args = FileShovelOptions()
	index = PgLineInserter(args)
	reader = args.get_csv_file_reader()
	offset = index.get_last_offset_from_database()

	if offset > 0:
		reader.csv_file.skip_lines = 0
		reader.csv_file.seek(offset)

	for line, current_line, current_line_offset in reader:
		if reader.reader.line_num % args.pg_rows_per_commit == 0:
			log.info("add %d row now at %d rows", args.pg_rows_per_commit, reader.reader.line_num)
		index.add_row(line, current_line, current_line_offset)

	log.info("done at %d rows", reader.reader.line_num)
	index.done()


if __name__ == "__main__":
	logging.basicConfig(level=logging.DEBUG)
	main()
