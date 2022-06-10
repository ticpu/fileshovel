#!/usr/bin/env python3
import logging

from fileshovel.pgsql import PgLineInserter
from fileshovel.options import FileShovelOptions

log = logging.getLogger("fileshovel.main")


def main():
	args = FileShovelOptions()
	log_level = {
		0: logging.CRITICAL,
		1: logging.ERROR,
		2: logging.WARN,
		3: logging.INFO,
		4: logging.DEBUG,
	}.get(args.verbose)
	logging.basicConfig(level=log_level)
	index = PgLineInserter(args)
	offset = index.get_last_offset_from_database()
	reader = args.get_csv_file_reader(last_offset=offset)

	try:
		for line, current_line, current_line_offset in reader:
			if reader.reader.line_num % args.pg_rows_per_commit == 0:
				log.info("add %d row now at %d rows", args.pg_rows_per_commit, reader.reader.line_num)
			index.add_row(line, current_line, current_line_offset)

	except KeyboardInterrupt:
		pass
	finally:
		index.done()

	log.info("done at %d rows", reader.reader.line_num)


if __name__ == "__main__":
	main()
