#!/usr/bin/env python3
import logging
import sys

from fileshovel.csvreader import RecordError
from fileshovel.pgsql import InsertError, PgLineInserter
from fileshovel.options import ConfigError, FileShovelOptions

log = logging.getLogger("fileshovel.main")


def main() -> int:
	args = FileShovelOptions()
	log_levels = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
	logging.basicConfig(level=log_levels[max(0, min(args.verbose, len(log_levels) - 1))])

	try:
		index = PgLineInserter(args)
		offset = index.get_last_offset_from_database()
		reader = args.get_csv_file_reader(last_offset=offset, on_idle=index.flush)

		for line, current_line, current_line_offset in reader:
			index.add_row(line, current_line, current_line_offset)

		index.flush()

	except (ConfigError, InsertError, RecordError) as e:
		log.error("%s", e)
		return 1
	except KeyboardInterrupt:
		return 130
	except Exception:
		log.exception("stopping on unexpected error")
		return 1

	return 0


if __name__ == "__main__":
	sys.exit(main())
