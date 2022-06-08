# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import logging
import os
import pickle
from datetime import datetime
from typing import List
from uuid import UUID

from options import FileShovelOptions

log = logging.getLogger("fileshovel.indexer")


class CsvIndexOffset:
	def __init__(self, line: int, offset: int, date: datetime, uuid: UUID = None):
		self.line = line
		self.offset = offset
		self.date = date
		self._uuid = uuid.bytes

	@property
	def uuid(self):
		return UUID(bytes=self._uuid)

	@uuid.setter
	def uuid(self, uuid: UUID):
		if uuid:
			self._uuid = uuid.bytes
		else:
			self._uuid = None


class CsvIndex:
	CURRENT_VERSION = 3

	def __init__(self, columns: List[str], date_column: int, date_format: str):
		self.version = CsvIndex.CURRENT_VERSION
		self.columns = columns
		self.date_column = date_column
		self.date_format = date_format
		self.date_index = {}
		self.uuid_index = {}

	def is_compatible(self, other_index) -> bool:
		if isinstance(other_index, CsvIndex):
			return self.version == other_index.version and \
					len(self.columns) == len(other_index.columns) and \
					self.date_column == other_index.date_column and \
					self.date_format == other_index.date_format and \
					isinstance(next(iter(other_index.date_index.values())), CsvIndexOffset)
		return False

	def add_index(self, line: int, offset_index: int, dt: datetime, uuid: UUID = None) -> CsvIndexOffset:
		offset_index = CsvIndexOffset(line, offset_index, dt, uuid)
		self.date_index[dt] = offset_index
		if uuid:
			self.uuid_index[uuid] = offset_index
		return offset_index

	def find_offset_from_datetime(self, dt: datetime) -> CsvIndexOffset:
		return self.date_index[dt]

	def find_offset_from_uuid(self, uuid: UUID) -> CsvIndexOffset:
		return self.uuid_index[uuid]


class CsvIndexer:

	def __init__(self, options: FileShovelOptions):
		self._options = options
		if os.path.isfile(self._options.index_file) and os.stat(self._options.index_file).st_size > 0:
			self.index = self.load()
		else:
			self.index = self.build_new_index()
			self.save()

	def load(self) -> CsvIndex:
		try:
			with open(self._options.index_file, 'rb') as index_file:
				index = pickle.load(index_file)
			if not isinstance(index, CsvIndex):
				raise TypeError("invalid type in pickle file: %s" % type(index))

			if not self.create_index().is_compatible(index):
				raise ValueError("index format on disk isn't compatible with options")
		except Exception as e:
			log.warning("index is corrupt or empty: %s", e)
			raise

		return index

	def save(self):
		with open(self._options.index_file, 'wb') as index_file:
			pickle.dump(self.index, index_file)

	def create_index(self) -> CsvIndex:
		return CsvIndex(
			self._options.columns,
			self._options.date_column,
			self._options.csv_date_format,
		)

	def build_new_index(self) -> CsvIndex:
		index = self.create_index()
		reader = self._options.get_csv_file_reader()
		date_column = self._options.date_column
		uuid_column = self._options.uuid_column
		date_format = self._options.csv_date_format

		for line in range(self._options.csv_skip_lines):
			next(reader)

		for line, current_line, current_line_offset in reader:
			index.add_index(
				current_line,
				current_line_offset,
				datetime.strptime(line[date_column], date_format),
				UUID(line[uuid_column]) if uuid_column else None,
			)

		return index
