# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import csv
from typing import Iterable, Tuple

from fileshovel.lineio import TellableLineIO


class CsvReader:

	def __init__(self, csv_file: TellableLineIO, *args, **kwargs):
		"""Wrapper around 'csv.reader' to iterate over line and offset."""
		self.csv_file = csv_file
		self.reader = csv.reader(csv_file, *args, **kwargs)

	def __iter__(self) -> Iterable[Tuple[str, int, int]]:
		csv_file = self.csv_file

		for line in self.reader:
			yield line, csv_file.current_line, csv_file.current_line_offset
