# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import csv
import io
from unittest import TestCase
from unittest.mock import patch, MagicMock

from fileshovel.csvreader import CsvReader
from fileshovel.lineio import TellableLineIO

a_filename = "/nonexistent/file.csv"
default_encoding = "utf8"


class CsvReaderTest(TestCase):

	def _reader_for(self, content: bytes) -> CsvReader:
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=content)
		with patch("builtins.open", mock_file):
			lineio = TellableLineIO(a_filename, "rb", default_encoding)
		return CsvReader(lineio)

	def test_validCsv_iterate_yieldsAllRows(self):
		reader = self._reader_for(b"a,b,c\n1,2,3\n4,5,6\n")
		rows = [row for row, _line, _offset in reader]
		self.assertEqual(rows, [["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]])

	def test_malformedRecord_iterate_skipsBadRecordAndContinues(self):
		original_limit = csv.field_size_limit()
		csv.field_size_limit(64)
		try:
			huge_field = b"x" * 256
			content = b"a,b\n1,2\n" + huge_field + b",bad\n3,4\n"
			reader = self._reader_for(content)
			rows = [row for row, _line, _offset in reader]
		finally:
			csv.field_size_limit(original_limit)

		self.assertEqual(rows, [["a", "b"], ["1", "2"], ["3", "4"]])

	def test_badUtf8InQuotedField_iterate_doesNotRaise(self):
		content = b'"Salle Conf\xc3","8194998499","public"\n"row","two","three"\n'
		reader = self._reader_for(content)
		rows = [row for row, _line, _offset in reader]
		self.assertEqual(len(rows), 2)
		self.assertIn("�", rows[0][0])
		self.assertEqual(rows[1], ["row", "two", "three"])
