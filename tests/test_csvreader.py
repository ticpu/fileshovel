# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import csv
import io
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from fileshovel.csvreader import CsvReader, RecordError
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

	def test_malformedRecord_iterate_stopsNamingLineAndOffset(self):
		original_limit = csv.field_size_limit()
		csv.field_size_limit(64)
		rows = []
		try:
			huge_field = b"x" * 256
			content = b"a,b\n1,2\n" + huge_field + b",bad\n3,4\n"
			with self.assertRaisesRegex(RecordError, "line 3, offset 8 "):
				for row, _line, _offset in self._reader_for(content):
					rows.append(row)
		finally:
			csv.field_size_limit(original_limit)

		self.assertEqual(rows, [["a", "b"], ["1", "2"]])

	def test_badUtf8InQuotedField_iterate_doesNotRaise(self):
		content = b'"Salle Conf\xc3","8194998499","public"\n"row","two","three"\n'
		reader = self._reader_for(content)
		rows = [row for row, _line, _offset in reader]
		self.assertEqual(len(rows), 2)
		self.assertIn("�", rows[0][0])
		self.assertEqual(rows[1], ["row", "two", "three"])


def read_all(content: bytes, last_offset: int, skip_lines: int):
	with os.fdopen(os.open(os.path.dirname(__file__), os.O_TMPFILE | os.O_RDWR), "r+b") as f:
		f.write(content)
		f.seek(0)

		with patch("builtins.open", MagicMock(return_value=f)):
			csv_file = TellableLineIO(a_filename, "rb", default_encoding, skip_lines=skip_lines)
			return [line for line, _, _ in CsvReader(csv_file, last_offset=last_offset)]


class CsvReaderResumeTest(TestCase):

	def test_offsetOfLastInsertedRow_resume_readsFollowingRows(self):
		rows = read_all(b"h1,h2\na,1\nb,2\nc,3\n", last_offset=len(b"h1,h2\na,1\n"), skip_lines=1)
		self.assertEqual(rows, [["c", "3"]])

	def test_offsetEqualsFileSize_resume_readsNothing(self):
		rows = read_all(b"a,1\n", last_offset=4, skip_lines=0)
		self.assertEqual(rows, [])

	def test_offsetBeyondFileSize_resume_readsFromStartSkippingHeader(self):
		with self.assertLogs("fileshovel.csvreader", "WARNING"):
			rows = read_all(b"h1,h2\na,1\n", last_offset=1000, skip_lines=1)
		self.assertEqual(rows, [["a", "1"]])
