# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import io
import os
import threading
from tempfile import mktemp
from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock, Mock

from fileshovel.lineio import TellableLineIO

a_filename = "/nonexistent/file.txt"
default_encoding = "utf8"


class TellableLineIOTest(TestCase):

	def test_emptyFile_readFile_returnsEmpty(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding)
			# noinspection PyUnusedLocal
			line = None

			with self.assertRaises(StopIteration) as e:
				line = next(iter(t))

			self.assertIsNotNone(e, "there shouldn't be any line to read but we got one: %s" % line)

	def test_oneCompleteLineFile_readFile_returnsOneLine(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"one line\n")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding)
			lines = list(t)
			self.assertEqual(len(lines), 1)

	def test_oneCompleteLineAndOneIncompleteLineFile_readFile_returnsOneLine(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"one line\none incomplete line")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding)
			line_count = 0

			for line in t:
				line_count += 1
				self.assertIn('\n', line)
				self.assertEqual(line_count, t.current_line)

		self.assertEqual(line_count, 1, "found an incomplete line in output")

	def test_threeLinesFile_readLineGetOffset_returnsCorrectOffset(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"012\n456\n890")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding)

			for line in t:
				self.assertEqual(t.current_line_offset, int(line[0]))

		mock_file.assert_called_once()

	def test_emptyFile_getEncoding_returnsPassedEncoding(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding)

		self.assertEqual(t.encoding, default_encoding)

	def test_emptyFile_openWithoutBinary_binaryModeAddedStillReturnsString(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"line\n")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "r", default_encoding)

			for line in t:
				self.assertEqual(type(line), str)

		mock_file.assert_called_once_with(a_filename, "rb")

	def test_3linesFile_skip2andRead_readsOneLine(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"line1\nline2\nline3\n")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding, skip_lines=2)
			line_count = 0

			for _ in t:
				line_count += 1

		self.assertEqual(line_count, 1)

	def test_3linesFile_readWithRegex_regexIsAppliedToOutput(self):
		mock_file = MagicMock()
		mock_file.return_value = io.BytesIO(initial_bytes=b"aline1\nbline2\ncline3\ndline4\n")

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding, regex_search=br"^([ab]).in", regex_replace=br"\1pin")

			for line in t:
				if t.current_line in [0, 1]:
					self.assertIn("pine", line, msg="regex conversion has failed")

	def test_10linesFile_skip2andReadEvery2_reads4Lines(self):
		mock_file = MagicMock()
		ten_lines = b"""1skip
		2skip
		3skip_nth
		4read
		5skip_nth
		6read
		7skip_nth
		8read
		9skip_nth
		0read
		"""
		mock_file.return_value = io.BytesIO(initial_bytes=ten_lines)

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding, skip_lines=2, every_nth=2)
			read_lines = 0
			last_read = None

			for line in t:
				read_lines += 1
				last_read = line
				self.assertIn("read", line)

			self.assertEqual(read_lines, 4, msg="last line read was %s" % last_read)

	@staticmethod
	def _twoLinesFileUsingInotify_readWholeFileAddNewLine_readsThirdLine(test_file_name, test_file, content: bytes,
			stop_at_line: int):
		test_file.write(content)
		test_file.flush()
		line = None
		line_count = 0
		t = TellableLineIO(test_file_name, "rb", default_encoding, watch=True, use_inotify=True)
		t.read = Mock(side_effect=t.read)
		t.readline = Mock(side_effect=t.readline)
		t.readlines = Mock(side_effect=t.readlines)

		for line in t:
			line_count += 1
			if line_count == stop_at_line:
				break

		return line, line_count

	def test_twoLinesFileUsingInotify_readWholeFileAddNewLine_readsThirdLine(self):
		def append_to_test_file(filename):
			with open(filename, "ab") as f:
				f.write(b"90\n")
				f.flush()

		test_file_name = None

		try:
			test_file_name = mktemp()
			threading.Timer(0.2, append_to_test_file, (test_file_name,)).start()
			with os.fdopen(os.open(test_file_name, os.O_RDWR | os.O_CREAT | os.O_TRUNC), "r+b") as test_file:
				line, line_count = self._twoLinesFileUsingInotify_readWholeFileAddNewLine_readsThirdLine(
					test_file_name,
					test_file,
					b"012\n456\n8",
					3,
				)
				self.assertEqual(3, line_count)
				self.assertEqual("890\n", line)

		finally:
			if test_file_name and os.path.isfile(test_file_name):
				os.remove(test_file_name)

	def test_twoLinesFileUsingInotify_deleteFileWriteNewLine_fileCallsCloseReturnsThirdLine(self):
		def remove_test_file(filename):
			os.remove(filename)
			with open(filename, "wb") as f:
				f.write(b"890\n")
				f.flush()

		test_file_name = None
		line_count = 0
		line = None

		try:
			test_file_name = mktemp()
			threading.Timer(0.2, remove_test_file, (test_file_name,)).start()
			with open(test_file_name, "w+b") as test_file:
				test_file.write(b"123\n456\n")
				test_file.flush()
			t = TellableLineIO(test_file_name, "rb", default_encoding, watch=True, use_inotify=True)
			real_close = t._file.close
			mock_close = MagicMock(side_effect=real_close)
			t._file.close = mock_close
			for line in t:
				line_count += 1
				if line_count == 3:
					break

			self.assertEqual("890\n", line)
			mock_close.assert_called_once()

		finally:
			if test_file_name and os.path.isfile(test_file_name):
				os.remove(test_file_name)

	def test_twoLineAndAHalfFile_readWholeFileCompleteLine_readsThirdLine(self):
		mock_file = MagicMock()
		test_file = os.fdopen(os.open("/tmp", os.O_TMPFILE | os.O_RDWR), "r+b")
		test_file.write(b"012\n456\n")
		test_file.flush()
		mock_file.return_value = os.fdopen(os.dup(test_file.fileno()), "rb")
		mock_file.return_value.seek(0)
		line_count = 0

		with patch("builtins.open", mock_file):
			t = TellableLineIO(a_filename, "rb", default_encoding, watch=True)

			for line in t:
				line_count += 1
				if line_count == 2:
					test_file.write(b"890\n")
					test_file.flush()
				elif line_count == 3:
					self.assertEqual("890\n", line)
					t.watch = False

		self.assertEqual(line_count, 3)
		mock_file.assert_called_once()
