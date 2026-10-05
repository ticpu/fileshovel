# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import os
import queue
import tempfile
import threading
from unittest import TestCase

from fileshovel.lineio import TellableLineIO


def append(path, data: bytes):
	with open(path, "ab") as f:
		f.write(data)


class Follower:
	"""Iterates a watched TellableLineIO on a daemon thread and hands lines back with their offsets."""

	def __init__(self, path, **kwargs):
		self.reader = TellableLineIO(path, "rb", "utf8", watch=True, **kwargs)
		self._lines = queue.Queue()
		threading.Thread(target=self._run, daemon=True).start()

	def _run(self):
		try:
			for line in self.reader:
				self._lines.put((line, self.reader.current_line_offset))
		except Exception as e:
			self._lines.put(e)

	def take(self, count, timeout=5):
		taken = []
		for _ in range(count):
			item = self._lines.get(timeout=timeout)
			if isinstance(item, Exception):
				raise item
			taken.append(item)
		return taken


class RotationTest(TestCase):

	def setUp(self):
		self.scratch = tempfile.TemporaryDirectory(dir=os.path.dirname(__file__), prefix="scratch-")
		self.path = os.path.join(self.scratch.name, "Master.csv")

	def tearDown(self):
		self.scratch.cleanup()

	def test_renamedTwice_appendsBetween_readsEveryLine(self):
		append(self.path, b"a\n")
		follower = Follower(self.path, use_inotify=True)
		self.assertEqual(follower.take(1), [("a\n", 0)])

		for generation in (1, 2):
			os.rename(self.path, "%s.%d" % (self.path, generation))
			append(self.path, b"g%d\n" % generation)
			self.assertEqual(follower.take(1), [("g%d\n" % generation, 0)])

	def test_renamedToEmptyFile_thenAppended_skipsHeaderAndReadsNewLines(self):
		append(self.path, b"header\nold1\nold2\n")
		follower = Follower(self.path, use_inotify=True, skip_lines=1)
		self.assertEqual(follower.take(2), [("old1\n", 7), ("old2\n", 12)])

		os.rename(self.path, self.path + ".1")
		append(self.path, b"")
		append(self.path, b"header\nnew1\nnew2\n")
		self.assertEqual(follower.take(2), [("new1\n", 7), ("new2\n", 12)])

	def test_renamed_oldFileAppendedBeforeRename_readsOldTailFirst(self):
		append(self.path, b"a\n")
		follower = Follower(self.path, use_inotify=True)
		follower.take(1)

		append(self.path, b"b\n")
		os.rename(self.path, self.path + ".1")
		append(self.path, b"c\n")
		self.assertEqual([line for line, _ in follower.take(2)], ["b\n", "c\n"])

	def test_partialLineLongerThanPrevious_completed_readsWholeLine(self):
		append(self.path, b"aa\nbbbbbbbbbb")
		follower = Follower(self.path, use_inotify=True)
		self.assertEqual(follower.take(1), [("aa\n", 0)])

		append(self.path, b"\ncc\n")
		self.assertEqual(follower.take(2), [("bbbbbbbbbb\n", 3), ("cc\n", 14)])

	def test_chmod_doesNotReread(self):
		append(self.path, b"a\n")
		follower = Follower(self.path, use_inotify=True)
		follower.take(1)

		os.chmod(self.path, 0o600)
		append(self.path, b"b\n")
		self.assertEqual(follower.take(1), [("b\n", 2)])

	def test_truncatedAfterEarlierAppend_readsFromStart(self):
		append(self.path, b"aaaa\n")
		follower = Follower(self.path, use_inotify=True)
		follower.take(1)
		append(self.path, b"bbbb\n")
		follower.take(1)

		os.truncate(self.path, 0)
		append(self.path, b"c\n")
		self.assertEqual(follower.take(1), [("c\n", 0)])

	def test_emptyFileWithHeaderToSkip_notWatched_readsNothing(self):
		append(self.path, b"")
		reader = TellableLineIO(self.path, "rb", "utf8", skip_lines=1)
		self.assertEqual(list(reader), [])
