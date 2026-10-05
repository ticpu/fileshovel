# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import io
import logging
import os
import re
import time
from typing import Iterable, Optional

import pyinotify
from pyinotify import WatchManager, Notifier

log = logging.getLogger("fileshovel.lineio")


class TellableLineIO(io.TextIOBase):

	def __init__(self, filename, mode, encoding, skip_lines=0, watch=0, use_inotify=False,
			regex_search=None, regex_replace: bytes = None, on_idle=None):
		if 'b' not in mode:
			mode += 'b'

		if regex_search and regex_replace is None:
			raise RuntimeError("regex_replace must be set if regex_search is set")

		if regex_search and type(regex_search) == bytes:
			regex_search = re.compile(regex_search)

		self._file = None
		self._encoding = encoding
		self.filename = filename
		self.mode = mode
		self.skip_lines = skip_lines
		self.watch = watch
		self.open_file()
		self._use_inotify = use_inotify
		self._resuming = False
		self.regex_search = regex_search
		self.regex_replace = regex_replace
		self.current_line = 0
		self.current_line_offset = 0
		self.on_idle = on_idle

	def open_file(self):
		if self._file:
			self._file.close()
		self._file = open(self.filename, self.mode)

	def get_size(self) -> int:
		return os.fstat(self._file.fileno()).st_size

	def resume_after(self, offset: int):
		"""Continue after the line starting at offset instead of skipping skip_lines."""
		size = self.get_size()
		if offset > size:
			raise ValueError("offset %d is beyond the size of %s (%d)" % (offset, self.filename, size))
		log.debug("seeking to %d", offset)
		self._file.seek(offset)
		self._resuming = True

	def close(self):
		if self._file:
			self._file.close()
		super().close()

	def tell(self) -> int:
		return self._file.tell()

	@property
	def encoding(self) -> str:
		return self._encoding

	def _setup_notifier(self) -> Optional[Notifier]:
		if self._use_inotify:
			watch_manager = WatchManager()
			watch_manager.add_watch(
				os.path.dirname(os.path.abspath(self.filename)),
				pyinotify.IN_MODIFY | pyinotify.IN_CREATE | pyinotify.IN_MOVED_TO,
				quiet=False,
			)
			return Notifier(watch_manager, default_proc_fun=lambda event: None)

	def _wait(self, notifier: Optional[Notifier]):
		if notifier:
			if notifier.check_events(timeout=60000):
				notifier.read_events()
				notifier.process_events()
		else:
			time.sleep(self.watch)

	def _replaced(self) -> bool:
		try:
			path_stat = os.stat(self.filename)
		except FileNotFoundError:
			log.debug("%s does not exist, keeping the open file", self.filename)
			return False
		file_stat = os.fstat(self._file.fileno())
		return (path_stat.st_dev, path_stat.st_ino) != (file_stat.st_dev, file_stat.st_ino)

	def __iter__(self) -> Iterable[str]:
		notifier = self._setup_notifier()
		try:
			yield from self._read_lines(notifier)
		finally:
			if notifier:
				notifier.stop()

	def _read_lines(self, notifier: Optional[Notifier]) -> Iterable[str]:
		regex_search = self.regex_search
		regex_replace = self.regex_replace
		current_line = 0
		to_skip = 1 if self._resuming else self.skip_lines
		eof_logged = False
		replaced = False

		while True:
			start = self._file.tell()
			line = self._file.readline()

			if line[-1:] == b"\n":
				if to_skip:
					to_skip -= 1
					continue

				current_line += 1

				if regex_search and regex_replace:
					line = regex_search.sub(regex_replace, line)

				self.current_line = current_line
				self.current_line_offset = start
				try:
					decoded = str(line, self._encoding)
				except UnicodeDecodeError as e:
					log.warning("decode error at line %d offset %d: %s; using replacement characters",
							current_line, start, e)
					decoded = str(line, self._encoding, errors="replace")
				yield decoded
				continue

			self._file.seek(start)

			if not eof_logged:
				eof_logged = True
				log.info("end of file has been reached for %s", self.filename)

			if replaced or not self.watch:
				if line:
					log.warning("ignoring incomplete last line at offset %d of %s", start, self.filename)
				if not self.watch:
					log.debug("reached end of file and not watching, closing")
					return
				log.info("%s was replaced, reopening", self.filename)
				self.open_file()
				to_skip, current_line, eof_logged, replaced = self.skip_lines, 0, False, False
				continue

			if self.on_idle:
				self.on_idle()

			self._wait(notifier)

			if self._replaced():
				replaced = True
			elif self.get_size() < start:
				log.warning("%s was truncated, reading from the start", self.filename)
				self._file.seek(0)
				to_skip, current_line = self.skip_lines, 0
