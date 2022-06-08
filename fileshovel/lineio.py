# -*- coding: utf-8 -*-
# vim:set noet ts=4 sw=4 fenc=utf-8 ff=unix ft=python:
import io
import logging
import os
import re
import time
from enum import Enum
from typing import Iterable, Union, Optional, List, IO

import pyinotify
from pyinotify import WatchManager, Notifier, Event

log = logging.getLogger("fileshovel.lineio")


class FileEventNotifier(Notifier):

	def get_events(self, timeout) -> List[Event]:
		events = []
		if self.check_events(timeout) is not False:
			self.read_events()
			for event in range(len(self._eventq)):
				e = Event({k: v for k, v in self._eventq.popleft().__dict__.items() if not k[0].startswith("_")})
				events.append(e)

		return events


class TellableLineIOEvent(Enum):
	NOTHING = 0
	MODIFY = 1
	DELETE = 2


class TellableLineIO(io.TextIOBase):

	def __init__(self, filename, mode, encoding, skip_lines=0, every_nth=0, watch=False, use_inotify=False,
			regex_search: Union[re.Pattern, bytes] = None, regex_replace: bytes = None):
		if 'b' not in mode:
			mode += 'b'

		if regex_search and regex_replace is None:
			raise RuntimeError("regex_replace must be set if regex_search is set")

		if regex_search and type(regex_search) != re.Pattern:
			regex_search = re.compile(regex_search)

		self._file = None
		self._encoding = encoding
		self.filename = filename
		self.mode = mode
		self.skip_lines = skip_lines
		self.every_nth = every_nth
		self.watch = watch
		self._file = IO()
		self._file_iter = None
		self.open_file()
		self._use_inotify = use_inotify
		self.regex_search = regex_search
		self.regex_replace = regex_replace
		self.current_line = 0
		self.current_line_offset = 0

	def open_file(self):
		if self._file:
			self._file.close()
		self._file = open(self.filename, self.mode)
		self._file_iter = iter(self._file)

	def get_size(self) -> int:
		if self._file:
			return os.fstat(self._file.fileno()).st_size

	def seek(self, offset, whence=io.SEEK_SET) -> int:
		if offset <= self.get_size():
			ret = self._file.seek(offset, whence)
			return ret
		else:
			return 0

	def tell(self) -> int:
		return self._file.tell()

	@property
	def encoding(self) -> str:
		return self._encoding

	def setup_watch_manager(self) -> Optional[FileEventNotifier]:
		if self._use_inotify and os.path.isfile(self.filename):
			mask = pyinotify.IN_MODIFY | \
					pyinotify.IN_ATTRIB | \
					pyinotify.IN_MOVE_SELF | \
					pyinotify.IN_DELETE_SELF

			watch_manager = WatchManager()
			watch_manager.add_watch(self.filename, mask=mask)
			notifier = FileEventNotifier(watch_manager)
			notifier.coalesce_events(True)
			return notifier

	@staticmethod
	def _wait_for_file_event(event_watcher) -> TellableLineIOEvent:
		for event in event_watcher.get_events(timeout=60000):
			if event.mask & pyinotify.IN_MODIFY:
				return TellableLineIOEvent.MODIFY
			elif event.mask & (pyinotify.IN_MOVE_SELF | pyinotify.IN_DELETE_SELF | pyinotify.IN_ATTRIB):
				return TellableLineIOEvent.DELETE
			else:
				log.warning("got unknown event mask %o", event.mask)

		return TellableLineIOEvent.NOTHING

	def __iter__(self) -> Iterable[str]:
		every_nth = self.every_nth
		current_line = 0
		regex_search = self.regex_search
		regex_replace = self.regex_replace
		eof_reached = False
		last_offset = 0
		line = ""
		event_watcher = self.setup_watch_manager()

		if self.skip_lines > 0:
			for line in range(self.skip_lines):
				next(self._file_iter)

		while True:

			for line in self._file_iter:
				if line[-1:] != b"\n":
					break

				current_line += 1

				if every_nth and current_line % every_nth != 0:
					continue

				if regex_search and regex_replace:
					line = regex_search.sub(regex_replace, line)

				self.current_line = current_line
				yield str(line, self._encoding)
				last_offset = self.current_line_offset
				self.current_line_offset = self.tell()

			if eof_reached is False:
				eof_reached = True
				log.info("end of file has been reached for %s", self.filename)

			if self.watch and eof_reached:
				if event_watcher:
					event = self._wait_for_file_event(event_watcher)
					if event is TellableLineIOEvent.DELETE:
						log.info("file has changed, need to re-open file")
						self.open_file()
						continue
					elif event is TellableLineIOEvent.MODIFY:
						if self.tell() > self.get_size():
							log.info("file size has reduced, need to re-open file")
							self.open_file()
							eof_reached = False
							continue
				else:
					time.sleep(int(self.watch))

				if line:
					self.seek(last_offset + len(line) - 1)
					next(self._file_iter)

			else:
				log.debug("reached end of file and not watching, closing")
				break
