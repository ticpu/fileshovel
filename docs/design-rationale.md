# Design rationale

## A failed insert or unparseable record stops the process

Any CSV record the parser cannot split, and any error preparing or inserting a batch, ends the process with a non-zero status after one ERROR line naming the table, the batch's offset and line range, and the database's error fields; systemd's restart policy brings it back. A row the table will never accept therefore crash-loops visibly instead of being skipped: these rows are emergency-call records, and a gap nobody sees is worse than a stalled feed that alerts. The rejected value itself is never logged, because CDR fields carry caller numbers and the database's error text quotes the value; the offset is enough to find the row in the file. Bytes invalid in the configured encoding are the exception: they become replacement characters with a warning, because the row's other fields still land.

## One writer commits batches in read order

Resume continues after the highest offset committed for this server, which is only correct if every row before it is also committed. Batches are therefore committed by a single writer in the order they were read; parallel writers would need ordered-commit tracking and are refused. The writer runs on a worker thread only during catch-up, to overlap reading with inserting, and becomes synchronous once the reader first reaches end of file, so no thread outlives catch-up and a failed long-running writer cannot leave the reader alive. Both phases go through the same insert function in pgsql.py.

## Rotation is detected by file identity, not by event type

On every wake the reader compares the device and inode of the path with those of its open file and switches only when they differ, after draining the old file to its end. inotify events serve only to wake it. In production the CSV is normally never rotated, so this path covers a manual move or truncation rather than a routine schedule. In-place truncation (copytruncate) loses rows written between copy and truncate and is not supported.

## Resume offsets carry no file identity

The stored offset is a byte position in whichever file generation was current, with nothing recording which file that was. An offset beyond the current file's size is taken as a rotation and reading restarts at the beginning, with a warning. A new file that has already grown past the old offset cannot be told apart and its earlier rows are skipped; closing that gap requires storing a file identity alongside the offset.
