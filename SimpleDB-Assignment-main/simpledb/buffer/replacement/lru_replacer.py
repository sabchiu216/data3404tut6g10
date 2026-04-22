"""
LRU Replacer implementation.
"""

from typing import List
from simpledb.buffer.buffer_frame import BufferFrame, AllBufferFramesPinnedException
from simpledb.buffer.replacement.replacer import Replacer


class LRUReplacer(Replacer):
    """Least-recently-used replacement policy."""

    def __init__(self):
        """Initialize LRU state."""
        self.access_order: List[BufferFrame] = []

    def get_name(self) -> str:
        """Get the name of the replacement policy."""
        return "LRU"

    def choose(self, pool: List[BufferFrame]) -> BufferFrame:
        """Choose the least recently used unpinned frame."""
        assert len(pool) > 0

        self.access_order = [frame for frame in self.access_order if frame in pool]

        for frame in pool:
            if frame not in self.access_order:
                self.access_order.append(frame)

        for frame in self.access_order:
            if not frame.is_pinned():
                return frame

        raise AllBufferFramesPinnedException()

    def notify(self, pool: List[BufferFrame], frame: BufferFrame) -> None:
        """Update access order when a frame is accessed."""
        if frame in self.access_order:
            self.access_order.remove(frame)
        self.access_order.append(frame)