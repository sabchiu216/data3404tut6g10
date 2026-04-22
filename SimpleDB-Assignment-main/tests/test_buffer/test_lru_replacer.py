import unittest

from simpledb.buffer.buffer_frame import BufferFrame, AllBufferFramesPinnedException
from simpledb.buffer.replacement.lru_replacer import LRUReplacer


class TestLRUReplacer(unittest.TestCase):

    def test_get_name(self):
        replacer = LRUReplacer()
        self.assertEqual(replacer.get_name(), "LRU")

    def test_choose_oldest_unpinned_frame(self):
        replacer = LRUReplacer()
        f1 = BufferFrame()
        f2 = BufferFrame()
        f3 = BufferFrame()
        pool = [f1, f2, f3]

        replacer.notify(pool, f1)
        replacer.notify(pool, f2)
        replacer.notify(pool, f3)

        victim = replacer.choose(pool)
        self.assertIs(victim, f1)

    def test_recent_access_moves_frame_to_end(self):
        replacer = LRUReplacer()
        f1 = BufferFrame()
        f2 = BufferFrame()
        pool = [f1, f2]

        replacer.notify(pool, f1)
        replacer.notify(pool, f2)
        replacer.notify(pool, f1)

        victim = replacer.choose(pool)
        self.assertIs(victim, f2)

    def test_pinned_frame_not_chosen(self):
        replacer = LRUReplacer()
        f1 = BufferFrame()
        f2 = BufferFrame()
        pool = [f1, f2]

        replacer.notify(pool, f1)
        replacer.notify(pool, f2)

        f1.pin()

        victim = replacer.choose(pool)
        self.assertIs(victim, f2)

    def test_all_pinned_raises(self):
        replacer = LRUReplacer()
        f1 = BufferFrame()
        f2 = BufferFrame()
        pool = [f1, f2]

        replacer.notify(pool, f1)
        replacer.notify(pool, f2)

        f1.pin()
        f2.pin()

        with self.assertRaises(AllBufferFramesPinnedException):
            replacer.choose(pool)


if __name__ == "__main__":
    unittest.main()