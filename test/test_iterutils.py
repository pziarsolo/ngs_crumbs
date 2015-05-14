# Copyright 2012 Jose Blanca, Peio Ziarsolo, COMAV-Univ. Politecnica Valencia
# This file is part of ngs_crumbs.
# ngs_crumbs is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# ngs_crumbs is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR  PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with ngs_crumbs. If not, see <http://www.gnu.org/licenses/>.

import unittest
import tempfile

from crumbs.iterutils import (sample, sample_low_mem, length, group_in_packets,
                              rolling_window, group_in_packets_fill_last,
                              sorted_items, unique, unique_unordered,
                              generate_windows, PeekableIterator,
                              RandomAccessIterator, RandomAccessChromIterator)
from crumbs.exceptions import SampleSizeError
from collections import namedtuple

# pylint: disable=R0201
# pylint: disable=R0904


class IterutilsTest(unittest.TestCase):
    'It tests the iterator tools.'

    def check_sampled_items(self, items, sampled_items, num_sampled_items):
        'It checks that the sample is valid'
        assert len(sampled_items) == num_sampled_items

        # no repeated ones
        sampled_items = set(sampled_items)
        assert len(sampled_items) == num_sampled_items

        assert sampled_items.issubset(items)

    def test_sample(self):
        'We can sample an iterator'
        items = range(100)

        num_sampled_items = 10
        for num_sampled_items in (10, 90):
            sampled_items = list(sample(items, num_sampled_items))
            self.check_sampled_items(items, sampled_items, num_sampled_items)

        # order not kept
        sampled_items = list(sample(range(1000), 5))
        self.check_sampled_items(range(1000), sampled_items, 5)
        # The following test could fail randomly from time to tme
        assert sampled_items != list(sorted(sampled_items))

        sampled_items = list(sample(range(1000), 5, in_disk=True))
        self.check_sampled_items(range(1000), sampled_items, 5)

    def test_sample_low_mem(self):
        'We can sample an iterator'
        length_ = 100
        items = range(length_)

        num_sampled_items = 10
        for num_sampled_items in (10, 90):
            sampled_items = list(sample_low_mem(items, length_,
                                                num_sampled_items))
            self.check_sampled_items(items, sampled_items, num_sampled_items)

    def test_sample_too_much(self):
        for items in [range(10), []]:
            try:
                list(sample_low_mem(items, 10, 20))
                self.fail('Sample size error expected')
            except SampleSizeError:
                pass

            try:
                list(sample(items, 20))
                self.fail('Sample size error expected')
            except SampleSizeError:
                pass

            try:
                list(sample(items, 20, in_disk=True))
                self.fail('Sample size error expected')
            except SampleSizeError:
                pass

    def test_length(self):
        'We can count an iterator'
        items = xrange(10)
        assert length(items) == 10

    def test_group_in_packets(self):
        'It groups an iterator in packets of items'
        packets = list(group_in_packets(range(4), 2))
        assert packets == [(0, 1), (2, 3)]

        packets = [packet for packet in group_in_packets(range(5), 2)]
        assert packets == [(0, 1), (2, 3), (4,)]

        packets = list(group_in_packets_fill_last(range(5), 2))
        assert packets == [(0, 1), (2, 3), (4, None)]

        packets = list(group_in_packets([], 2))
        assert packets == []

    def test_rolling_window(self):
        'We get the items along a rolling window'
        # with series
        serie = '12345'
        assert [''.join(win) for win in rolling_window(serie, 3)] == ['123',
                                                                      '234',
                                                                      '345']
        assert not [''.join(win) for win in rolling_window(serie, 6)]
        assert [''.join(w) for w in rolling_window(serie, 5)] == ['12345']

        # with iterator
        iterator = iter(serie)
        assert [''.join(win) for win in rolling_window(iterator, 3)] == ['123',
                                                                         '234',
                                                                         '345']
        iterator = iter(serie)
        assert not [''.join(win) for win in rolling_window(iterator, 6)]
        iterator = iter(serie)
        assert [''.join(w) for w in rolling_window(iterator, 5)] == ['12345']

        # with step
        series = ['1234567890', '123456789', '12345678', '1234567']
        expected = [['1234', '3456', '5678', '7890'], ['1234', '3456', '5678'],
                    ['1234', '3456', '5678'], ['1234', '3456']]
        for serie, exp in zip(series, expected):
            wins1 = [''.join(win) for win in rolling_window(serie, 4, 2)]
            assert wins1 == exp

            iterator = iter(serie)
            wins2 = [''.join(win) for win in rolling_window(iterator, 4, 2)]
            assert wins1 == wins2

    def test_sorted_items(self):
        items = [1, 2, 3, 4, 4, 3, 2, 1]
        unique_items = sorted_items(iter(items))
        assert list(unique_items) == [1, 1, 2, 2, 3, 3, 4, 4]
        unique_items = sorted_items(iter(items), tempdir=tempfile.tempdir)
        assert list(unique_items) == [1, 1, 2, 2, 3, 3, 4, 4]
        unique_items = sorted_items(iter(items),
                                    max_items_in_memory=3)
        assert list(unique_items) == [1, 1, 2, 2, 3, 3, 4, 4]

        items = iter([])
        assert not list(unique_items)

    def test_unique_items(self):
        items = [1, 1, 2, 2, 3, 3, 4]
        unique_items = unique(items)
        assert list(unique_items) == [1, 2, 3, 4]

    def test_unique_unordered_items(self):
        items = [1, 2, 3, 4, 4, 3, 2, 1]
        unique_items = unique_unordered(items)
        assert list(unique_items) == [1, 2, 3, 4]

    def test_key(self):
        items = [(1, 'a'), (1, 'b'), (2, 'a')]
        _sorted_items = list(sorted_items(iter(items), key=lambda x: x[0],
                                          max_items_in_memory=1))
        assert _sorted_items == [(1, 'a'), (1, 'b'), (2, 'a')]
        unique_items = unique(_sorted_items, key=lambda x: x[0])
        assert list(unique_items) == [(1, 'a'), (2, 'a')]

        _sorted_items = sorted_items(iter(items), key=lambda x: x[1])
        assert list(_sorted_items) == [(1, 'a'), (2, 'a'), (1, 'b')]
        unique_items = unique(_sorted_items, key=lambda x: x[1])
        assert list(unique_items) == [(1, 'a'), (1, 'b')]


class GenerateWindowsTests(unittest.TestCase):
    def generate_wins(self, size, step, number):
        windows = generate_windows(size=size, step=step)
        wins = []
        for index, win in enumerate(windows):
            if index >= number:
                break
            wins.append(win)
        return wins

    def test_generate_windows(self):
        assert self.generate_wins(10, 10, 3) == [(0, 10), (10, 20), (20, 30)]
        assert self.generate_wins(10, None, 3) == [(0, 10), (10, 20), (20, 30)]
        assert self.generate_wins(10, 5, 3) == [(0, 10), (5, 15), (10, 20)]

        res = [(0, 10), (10, 20), (20, 30)]
        assert list(generate_windows(size=10, step=10, end=30)) == res


class PeekableIterTest(unittest.TestCase):
    def test_peek(self):
        stream = iter(range(5))
        stream = PeekableIterator(stream)
        assert stream.peek() == 0
        assert stream.next() == 0
        assert stream.next() == 1
        assert stream.peek() == 2
        assert stream.next() == 2
        assert stream.next() == 3
        assert stream.next() == 4
        try:
            stream.peek()
            self.fail('StopIteration expected')
        except StopIteration:
            pass


class RandomAccessTest(unittest.TestCase):
    def test_iter_access(self):
        seq1 = range(100)
        seq2 = RandomAccessIterator(iter(seq1), 11)
        assert list(seq1) == list(seq2)

    def test_next_items(self):
        seq1 = range(10)
        seq2 = RandomAccessIterator(iter(seq1), 7)
        assert seq1[0] == seq1[0]
        assert seq2[0:3] == seq1[0:3]
        assert seq2[:3] == seq1[:3]
        assert seq2[2] == seq1[2]

        first = seq2.next()
        assert first == 0
        assert seq2[0:4] == seq1[0:4]

        item = seq2.next()
        assert item == 1
        assert seq2[0:5] == seq1[0:5]

        item = seq2.next()
        assert item == 2
        assert seq2[0:6] == seq1[0:6]

        item = seq2.next()
        assert item == 3
        assert seq2[0:7] == seq1[0:7]

        try:
            seq2[0:8]
            self.fail('IndexError expexted')
        except IndexError:
            pass

        item = seq2.next()
        assert item == 4
        assert seq2[1:8] == seq1[1:8]
        assert seq2[1] == seq1[1]
        try:
            seq2[0]
            self.fail('IndexError expexted')
        except IndexError:
            pass

        item = seq2.next()
        assert item == 5
        assert seq2[2:9] == seq1[2:9]

        try:
            seq2[1:8]
            self.fail('IndexError expexted')
        except IndexError:
            pass

        item = seq2.next()
        assert item == 6
        assert seq2[3:10] == seq1[3:10]

        item = seq2.next()
        assert item == 7
        assert seq2[3:10] == seq1[3:10]
        item = seq2.next()
        assert item == 8
        item = seq2.next()
        assert item == 9
        assert seq2[3:10] == seq1[3:10]

    def test_short_iter(self):
        seq1 = range(3)
        seq2 = RandomAccessIterator(iter(seq1), 7)
        assert seq1[0] == seq1[0]
        assert seq2[0:3] == seq1[0:3]
        assert seq2[:3] == seq1[:3]

        assert seq1[0] == seq1[0]
        assert seq2[0:3] == seq1[0:3]
        assert seq2[:3] == seq1[:3]

        first = seq2.next()
        assert first == 0
        assert seq2[0:4] == seq1[0:4]

        item = seq2.next()
        assert item == 1
        assert seq2[0:4] == seq1[0:4]

        item = seq2.next()
        assert item == 2
        assert seq2[0:4] == seq1[0:4]

        try:
            item = seq2.next()
            self.fail('StopIteration expected')
        except StopIteration:
            pass

        seq1 = range(2)
        seq2 = RandomAccessIterator(iter(seq1), 7)
        assert seq1[0] == seq1[0]
        assert seq2[0:3] == seq1[0:3]
        assert seq2[:3] == seq1[:3]

    def test_empty_iter(self):
        seq1 = []
        seq2 = RandomAccessIterator(iter(seq1), 7)
        assert seq2[0:3] == seq1[0:3]

        try:
            seq2.next()
            self.fail('StopIteration expected')
        except StopIteration:
            pass


class RandomAccessChromIteratorTest(unittest.TestCase):
    fake_pos = namedtuple('pos', ['chrom', 'start', 'end'])

    def test_initialize(self):
        items = [self.fake_pos('chrom1', 24, 24),
                 self.fake_pos('chrom1', 54, 54),
                 self.fake_pos('chrom1', 134, 134),
                 self.fake_pos('chrom1', 145, 145),
                 self.fake_pos('chrom1', 155, 155),
                 self.fake_pos('chrom2', 155, 155),
                 self.fake_pos('chrom3', 155, 155),
                 self.fake_pos('chrom3', 165, 165)]

        pos_getter = lambda x: (x.chrom, x.start, x.end)
        random_iter = RandomAccessChromIterator(iter(items), win_len=31,
                                                pos_getter=pos_getter)

        assert list(random_iter) == items

    def test_gt_lt(self):

        pos_getter = lambda x: (x.chrom, x.start, x.end)
        random_iter = RandomAccessChromIterator(iter([]), win_len=31,
                                                pos_getter=pos_getter)

        assert random_iter._lt(('chrom1', 24, 24), ('chrom1', 54, 54))
        assert random_iter._lt(('chrom1', 24, 24), ('chrom2', 54, 54))
        assert not random_iter._lt(('chrom2', 24, 24), ('chrom1', 54, 54))
        assert not random_iter._lt(('chrom1', 54, 54), ('chrom1', 53, 53))
        assert not random_iter._lt(('chrom1', 4, 4), ('chrom1', 3, 53))
        assert random_iter._lt(('chrom1', 4, 4), ('chrom1', 5, 53))
        assert not random_iter._lt(('chrom1', 1, 60), ('chrom1', 24, 24))

        assert not random_iter._gt(('chrom1', 24, 24), ('chrom1', 54, 54))
        assert not random_iter._gt(('chrom1', 24, 24), ('chrom2', 54, 54))
        assert random_iter._gt(('chrom2', 24, 24), ('chrom1', 54, 54))
        assert not random_iter._gt(('chrom1', 24, 24), ('chrom2', 54, 54))
        assert random_iter._gt(('chrom1', 54, 54), ('chrom1', 53, 53))

        assert not random_iter._gt(('chrom1', 20, 54), ('chrom1', 53, 53))
        assert not random_iter._gt(('chrom1', 20, 54), ('chrom1', 53, 58))
        assert not random_iter._gt(('chrom1', 20, 54), ('chrom1', 56, 59))
        assert random_iter._gt(('chrom1', 20, 54), ('chrom1', 5, 9))
        assert not random_iter._gt(('chrom1', 134, 134), ('chrom1', 1, 160))

    def test_in_buff(self):
        items = [self.fake_pos('chrom1', 24, 24),
                 self.fake_pos('chrom1', 54, 54),
                 self.fake_pos('chrom1', 134, 134),
                 self.fake_pos('chrom1', 145, 145),
                 self.fake_pos('chrom1', 155, 155),
                 self.fake_pos('chrom3', 155, 155),
                 self.fake_pos('chrom4', 155, 155),
                 self.fake_pos('chrom4', 165, 165)]

        pos_getter = lambda x: (x.chrom, x.start, x.end)
        random_iter = RandomAccessChromIterator(iter(items), win_len=31,
                                                pos_getter=pos_getter)
        assert random_iter._in_buff(('chrom1', 1, 60))
        assert not random_iter._in_buff(('chrom1', 1, 160))
        assert not random_iter._in_buff(('chrom1', 60, 160))
        assert random_iter._in_buff(('chrom1', 1, 22))

        random_iter.next()
        random_iter.next()
        random_iter.next()
        assert not random_iter._in_buff(('chrom1', 1, 60))
        assert random_iter._in_buff(('chrom2', 1, 60))
        assert random_iter._in_buff(('chrom3', 1, 60))
        assert not random_iter._in_buff(('chrom3', 1, 160))

    def test_fetch(self):
        self._t3st_fetch(debug_min_for_bisect=0)
        self._t3st_fetch(debug_min_for_bisect=5)

    def _t3st_fetch(self, debug_min_for_bisect):
        items = [self.fake_pos('chrom1', 24, 24),
                 self.fake_pos('chrom1', 54, 54),
                 self.fake_pos('chrom1', 134, 134),
                 self.fake_pos('chrom1', 145, 145),
                 self.fake_pos('chrom1', 155, 155),
                 self.fake_pos('chrom2', 155, 155),
                 self.fake_pos('chrom3', 155, 155),
                 self.fake_pos('chrom3', 165, 165),
                 self.fake_pos('chrom6', 165, 165)]
        pos_getter = lambda x: (x.chrom, x.start, x.end)
        random_iter = RandomAccessChromIterator(iter(items), win_len=31,
                                                pos_getter=pos_getter,
                                                debug_min_for_bisect=debug_min_for_bisect)

        # first item
        assert list(random_iter.fetch(('chrom1', 1, 60))) == [('chrom1', 24, 24),
                                                              ('chrom1', 54, 54)]

        # first item
        assert list(random_iter.fetch(('chrom1', 1, 60))) == [('chrom1', 24, 24),
                                                              ('chrom1', 54, 54)]
        try:
            random_iter.fetch(('chrom1', 1, 135))
            self.fail('expected IndexError')
        except IndexError:
            pass
        random_iter.next()
        random_iter.next()
        assert ('chrom1', 134, 134) == random_iter.next()

        try:
            random_iter.fetch(('chrom1', 1, 60))
            self.fail('expected IndexError')
        except IndexError:
            pass
        assert len(list(random_iter.fetch(('chrom1', 60, 160)))) == 3
        assert len(list(random_iter.fetch(('chrom1', 60, 140)))) == 1
        assert len(list(random_iter.fetch(('chrom1', 60, 150)))) == 2

if __name__ == '__main__':
    import sys
    sys.argv = ['', 'RandomAccessChromIteratorTest']
    unittest.main()
