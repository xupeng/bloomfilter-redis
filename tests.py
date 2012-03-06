import random
import redis
import sys
import unittest

from datetime import datetime,timedelta

from bloomfilter import BloomFilter, TimeSeriesBloomFilter

class SimpleTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = redis.Redis()

        self.single = BloomFilter(connection=self.connection,
                        bitvector_key='test_bloomfilter',
                        n=1024,
                        k=4)

        self.timeseries = TimeSeriesBloomFilter(connection=self.connection,
                            bitvector_key='test_timed_bloomfilter',
                            n=1024*8,
                            k=4,
                            time_resolution=timedelta(microseconds=1000),
                            time_limit=timedelta(microseconds=10000))

    def tearDown(self):
        # remove the key in redis
        self.connection.delete('test_bloomfilter')

class SimpleTest(SimpleTestCase):
    def test_add(self):
        f = self.single

        f.add('three')
        f.add('four')
        f.add('five')
        f.add('six')
        f.add('seven')
        f.add('eight')
        f.add('nine')
        f.add("ten")

        # test membership operations
        assert 'ten' in f
        assert 'five' in f
        assert 'two' not in f
        assert 'eleven' not in f

    def test_delete(self):
        f = self.single

        f.add('ten')
        assert 'ten' in f

        f.delete('ten')
        assert 'ten' not in f


    def test_timeseries_add(self):
        f = self.timeseries

        assert 'test_value' not in f
        f.add('test_value')
        assert 'test_value' in f

    def test_timeseries_delay(self):
        f = self.timeseries

        f.add('test_value')
        start = datetime.now()
        # allow for 3ms delay in storing/timer resolution
        delay = timedelta(microseconds=3000)

        # make sure that the filter doesn't say that test_value is in the filter for too long
        while 'test_value' in f:
            assert datetime.now() < (start+timedelta(microseconds=10000)+delay)
        assert 'test_value' not in f

if __name__ == '__main__':
    unittest.main()
