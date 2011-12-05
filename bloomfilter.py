import math
import time

from datetime import datetime, timedelta

# For hash functions see http://www.partow.net/programming/hashfunctions/index.html
# Author Arash Partow, CPL http://www.opensource.org/licenses/cpl1.0.php
def FNVHash(key):
    fnv_prime = 0x811C9DC5
    hash = 0
    for i in range(len(key)):
      hash *= fnv_prime
      hash ^= ord(key[i])
    return hash

def APHash(key):
    hash = 0xAAAAAAAA
    for i in range(len(key)):
      if ((i & 1) == 0):
        hash ^= ((hash <<  7) ^ ord(key[i]) * (hash >> 3))
      else:
        hash ^= (~((hash << 11) + ord(key[i]) ^ (hash >> 5)))
    return hash

class TimeSeriesBloomFilter(object):    
    # todo: expire bloom filter keys after their 'time_limit' is passed, this can happen
    #       in the add() pipeline
    # todo: support a 'now' so that we can add stuff in the past
    # todo: make it more clear how all this works
    # todo: create a helper function that calculates the total amount of memory stored
    
    def __init__(self, connection, bitvector_key, n, k, **kwargs):
        self.time_resolution = kwargs.get('time_resolution', timedelta(minutes=1))
        self.time_limit = kwargs.get('time_limit', timedelta(hours=1))
        self.connection = connection
        self.bitvector_key = bitvector_key
        self.n = n
        self.k = k
    
    def most_current_filters(self, **kwargs):
        within = kwargs.pop('within')
        
        resolution_microseconds = self.time_resolution.days*86400 + \
            self.time_resolution.seconds + self.time_resolution.microseconds
        
        # how many bloom filters will we need to iterate for this?
        num_filters = int(math.ceil(float((within.days*86400 + within.seconds)*1e6 + \
                        within.microseconds) / resolution_microseconds))
        
        # get a datetime object of the 'current' filter
        block_now = resolution_microseconds * math.floor(time.time()*1e6 / resolution_microseconds)
        now = datetime.fromtimestamp(block_now/1e6)
        
        for x in xrange(num_filters):
            filter_date = now - x * self.time_resolution
            filter_bitvector_key = '%s|%s' % (self.bitvector_key, filter_date.isoformat())
            yield BloomFilter(self.connection, filter_bitvector_key, self.n, self.k)
    
    def add(self, key, **kwargs):
        within = kwargs.get('within', self.time_resolution)
        
        # add to the current bloom filter
        for bloom_filter in self.most_current_filters(within=within):
            bloom_filter.add(key)
    
    def __contains__(self, key, **kwargs):
        # checks if this time series bloom filter has 
        # contained an element within the last x minutes
        within = kwargs.get('within', self.time_limit)
        
        for bloom_filter in self.most_current_filters(within=within):
            if key in bloom_filter:
                return True
        else:
            return False
    
    # lookup support for the 'within' parameter that we can't express in the magic __contains__
    contains = __contains__

class BloomFilter(object):
    def __init__(self, connection, bitvector_key, n, k):
        # create a bloom filter based on a redis connection, a bitvector_key (name) for it
        # and the settings n & k, which dictate how effective it will be
        # - n is the amount of bits it will use, I have had success with 85001024 (500kiB)
        #   for 100k values. If you have fewer, you can get away with using fewer bits.
        #   in general, the more bits, the fewer false positives
        # - k is the number of hash derivations it uses, too many will fill up the filter 
        #   too quickly, not enough will lead to many false positives
        
        self.connection = connection
        self.bitvector_key = bitvector_key
        self.n = n
        self.k = k
    
    def __contains__(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.getbit(self.bitvector_key, hashed_offset)
        results = pipeline.execute()
        return all(results)
    
    def add(self, key, set_value=1, transaction=False):
        # set bits for every hash to 1
        # sometimes we can use pipelines here instead of MULTI,
        # which makes it a bit faster
        pipeline = self.connection.pipeline(transaction=transaction)
        for hashed_offset in self.calculate_offsets(key):
            pipeline.setbit(self.bitvector_key, hashed_offset, set_value)
        pipeline.execute()
    
    def delete(self, key):
        # delete is just an add with value 0
        # make sure the pipeline gets wrapped in MULTI/EXEC, so
        # that a deleted element is either fully deleted or not
        # at all, in case someone is checking __contains__ while
        # an element is being deleted
        self.add(key, set_value=0, transaction=True)
    
    def calculate_offsets(self, key):
        # we're using only two hash functions with different settings, as described
        # by Kirsch & Mitzenmacher: http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
        hash_1 = FNVHash(key)
        hash_2 = APHash(key)
        
        for i in range(self.k):
            yield (hash_1 + i * hash_2) % self.n

if __name__ == '__main__':
    import random
    import redis
    import sys
    
    from datetime import datetime
    
    connection = redis.Redis()
    
    if 'benchmark' in sys.argv:
        connection.delete('test_bloomfilter')
        
        filter_size = 8 * 500 * 1024
        test_amount = 100000
        
        f = BloomFilter(connection=connection, key='test_bloomfilter', n=filter_size, k=4)
        
        print "filling bloom filter of %.2fkB size with %ik values" % \
            (filter_size/1024.0/8, test_amount/1000)
        
        # create a reference dict so that we can check for false positives
        ref = {}
        for x in xrange(test_amount):
            ref['%.8f' % random.random()] = True
        
        # add values to filter
        start = datetime.now()
        for val in ref.iterkeys():
            f.add(val)
        
        # calculate results
        duration = datetime.now()-start
        duration = duration.seconds+duration.microseconds/1000000.0
        per_second = test_amount/duration
        print "adding %i values took %.2fs (%i values/sec, %.2f us/value)" % \
            (test_amount, duration, per_second, 1000000.0/per_second)
        
        # try random values and see how many false positives we'll get
        false_positives = 0
        correct_negatives = 0
        while correct_negatives < test_amount:
            val = '%.8f' % random.random()
            if (val in f) and (val not in ref):
                false_positives += 1
            else:
                correct_negatives += 1
        
        print "correct: %s / false: %s -> %.4f%% false positives" % \
            (correct_negatives, false_positives, 100*false_positives/float(correct_negatives))
        
        # remove the key in redis
        connection.delete('test_bloomfilter')