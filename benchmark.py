import random
import redis
import sys

from datetime import datetime

from bloomfilter import BloomFilter, TimeSeriesBloomFilter

connection = redis.Redis()

filter_size = 8 * 500 * 1024
test_amount = 10000


f = BloomFilter(connection=connection, bitvector_key='test_bloomfilter', n=filter_size, k=4)

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