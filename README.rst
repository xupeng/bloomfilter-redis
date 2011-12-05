=================
bloomfilter-redis
=================

Standard & Time series bloom filters, backed by redis bitvectors

Overview
========

This is the little bloom filter we're using to filter unique views using redis.

It doesn't do anything special, but I didn't find any small and dependency-free bloom
filter written in Python that use Redis as their backend.

Time Series
========
If you're tracking users over time, and you want to answer the question "have we seen
this guy in the past 2 minutes", this is exactly right for you. For high-throughput
applications this is very space-effective. The total memory footprint is known before-
hand, and is based on the amount of history you want to save and the resolution.

You might track users in the past 2 minutes with a 10-second resolution using 12 bloom
filters. User hits are logged into the most recent bloom filter, and checking if you have
seen a user in the past 2 minutes will just go back through those 12 filters.

The finest resolutions possible are around 1ms. If you're pushing it to this limit you'll
have to take care of a bunch of things: Storing to and retrieving from Redis takes some
time. Timestamps aren't all that exact, especially when running on a virtual machine. If
you're using multiple machines, their clocks have to be perfectly in sync.

Quick Benchmarks
================

Quick benchmark for ballpark figures on a MacbookPro (2x 2.66GHz) with Python 2.7,
hiredis and Redis 2.9 (unstable). Each benchmark was run with k=4 hashes per key. Keys
are random strings of 10 chars length:

Big filter with fewer values:
filling bloom filter of 1024.00kB size with 10k values
adding 10000 values took 2.09s (4790 values/sec, 208.73 us/value)
correct: 100000 / false: 0 -> 0% false positives

Small filter with a lot of values:
filling bloom filter of 500.00kB size with 100k values
adding 100000 values took 22.30s (4485 values/sec, 222.96 us/value)
correct: 100000 / false: 3 -> 0.003% false positives

4 parallel Python processes:
filling bloom filter of 1024.00kB size with 2M values
adding 2000000 values took 214.69s (9316 values/sec, 429.38 us/value)