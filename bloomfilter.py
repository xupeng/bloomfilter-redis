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

class BloomFilter(object):
    def __init__(self, connection, key, n, k):
        self.connection = connection
        self.n = n
        self.k = k
        self.bitvector_key = key
    
    def __contains__(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.getbit(self.bitvector_key, hashed_offset)
        results = pipeline.execute()
        return all(results)
    
    def add(self, key):
        pipeline = self.connection.pipeline()
        for hashed_offset in self.calculate_offsets(key):
            pipeline.setbit(self.bitvector_key, hashed_offset, 1)
        pipeline.execute()
    
    def calculate_offsets(self, key):
        hash_1 = FNVHash(key)
        hash_2 = APHash(key)
        
        for i in range(self.k):
            yield (hash_1 + i * hash_2) % self.n

if __name__ == '__main__':
    import redis
    import random
    connection = redis.Redis()
    
    f = BloomFilter(connection=connection, key='test_bloomfilter', n=1024, k=4)
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
    
    connection.delete('test_bloomfilter')
