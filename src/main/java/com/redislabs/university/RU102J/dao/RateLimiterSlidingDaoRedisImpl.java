package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import static com.redislabs.university.RU102J.core.KeyHelper.getKey;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        try (Jedis jedis = jedisPool.getResource()) {
            // key of the sorted set: [limiter]:[windowSize]:[name]:[maxHits]
            String key = getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);

            // get the current time in millisecond as score for current hit and the [timestamp]-[random-number]
            long currentTimeMillis = System.currentTimeMillis();
            String member = currentTimeMillis + ":" + Math.random();


            // Add current value, remove expired value, and get the total number of elements in the sorted set using Redis transaction
            Transaction transaction = jedis.multi();
            transaction.zadd(key, currentTimeMillis, member);
            transaction.zremrangeByScore(key, 0, currentTimeMillis - windowSizeMS);
            Response<Long> hits = transaction.zcard(key);
            transaction.exec();

            // throw exception if exceed the limit
            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }
}
