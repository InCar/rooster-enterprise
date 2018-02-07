package com.incarcloud.rooster.cache;

import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * Redis缓存管理器
 *
 * @author Aaric, created on 2018-02-07T11:45.
 * @since 2.1.20-SNAPSHOT
 */
public class RedisCacheManager implements ICacheManager {

    /**
     * Spring Redis操作对象
     */
    private RedisTemplate<String, String> redisTemplate;

    /**
     * 注入Spring Redis操作实现
     *
     * @param redisTemplate Spring Redis操作对象
     */
    public RedisCacheManager(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void set(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    @Override
    public void set(String key, String value, int ttl) {
        redisTemplate.opsForValue().set(key, value, ttl, TimeUnit.SECONDS);
    }

    @Override
    public void expire(String key, int ttl) {
        redisTemplate.expire(key, ttl, TimeUnit.SECONDS);
    }

    @Override
    public String get(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    @Override
    public void delete(String key) {
        redisTemplate.delete(key);
    }
}
