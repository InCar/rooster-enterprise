package com.incarcloud.rooster.cache;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.BoundGeoOperations;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.List;
import java.util.Set;
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
     * Spring Redis String操作对象
     */
    private ValueOperations<String, String> valueOperations;

    /**
     * Spring Redis Hash操作对象
     */
    private HashOperations<String, String, String> hashOperations;

    /**
     * 注入Spring Redis操作实现
     *
     * @param redisTemplate Spring Redis操作对象
     */
    public RedisCacheManager(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.valueOperations = redisTemplate.opsForValue();
        this.hashOperations = redisTemplate.opsForHash();
    }

    @Override
    public void set(String key, String value) {
        valueOperations.set(key, value);
    }

    @Override
    public void set(String key, String value, int ttl) {
        valueOperations.set(key, value, ttl, TimeUnit.SECONDS);
    }

    @Override
    public String get(String key) {
        return valueOperations.get(key);
    }

    @Override
    public void delete(String key) {
        redisTemplate.delete(key);
    }

    @Override
    public void expire(String key, int ttl) {
        redisTemplate.expire(key, ttl, TimeUnit.SECONDS);
    }

    @Override
    public Set<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }

    @Override
    public void hset(String key, String hashKey, String value) {
        hashOperations.put(key, hashKey, value);
    }

    @Override
    public String hget(String key, String hashKey) {
        return hashOperations.get(key, hashKey);
    }

    @Override
    public void hdelete(String key, String hashKey) {
        hashOperations.delete(key, hashKey);
    }

    @Override
    public Long hsize(String key) {
        return hashOperations.size(key);
    }

    @Override
    public void gset(String key, String flagKey, double longitude, double latitude) {
        BoundGeoOperations<String, String> boundGeoOperations = redisTemplate.boundGeoOps(key);
        boundGeoOperations.geoAdd(new Point(longitude, latitude), flagKey);

    }

    @Override
    public double[] gget(String key, String flagKey) {
        BoundGeoOperations<String, String> boundGeoOperations = redisTemplate.boundGeoOps(key);
        List<Point> pointList = boundGeoOperations.geoPos(flagKey);
        if (null != pointList && 1 == pointList.size()) {
            return new double[]{pointList.get(0).getX(), pointList.get(0).getY()};
        }
        return null;
    }

    @Override
    public void gdelete(String key, String flagKey) {
        BoundGeoOperations<String, String> boundGeoOperations = redisTemplate.boundGeoOps(key);
        boundGeoOperations.geoRemove(flagKey);
    }
}
