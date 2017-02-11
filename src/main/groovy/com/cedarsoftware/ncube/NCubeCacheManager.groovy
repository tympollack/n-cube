package com.cedarsoftware.ncube

import com.google.common.cache.CacheBuilder
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.cache.guava.GuavaCache

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit

class NCubeCacheManager implements CacheManager
{
    private ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>()

    Cache getCache(String name)
    {
        name = name.toLowerCase()
        Cache cache = caches[name]

        if (cache == null)
        {
            cache = new GuavaCache(name, CacheBuilder.newBuilder().expireAfterAccess(4, TimeUnit.HOURS).concurrencyLevel(16).build())
            Cache mapRef = caches.putIfAbsent(name, cache)
            if (mapRef != null)
            {
                cache = mapRef
            }
        }
        return cache
    }

    Collection<String> getCacheNames()
    {
        return caches.keySet()
    }
}
