package com.cedarsoftware.ncube

import com.google.common.cache.CacheBuilder
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import groovy.transform.CompileStatic
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.cache.guava.GuavaCache

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class NCubeCacheManager implements CacheManager
{
    private ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>()

    Cache getCache(String name)
    {
        Cache cache = caches[name]  // name = ApplicationID.toString()

        if (cache == null)
        {
            cache = new GuavaCache(name, CacheBuilder.newBuilder()
//                    .expireAfterWrite(5, TimeUnit.MINUTES)
//                    .expireAfterAccess(4, TimeUnit.HOURS)
                    .concurrencyLevel(16)
                    .removalListener(new NCubeRemovalListener())
//                    .maximumSize(100000)      // Another option for eviction if # cubes in memory grows too large
                    .build() as com.google.common.cache.Cache)
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

    /**
     * Apply the passed in Closure to all values in the cache.
     * @param name String name of the Cache
     * @param closure Closure to apply to each value
     */
    void applyToValues(String name, Closure closure)
    {
        Cache cache = caches[name]
        if (cache == null)
        {
            return
        }
        GuavaCache gCache = cache as GuavaCache
        com.google.common.cache.Cache googleCache = gCache.nativeCache
        Iterator i = googleCache.asMap().values().iterator()
        while (i.hasNext())
        {
            Object value = i.next()
            closure(value)
        }
    }

    /**
     * Cube cache eviction listener.  This class exists to clear the class loader cache entries
     * associated to cells that have generated classes dynamically.  By removing the references
     * from those caches, the dynamically generated classes can be garbage collected (Java 8+).
     */
    private static class NCubeRemovalListener implements RemovalListener
    {
        void onRemoval(RemovalNotification removalNotification)
        {
            Object value = removalNotification.value
            if (value instanceof NCube)
            {
                NCube ncube = value as NCube
                for (Object cellValue : ncube.cellMap.values())
                {
                    if (cellValue instanceof UrlCommandCell)
                    {
                        UrlCommandCell cell = cellValue as UrlCommandCell
                        cell.clearClassLoaderCache()
                    }
                }
            }
        }
    }
}
