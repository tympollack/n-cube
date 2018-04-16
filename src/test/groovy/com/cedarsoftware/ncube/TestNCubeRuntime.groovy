package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test
import org.springframework.cache.caffeine.CaffeineCache

import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static org.junit.Assert.fail

@CompileStatic
class TestNCubeRuntime extends NCubeBaseTest
{
    @Test
    void testClearCubesFromCache()
    {
        populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, ['testbranch', 'testage'])
        Map cache = ((CaffeineCache) testClient.getCacheForApp(ApplicationID.testAppId)).nativeCache.asMap()
        assert 2 == cache.size()
        assert !cache.containsKey('testbranch')
        assert !cache.containsKey('testage')
        assert cache.containsKey('testcube')
    }

    @Test
    void testIsCubeCached()
    {
        populateCache()
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testbranch')
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testage')
        assert ncubeRuntime.isCached(ApplicationID.testAppId, 'testcube')
        ncubeRuntime.clearCache(ApplicationID.testAppId)
        Map cache = ((CaffeineCache) testClient.getCacheForApp(ApplicationID.testAppId)).nativeCache.asMap()
        assert !cache.containsKey('testbranch')
        assert !cache.containsKey('testage')
        assert !cache.containsKey('testcube')
    }

    @Test
    void testClearCubesFromCacheNullCubeNames()
    {
        populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, null)
        Map cache = ((CaffeineCache) testClient.getCacheForApp(ApplicationID.testAppId)).nativeCache.asMap()
        assert 0 == cache.size()
    }

    @Test
    void testClearCubesFromCacheNullApp()
    {
        populateCache()
        try
        {
            ncubeRuntime.clearCache(null, ['testbranch', 'testage'])
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertContainsIgnoreCase(e.message, 'ApplicationID', 'null')
        }
    }

    @Test
    void testClearCubesFromCacheNameNotInCache()
    {
        Map cache = populateCache()
        ncubeRuntime.clearCache(ApplicationID.testAppId, ['testnone'])
        assert 4 == cache.size()
        assert cache.containsKey('testbranch')
        assert cache.containsKey('testage')
        assert cache.containsKey('testcube')
    }

    private static Map populateCache()
    {
        createRuntimeCubeFromResource('test.branch.1.json')
        createRuntimeCubeFromResource('test.branch.age.1.json')
        createRuntimeCubeFromResource('testCube1.json')
        Map cache = ((CaffeineCache) testClient.getCacheForApp(ApplicationID.testAppId)).nativeCache.asMap()
        return cache
    }
}
