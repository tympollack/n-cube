package com.cedarsoftware.ncube

import org.junit.Before
import org.junit.Test

import static com.cedarsoftware.ncube.TestWithPreloadedDatabase.*
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

class TestUrlClassLoader extends NCubeCleanupBaseTest
{
    @Before
    void setup()
    {
//        runtime.getCacheForApp(customId).cache.size()
    }

    private NCubeRuntime getRuntime()
    {
        return mutableClient as NCubeRuntime
    }
    
    private int getCacheSize(ApplicationID applicationID)
    {
        return runtime.getCacheForApp(applicationID).cache.size()
    }

    @Test
    void testToMakeSureOldStyleSysClasspathThrowsException()
    {
        preloadCubes(appId, 'sys.classpath.old.style.json')

        // nothing in cache until we try and get the classloader or load a cube.
        assertEquals(0, getCacheSize(appId))

        //  url classloader has 1 item
        try
        {
            mutableClient.getUrlClassLoader(appId, [:])
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'sys.classpath cube', 'exists', 'urlclassloader')
        }
    }

    @Test
    void testUrlClassLoader()
    {
        preloadCubes(appId, 'sys.classpath.cp1.json')

        // nothing in cache until we try and get the classloader or load a cube.
        assertEquals(0, getCacheSize(appId))

        //  url classloader has 1 item
        Map input = [:]
        URLClassLoader loader = mutableClient.getUrlClassLoader(appId, input)
        assertEquals(1, loader.URLs.length)
        assertEquals(2, getCacheSize(appId))
        assertEquals(new URL('http://files.cedarsoftware.com/tests/ncube/cp1/'), loader.URLs[0])

        assertEquals(2, getCacheSize(appId))

        assertNotNull(mutableClient.getUrlClassLoader(appId, input))
        assertEquals(2, getCacheSize(appId))

        mutableClient.clearCache(appId)
        assertEquals(0, getCacheSize(appId))

        assertEquals(1, mutableClient.getUrlClassLoader(appId, input).URLs.length)
        assertEquals(2, getCacheSize(appId))
    }

    @Test
    void testClearCacheWithClassLoaderLoadedByCubeRequest()
    {
        preloadCubes(appId, 'sys.classpath.cp1.json', 'GroovyMethodClassPath1.json')

        assertEquals(0, getCacheSize(appId))
        NCube cube = mutableClient.getCube(appId, 'GroovyMethodClassPath1')
        assertEquals(1, getCacheSize(appId))

        Map input = new HashMap()
        input.put('method', 'foo')
        Object x = cube.getCell(input)
        assertEquals('foo', x)

        assertEquals(3, getCacheSize(appId))

        input.put('method', 'foo2')
        x = cube.getCell(input)
        assertEquals('foo2', x)

        input.put('method', 'bar')
        x = cube.getCell(input)
        assertEquals('Bar', x)

        //  clear cache so we get different answers this time.  classpath 2 has already been loaded in database.
        mutableClient.clearCache(appId)

        assertEquals(0, getCacheSize(appId))

        cube = mutableClient.getCube(appId, 'GroovyMethodClassPath1')
        assertEquals(1, getCacheSize(appId))

        input = new HashMap()
        input.put('method', 'foo')
        x = cube.getCell(input)
        assertEquals('boo', x)

        assertEquals(3, getCacheSize(appId))

        input.put('method', 'foo2')
        x = cube.getCell(input)
        assertEquals('boo2', x)

        input.put('method', 'bar')
        x = cube.getCell(input)
        assertEquals('far', x)
    }

    @Test
    void testMultiCubeClassPath()
    {
        preloadCubes(appId, "sys.classpath.base.json", "sys.classpath.json", "sys.status.json", "sys.versions.json", "sys.version.json", "GroovyMethodClassPath1.json")

        assertEquals(0, NCubeManager.getCacheForApp(appId).size())
        NCube cube = NCubeManager.getCube(appId, "GroovyMethodClassPath1")

        // classpath isn't loaded at this point.
        assertEquals(1, NCubeManager.getCacheForApp(appId).size())

        def input = [:]
        input.env = "DEV"
        input.put("method", "foo")
        Object x = cube.getCell(input)
        assertEquals("foo", x)

        assertEquals(5, NCubeManager.getCacheForApp(appId).size())

        // cache hasn't been cleared yet.
        input.put("method", "foo2")
        x = cube.getCell(input)
        assertEquals("foo2", x)

        input.put("method", "bar")
        x = cube.getCell(input)
        assertEquals("Bar", x)

        NCubeManager.clearCache(appId)

        // Had to reget cube so I had a new classpath
        cube = NCubeManager.getCube(appId, "GroovyMethodClassPath1")

        input.env = 'UAT'
        input.put("method", "foo")
        x = cube.getCell(input)

        assertEquals("boo", x)

        assertEquals(5, NCubeManager.getCacheForApp(appId).size())

        input.put("method", "foo2")
        x = cube.getCell(input)
        assertEquals("boo2", x)

        input.put("method", "bar")
        x = cube.getCell(input)
        assertEquals("far", x)

        //  clear cache so we get different answers this time.  classpath 2 has already been loaded in database.
        NCubeManager.clearCache(appId)
        assertEquals(0, NCubeManager.getCacheForApp(appId).size())
    }
}