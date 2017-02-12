package com.cedarsoftware.ncube

import com.cedarsoftware.util.JsonHttpClient
import groovy.transform.CompileStatic
import org.junit.Test

import static org.junit.Assert.fail

@CompileStatic
class TestNCubeClient
{
    private static final ApplicationID TEST_APP = new ApplicationID('NONE', 'test.app.1', '2.0.4', 'SNAPSHOT', 'jsnyder4')
    private static NCubeRuntime runtimeClient = new NCubeRuntime(
            new JsonHttpClient('nce-sb.td.afg', 443, 'n-cube-editor', 'jsnyder4', 'Winter2016'),
            new NCubeCacheManager(),
            new NCubeCacheManager(),
            true)

    static NCubeRuntime getNcubeRuntime()
    {
        return runtimeClient
    }

    @Test
    void testPlinko()
    {
        NCube ncube = runtimeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'RATP'])
        assert 'waldo' == result
    }

    @Test
    void testSearch()
    {
        Object[] dtos = runtimeClient.search(TEST_APP, '0Plinko', null, null)
        runtimeClient.search(TEST_APP, '0Plinko', null, null)
        runtimeClient.search(TEST_APP, '0Plinko', null, null)
        runtimeClient.search(TEST_APP, '0Plinko', null, null)
        runtimeClient.search(TEST_APP, '0Plinko', null, null)
        runtimeClient.search(TEST_APP, '0Plinko', null, null)
        assert 1 == dtos.size()
    }

    @Test
    void testHttpReference()
    {
        NCube ncube = runtimeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'SHS'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRelativeUrl()
    {
        NCube ncube = runtimeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRefAxis()
    {
        NCube ncube = runtimeClient.getCube(TEST_APP, '0RefAxisAndOrder')
        def result = ncube.getCell([test: 'test1', letter: 'a', place: 'USAddress'])
        assert 'foo' == result
    }

    @Test
    void testUpdateCube()
    {
        NCube ncube = runtimeClient.getCube(TEST_APP, '0RefAxisAndOrder')
        ncube.deleteAxis('test')
        try
        {
            runtimeClient.updateCube(ncube)
            fail()
        }
        catch (IllegalStateException e)
        {
            assert e.message.toLowerCase().contains('non-runtime')
        }
    }
}