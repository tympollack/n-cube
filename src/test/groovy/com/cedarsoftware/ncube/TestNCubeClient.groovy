package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

import static org.junit.Assert.fail

@CompileStatic
@RunWith(SpringRunner.class)
@ContextConfiguration(locations = ['/config/beans.xml'])
@ActiveProfiles(profiles = ['runtime','test-immutable'])
class TestNCubeClient
{
    private static final ApplicationID TEST_APP = new ApplicationID('NONE', 'test.app.1', '2.0.4', 'SNAPSHOT', 'jsnyder4')

    private NCubeMutableClient getClient()
    {
        return NCubeRuntime.instance
    }

    @Test
    void testPlinko()
    {
        NCube ncube = client.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'RATP'])
        assert 'waldo' == result
    }

    @Test
    void testSearch()
    {
        Object[] dtos = client.search(TEST_APP, '0Plinko', null, null)
        client.search(TEST_APP, '0Plinko', null, null)
        client.search(TEST_APP, '0Plinko', null, null)
        client.search(TEST_APP, '0Plinko', null, null)
        client.search(TEST_APP, '0Plinko', null, null)
        client.search(TEST_APP, '0Plinko', null, null)
        assert 1 == dtos.size()
    }

    @Test
    void testHttpReference()
    {
        NCube ncube = client.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'SHS'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRelativeUrl()
    {
        NCube ncube = client.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRefAxis()
    {
        NCube ncube = client.getCube(TEST_APP, '0RefAxisAndOrder')
        def result = ncube.getCell([test: 'test1', letter: 'a', place: 'USAddress'])
        assert 'foo' == result
    }

    @Test
    void testUpdateCube()
    {
        NCube ncube = client.getCube(TEST_APP, '0output')
        ncube.deleteAxis('Row')
        try
        {
            client.updateCube(ncube)
            fail()
        }
        catch (IllegalStateException e)
        {
            assert e.message.toLowerCase().contains('non-runtime')
        }
    }
}