package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Ignore
import org.junit.Test

@CompileStatic
class TestNCubeClient
{
    private static final ApplicationID TEST_APP = new ApplicationID('NONE', 'test.app.1', '2.0.4', 'SNAPSHOT', 'jsnyder4')
    private NCubeRuntimeClient ncubeClient = NCubeRuntime.instance

    @Ignore
    @Test
    void testHttpProxy()
    {
//        def names = NCubeManagerFull.appNames
//        println names
    }

    @Ignore
    @Test
    void testDirectProxy()
    {
//        NCubeManagerImpl impl = new NCubeManagerImpl()
//        NCubeClientDirectProxy.manager = impl
//        NCubeClient client = NCubeClientDirectProxy.ncubeClient
//
//        NCubeManagerFull.client = client
//        def names = NCubeManagerFull.appNames
//        println names
    }

    @Test
    void testPlinko()
    {
        NCube ncube = ncubeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'RATP'])
        assert 'waldo' == result
    }

    @Test
    void testSearch()
    {
        Object[] dtos = ncubeClient.search(TEST_APP, '0Plinko', null, null)
        ncubeClient.search(TEST_APP, '0Plinko', null, null)
        ncubeClient.search(TEST_APP, '0Plinko', null, null)
        ncubeClient.search(TEST_APP, '0Plinko', null, null)
        ncubeClient.search(TEST_APP, '0Plinko', null, null)
        ncubeClient.search(TEST_APP, '0Plinko', null, null)
        assert 1 == dtos.size()
    }

    @Test
    void testHttpReference()
    {
        NCube ncube = ncubeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1', bu: 'SHS'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRelativeUrl()
    {
        NCube ncube = ncubeClient.getCube(TEST_APP, '0Plinko')
        def result = ncube.getCell([setting: 'prop1'])
        assert 'Hello, world.' == result
    }

    @Test
    void testRefAxis()
    {
        NCube ncube = ncubeClient.getCube(TEST_APP, '0RefAxisAndOrder')
        def result = ncube.getCell([test: 'test1', letter: 'a', ref: 'USAddress'])
        assert 'foo' == result
    }
}