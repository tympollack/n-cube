package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

@CompileStatic
@RunWith(SpringRunner.class)
@ContextConfiguration(locations = ['/config/beans.xml', '/config/nce.xml'])
@ActiveProfiles(profiles = ['runtime'])
class TestStuff
{

    private static NCubeRuntime getRuntimeClient()
    {
        return NCubeRuntime.instance
    }

    @Before
    void setUp()
    {
        NCube cp = NCubeManager.getNCubeFromResource(TestNCubeManager.defaultSnapshotApp, 'sys.classpath.tests.json')
        runtimeClient.createCube(cp)
        cp = NCubeManager.getNCubeFromResource(ApplicationID.testAppId, 'sys.classpath.tests.json')
        runtimeClient.createCube(cp)
    }

    @After
    void tearDown()
    {

    }

    @Test
    void testStuff()
    {
        println 'test'
    }
}