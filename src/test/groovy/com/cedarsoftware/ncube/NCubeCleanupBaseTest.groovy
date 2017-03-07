package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Ignore

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
@Ignore
class NCubeCleanupBaseTest extends NCubeBaseTest
{
    private String NCUBE_PARAMS

    @Before
    void setup()
    {
        NCUBE_PARAMS = System.getProperty('NCUBE_PARAMS')
        NCube cp = runtimeClient.getNCubeFromResource(TestNCubeManager.defaultSnapshotApp, 'sys.classpath.tests.json')
        mutableClient.createCube(cp)
        cp = runtimeClient.getNCubeFromResource(ApplicationID.testAppId, 'sys.classpath.tests.json')
        mutableClient.createCube(cp)
    }

    @After
    void teardown()
    {
        if (NCUBE_PARAMS)
        {
            System.setProperty('NCUBE_PARAMS', NCUBE_PARAMS)
        }
        NCubeRuntime runtime = mutableClient as NCubeRuntime
        runtime.clearTestDatabase()
        runtime.clearCache()
    }

    NCube createCubeFromResource(ApplicationID appId = ApplicationID.testAppId, String fileName)
    {
        String json = NCubeRuntime.getResourceAsString(fileName)
        NCube ncube = NCube.fromSimpleJson(json)
        ncube.applicationID = appId
        mutableClient.createCube(ncube)
        return ncube
    }

    void preloadCubes(ApplicationID id, String ...names)
    {
        for (String name : names)
        {
            createCubeFromResource(id, name)
        }
        runtimeClient.clearCache(id)
    }
}