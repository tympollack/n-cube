package com.cedarsoftware.ncube

import com.cedarsoftware.util.JsonHttpProxy
import groovy.transform.CompileStatic
import org.junit.Ignore
import org.junit.Test

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

@CompileStatic
@Ignore
// TODO: Figure out how to run only if active profile = 'client'
class TestJavascriptAPIs extends NCubeCleanupBaseTest
{
    private static final ApplicationID BRANCH1 = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'test', '1.28.0', ReleaseStatus.SNAPSHOT.name(), 'FOO')
    private static final ApplicationID BRANCH2 = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'test', '1.28.0', ReleaseStatus.SNAPSHOT.name(), 'BAR')

    @Test
    void testIsCubeUpToDate()
    {
        NCube ncube1 = NCubeBuilder.discrete1D
        NCube ncube2 = NCubeBuilder.discrete1D
        ncube1.applicationID = BRANCH1
        mutableClient.createCube(ncube1)
        ncube2.applicationID = BRANCH2
        mutableClient.createCube(ncube2)

        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // new state (cube not in HEAD) is considered up-to-date
        mutableClient.commitBranch(ncube1.applicationID)

        assert !mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // Not up-to-date because BRANCH2 created cube (no HEAD sha1) which matches a cube in HEAD
        mutableClient.commitBranch(ncube2.applicationID)

        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // same as HEAD, up-to-date
        assert !mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // same as HEAD, but no HEAD SHA1

        mutableClient.updateBranch(BRANCH2)                                       // pick up changes from HEAD
        assert mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)    // same as HEAD, with HEAD-SHA1 now on branch cube

        ncube2.addColumn('state', 'AL')
        mutableClient.updateCube(ncube2)
        mutableClient.commitBranch(BRANCH2)
        assert !mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // out of date
        assert mutableClient.isCubeUpToDate(ncube2.applicationID, ncube2.name)

        mutableClient.updateBranch(BRANCH1)                                       // pick up changes from HEAD
        assert mutableClient.isCubeUpToDate(ncube1.applicationID, ncube1.name)    // up to date

        assert mutableClient.isCubeUpToDate(BRANCH1.asHead(), ncube1.name)          // HEAD is always true
    }

    @Test
    void testHeartBeat()
    {
        Map health = call('heartBeat', [[:]]) as Map
        assert health
        Map stats = health.serverStats as Map
        assert stats.containsKey('User ID')
        assert stats.containsKey('Java version')
    }

    @Test
    void testGetApplicationID()
    {
        System.setProperty("NCUBE_PARAMS", '{"branch":"jose"}')

        ApplicationID appId = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'ncube.test', '0.0.0', 'SNAPSHOT', 'jose')
        createCubeFromResource(appId, "sys.bootstrap.test.1.json")

        ApplicationID bootId = runtimeClient.getApplicationID(appId.tenant, appId.app, [env:null]) as ApplicationID
        assert bootId.tenant == 'NONE'
        assert bootId.app == 'ncube.test'
        assert bootId.version == '1.28.0'
        assert bootId.status == 'RELEASE'
        assert bootId.branch == 'HEAD'
    }
    
    @Test
    void testFetchJsonRevDiffs()
    {
        createCubeFromResource(BRANCH1, 'test.branch.1.json')
        List<NCubeInfoDto> cubes = runtimeClient.search(BRANCH1, 'TestBranch', null, null)
        assert cubes.size() == 1
        NCubeInfoDto origDto = cubes[0]
        String origId = origDto.id
        NCube foo = runtimeClient.getNCubeFromResource(BRANCH1, 'test.branch.2.json')
        mutableClient.updateCube(foo)
        List<NCubeInfoDto> cubes2 = runtimeClient.search(BRANCH1, 'TestBranch', null, null)
        assert cubes2.size() == 1
        NCubeInfoDto newDto = cubes2[0]
        String newId = newDto.id

        List<Delta> result = call('fetchJsonRevDiffs', [newId, origId]) as List
        assert result.size() == 4
    }

    @Test
    void testFetchJsonBranchDiffs()
    {
        createCubeFromResource(BRANCH1, 'test.branch.1.json')
        createCubeFromResource(BRANCH2, 'test.branch.2.json')
        List<NCubeInfoDto> cubes = runtimeClient.search(BRANCH1, 'TestBranch', null, null)
        assert cubes.size() == 1
        NCubeInfoDto origDto = cubes[0]
        List<NCubeInfoDto> cubes2 = runtimeClient.search(BRANCH2, 'TestBranch', null, null)
        assert cubes2.size() == 1
        NCubeInfoDto newDto = cubes2[0]

        List<Delta> result = call('fetchJsonBranchDiffs', [newDto, origDto]) as List
        assert result.size() == 4
    }

    private Object call(String methodName, List args)
    {
        JsonHttpProxy proxy = NCubeAppContext.getBean('jsonHttpProxy') as JsonHttpProxy
        proxy.invokeMethod('call', ['ncubeController', methodName, args])
    }
}
