package com.cedarsoftware.ncube

import com.cedarsoftware.util.EnvelopeException
import com.cedarsoftware.util.io.JsonReader
import groovy.transform.CompileStatic
import org.junit.Test

/**
 * NCubeController Tests
 *
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
class TestCommit extends NCubeCleanupBaseTest
{
    private static ApplicationID appId = ApplicationID.testAppId
    private static ApplicationID sysApp = new ApplicationID(ApplicationID.DEFAULT_TENANT, 'sys.app', '0.0.0', ReleaseStatus.SNAPSHOT.toString(), ApplicationID.HEAD)

    @Test
    void testGenerateCommitLink()
    {
        NCube ncube = createCubeFromResource('test.branch.1.json')
        List<NCubeInfoDto> dtos = mutableClient.search(appId, ncube.name, null, null)
        String commitId = mutableClient.generateCommitLink(appId, dtos.toArray())
        assert commitId

        NCube commitCube = mutableClient.getCube(sysApp, "tx.${commitId}")
        assert 'open' == commitCube.getCell([property: 'status'])

        String appIdStr = commitCube.getCell([property: 'appId'])
        ApplicationID commitApp = ApplicationID.convert(appIdStr)
        assert appId == commitApp
//        assert 'NONE/DEFAULT_APP/999.99.9/SNAPSHOT/TEST/' == appIdStr

        String cubeNames = commitCube.getCell([property: 'cubeNames'])
        List commitInfos = JsonReader.jsonToJava(cubeNames) as List
        assert 1 == commitInfos.size()
        Map commitInfo = commitInfos[0]
        assert commitInfo.name == 'TestBranch'
        assert commitInfo.changeType == null
        assert commitInfo.id
        assert commitInfo.head == null
//        assertContainsIgnoreCase(cubeNames, 'java.util.ArrayList', 'java.util.LinkedHashMap', 'name', 'TestBranch', 'changeType', 'null', 'id', 'head', 'null')
    }

    @Test
    void testCancelAndReopenCommit()
    {
        NCube ncube = createCubeFromResource('test.branch.1.json')
        List<NCubeInfoDto> dtos = mutableClient.search(appId, ncube.name, null, null)
        String commitId = mutableClient.generateCommitLink(appId, dtos.toArray())
        assert commitId

        NCube commitCube = mutableClient.getCube(sysApp, "tx.${commitId}")
        assert 'open' == commitCube.getCell([property: 'status'])

        // cancel commit
        mutableClient.cancelCommit(commitId)
        commitCube = mutableClient.getCube(sysApp, "tx.${commitId}")
        assert 'closed cancelled' == commitCube.getCell([property: 'status'])

        // attempt to cancel a previously cancelled commit
        try
        {
            mutableClient.cancelCommit(commitId)
        }
        catch (EnvelopeException e)
        {
            assertEnvelopeExceptionContains(e, 'request', 'closed')
        }

        // reopen a commit
        mutableClient.reopenCommit(commitId)
        commitCube = mutableClient.getCube(sysApp, "tx.${commitId}")
        assert 'open' == commitCube.getCell([property: 'status'])

        // attempt to reopen a previously reopened commit
        try
        {
            mutableClient.cancelCommit(commitId)
        }
        catch (EnvelopeException e)
        {
            assertEnvelopeExceptionContains(e, 'request', 'closed')
        }
    }

    @Test
    void testGetCommits()
    {
        preloadCubes(ApplicationID.testAppId, 'test.branch.1.json', 'test.branch.age.1.json')
        List<NCubeInfoDto> branchDtos = mutableClient.search(appId, 'TestBranch', null, null)
        List<NCubeInfoDto> ageDtos = mutableClient.search(appId, 'TestAge', null, null)
        mutableClient.generateCommitLink(appId, branchDtos.toArray())
        mutableClient.generateCommitLink(appId, ageDtos.toArray())

        Object[] commits = mutableClient.commits
        assert 2 == commits.length
    }
}