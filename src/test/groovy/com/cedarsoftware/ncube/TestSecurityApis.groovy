package com.cedarsoftware.ncube

import com.cedarsoftware.util.EnvelopeException
import org.junit.Ignore
import org.junit.Test

import static com.cedarsoftware.ncube.TestNCubeManager.defaultBootApp
import static com.cedarsoftware.ncube.TestNCubeManager.defaultSnapshotApp
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

/**
 * NCubeManager Tests
 *
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

@Ignore
class TestSecurityApis extends NCubeCleanupBaseTest
{
    private static NCubeManagerImpl getManager()
    {
        return mutableClient as NCubeManagerImpl
    }

    @Test
    void testSysLockSecurity()
    {
        String origUser = manager.userId
        ApplicationID branchBootAppId = defaultBootApp.asBranch(origUser)
        Map lockCoord = [(AXIS_SYSTEM): null]

        // create branch
        manager.copyBranch(branchBootAppId.asHead(), branchBootAppId)

        // update sys lock in branch
        NCube sysLockCube = manager.getCube(branchBootAppId, SYS_LOCK)
        sysLockCube.setCell(origUser, lockCoord)
        manager.updateCube(sysLockCube)

        // commit sys lock to HEAD
        Object[] cubeInfos = manager.search(branchBootAppId, SYS_LOCK, null, [(SEARCH_ACTIVE_RECORDS_ONLY): true])
        Map<String, Object> commitResult = manager.commitBranch(branchBootAppId, cubeInfos)
        assertEquals(1, (commitResult[mutableClient.BRANCH_UPDATES] as Map).size())

        // make sure HEAD took the lock
        sysLockCube = manager.getCube(branchBootAppId, SYS_LOCK)
        NCube headSysLockCube = manager.getCube(defaultBootApp, SYS_LOCK)
        assertEquals(sysLockCube.getCell(lockCoord), headSysLockCube.getCell(lockCoord))

        // try creating a new cube in branch, should get exception
        NCube testCube = new NCube('test')
        testCube.applicationID = branchBootAppId
        manager.createCube(testCube)  // works without error because current user has the lock

        String currUser = manager.userId
        manager.userId = 'garpley'                   // change user
        try
        {
            manager.updateCube(testCube)
            fail()
        }
        catch (SecurityException e)
        {
            assertTrue(e.message.contains('Application is not locked by you'))
        }
        manager.userId = origUser
    }

    @Test
    void testIsAdminFail()
    {
        String origUser = manager.userId
        assertNotNull(manager.getCube(defaultBootApp, SYS_USERGROUPS))
        manager.userId = 'bad'
        assert !manager.isAdmin(defaultSnapshotApp)
        manager.userId = origUser
    }

    @Test
    void testImpersonation()
    {
        String origUser = manager.userId
        assertNotNull(manager.getCube(defaultBootApp, SYS_USERGROUPS))
        assert manager.isAdmin(defaultSnapshotApp)

        String fake = 'fakeUserId'
        manager.fakeId = fake

        assert manager.userId != fake
        assert manager.fakeId == fake
        assert manager.impliedId == fake
        assert !manager.isAdmin(defaultSnapshotApp)

        String cubeName = 'test.cube'
        NCube cube = new NCube(cubeName)
        cube.applicationID = defaultSnapshotApp
        cube.addAxis(new Axis('axis', AxisType.DISCRETE, AxisValueType.STRING, true))
        cube.setCell('test', [axis: null])
        manager.createCube(cube)
        List<NCubeInfoDto> revs = manager.getRevisionHistory(defaultSnapshotApp, cubeName)
        assert manager.userId == revs.first().createHid
        manager.userId = origUser
    }

    @Test
    void testAppPermissionsFail()
    {
        String origUser = manager.userId
        String otherUser = 'otherUser'
        String testAxisName = 'testAxis'
        ApplicationID branchBootApp = defaultBootApp.asBranch(ApplicationID.TEST_BRANCH)

        //check app permissions cubes
        assertNotNull(manager.getCube(defaultBootApp, SYS_PERMISSIONS))
        assertNotNull(manager.getCube(defaultBootApp, SYS_USERGROUPS))
        assertNotNull(manager.getCube(defaultBootApp, SYS_LOCK))

        //set otheruser as having branch permissions
        NCube branchPermCube = manager.getCube(branchBootApp, SYS_BRANCH_PERMISSIONS)
        branchPermCube.getAxis(AXIS_USER).addColumn(otherUser)
        branchPermCube.setCell(true, [(AXIS_USER): otherUser, (AXIS_RESOURCE): null])
        manager.updateCube(branchPermCube)

        //set otheruser as no app permissions
        NCube userCube = manager.getCube(defaultBootApp, SYS_USERGROUPS)
        userCube.getAxis(AXIS_USER).addColumn(otherUser)
        userCube.setCell(true, [(AXIS_USER): otherUser, (AXIS_ROLE): ROLE_READONLY])
        manager.persister.updateCube(userCube, origUser)
        assertFalse(userCube.getCell([(AXIS_USER): otherUser, (AXIS_ROLE): ROLE_USER]) as Boolean)
        assertTrue(userCube.getCell([(AXIS_USER): otherUser, (AXIS_ROLE): ROLE_READONLY]) as Boolean)

        NCube testCube = new NCube('test')
        testCube.applicationID = defaultSnapshotApp
        testCube.addAxis(new Axis(testAxisName, AxisType.DISCRETE, AxisValueType.STRING, true))
        manager.createCube(testCube)

        //try to update a cube from bad user
        try
        {
            manager.userId = otherUser
            testCube.setCell('testval', [(testAxisName): null])
            manager.updateCube(testCube)
            fail()
        }
        catch (SecurityException e)
        {
            assertTrue(e.message.contains('not performed'))
            assertTrue(e.message.contains(Action.UPDATE.name()))
            assertTrue(e.message.contains(testCube.name))
        }
        manager.userId = origUser
    }

    @Test
    void testBranchPermissionsFail()
    {
        String testAxisName = 'testAxis'
        String origUser = manager.userId
        ApplicationID appId = defaultSnapshotApp.asBranch(origUser)

        //create new branch and make sure of permissions
        manager.copyBranch(appId.asHead(), appId)
        NCube branchPermCube = manager.getCube(defaultBootApp.asBranch(origUser), SYS_BRANCH_PERMISSIONS)
        Axis userAxis = branchPermCube.getAxis(AXIS_USER)
        List<Column> columnList = userAxis.columnsWithoutDefault
        assertEquals(1, columnList.size())
        assertEquals(origUser, columnList.get(0).value)

        //check app permissions cubes
        assertNotNull(manager.getCube(defaultBootApp, SYS_PERMISSIONS))
        assertNotNull(manager.getCube(defaultBootApp, SYS_USERGROUPS))
        assertNotNull(manager.getCube(defaultBootApp, SYS_LOCK))

        //new cube on branch from good user
        NCube testCube = new NCube('test')
        testCube.applicationID = appId
        testCube.addAxis(new Axis(testAxisName, AxisType.DISCRETE, AxisValueType.STRING, true))
        manager.createCube(testCube)

        //try to create a cube as a different user in that branch
        try
        {
            manager.userId = 'otherUser'
            testCube.setCell('testval', [(testAxisName): null])
            manager.updateCube(testCube)
            fail()
        }
        catch (SecurityException e)
        {
            assertTrue(e.message.contains('not performed'))
            assertTrue(e.message.contains(Action.UPDATE.name()))
            assertTrue(e.message.contains(testCube.name))
        }
        manager.userId = origUser
    }

    @Test
    void testCommitFailsWithoutPermissions()
    {
        preloadCubes(BRANCH2, "test.branch.1.json")
        mutableClient.commitBranch(BRANCH2)
        mutableClient.copyBranch(HEAD, BRANCH1)

        // cube is updated by someone with access
        String cubeName = 'TestBranch'
        NCube testBranchCube = mutableClient.getCube(BRANCH1, cubeName)
        testBranchCube.setCell('AAA', [Code: 15])
        mutableClient.updateCube(testBranchCube)

        // set permission on cube to deny commit for normal user
        ApplicationID branchBoot = BRANCH1.asVersion('0.0.0')
        NCube sysPermissions = mutableClient.getCube(branchBoot, SYS_PERMISSIONS)
        sysPermissions.addColumn(AXIS_RESOURCE, cubeName)
        sysPermissions.setCell(true, [(AXIS_RESOURCE): cubeName, (AXIS_ROLE): ROLE_USER, (AXIS_ACTION): Action.READ.lower()])
        mutableClient.updateCube(sysPermissions)
        List<NCubeInfoDto> dtos = runtimeClient.search(branchBoot, SYS_PERMISSIONS, null, null)
        assert dtos.size() == 1
        NCubeInfoDto permissionDto = dtos[0]
        mutableClient.commitBranch(branchBoot, permissionDto)

        // set testUser to have user role on branch
        NCube sysBranchPermissions = mutableClient.getCube(branchBoot, SYS_BRANCH_PERMISSIONS)
        String testUser = 'testUser'
        sysBranchPermissions.addColumn(AXIS_USER, testUser)
        sysBranchPermissions.setCell(true, [(AXIS_USER): testUser])
        mutableClient.updateCube(sysBranchPermissions)

        // impersonate testUser, who shouldn't be able to commit the changed cube
        mutableClient.userId = testUser

        try
        {
            mutableClient.commitBranch(BRANCH1)
            fail()
        }
        catch (EnvelopeException e)
        {
            assert (e.envelopeData[mutableClient.BRANCH_ADDS] as Map).size() == 0
            assert (e.envelopeData[mutableClient.BRANCH_DELETES] as Map).size() == 0
            assert (e.envelopeData[mutableClient.BRANCH_UPDATES] as Map).size() == 0
            assert (e.envelopeData[mutableClient.BRANCH_RESTORES] as Map).size() == 0
            assert (e.envelopeData[mutableClient.BRANCH_REJECTS] as Map).size() == 1
        }
    }
}