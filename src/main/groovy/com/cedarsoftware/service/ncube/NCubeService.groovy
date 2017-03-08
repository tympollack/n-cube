package com.cedarsoftware.service.ncube

import com.cedarsoftware.ncube.*
import groovy.transform.CompileStatic
import org.springframework.stereotype.Service

/**
 * RESTful Ajax/JSON API for editor application
 *
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
@Service
class NCubeService implements NCubeMutableClient
{
    private NCubeClient ncubeClient

    NCubeService(NCubeClient ncubeClient)
    {
        this.ncubeClient = ncubeClient
    }

    NCubeRuntimeClient getRuntimeClient()
    {
        return ncubeClient as NCubeRuntimeClient
    }

    NCubeMutableClient getMutableClient()
    {
        return ncubeClient as NCubeMutableClient
    }

    void setUserId(String user)
    {
        mutableClient.userId = user
    }

    List<NCubeInfoDto> search(ApplicationID appId, String cubeNamePattern, String contentMatching, Map options)
    {
         return mutableClient.search(appId, cubeNamePattern, contentMatching, options)
    }

    Boolean restoreCubes(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.restoreCubes(appId, cubeNames)
    }

    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion = false)
    {
        return mutableClient.getRevisionHistory(appId, cubeName, ignoreVersion)
    }

    List<String> getAppNames()
    {
        throw new IllegalStateException("getAppNames() should not be called on ${getClass().name} without tenant. Please check Spring bean configuration.")
    }

    List<String> getAppNames(String tenant)
    {
        return mutableClient.getAppNames(tenant)
    }

    Object[] getVersions(String app)
    {
        throw new IllegalStateException("getVersions() should not be called on ${getClass().name} without tenant. Please check Spring bean configuration.")
    }
    
    Map<String, List<String>> getVersions(String tenant, String app)
    {
        return mutableClient.getVersions(tenant, app)
    }

    Integer copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory = false)
    {
        return mutableClient.copyBranch(srcAppId, targetAppId, copyWithHistory)
    }

    Set<String> getBranches(ApplicationID appId)
    {
        return mutableClient.getBranches(appId)
    }

    Integer getBranchCount(ApplicationID appId)
    {
        return mutableClient.getBranchCount(appId)
    }

    List<NCubeInfoDto> getBranchChangesForHead(ApplicationID appId)
    {
        return mutableClient.getBranchChangesForHead(appId)
    }

    List<NCubeInfoDto> getHeadChangesForBranch(ApplicationID appId)
    {
        return mutableClient.getHeadChangesForBranch(appId)
    }

    List<NCubeInfoDto> getBranchChangesForMyBranch(ApplicationID appId, String branch)
    {
        return mutableClient.getBranchChangesForMyBranch(appId, branch)
    }

    String generateCommitLink(ApplicationID appId, Object[] infoDtos)
    {
        return mutableClient.generateCommitLink(appId, infoDtos)
    }

    Map<String, Object> honorCommit(String commitId)
    {
        return mutableClient.honorCommit(commitId)
    }

    NCube cancelCommit(String commitId)
    {
        return mutableClient.cancelCommit(commitId)
    }

    NCube reopenCommit(String commitId)
    {
        return mutableClient.reopenCommit(commitId)
    }

    Object[] getCommits()
    {
        return mutableClient.commits
    }

    Map<String, Object> commitBranch(ApplicationID appId, Object[] infoDtos = null)
    {
        return mutableClient.commitBranch(appId, infoDtos)
    }

    Integer rollbackCubes(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.rollbackCubes(appId, cubeNames)
    }
    
    Map<String, Object> updateBranch(ApplicationID appId, Object[] cubeDtos = null)
    {
        return mutableClient.updateBranch(appId, cubeDtos)
    }

    Boolean deleteBranch(ApplicationID appId)
    {
        return mutableClient.deleteBranch(appId)
    }

    NCube mergeDeltas(ApplicationID appId, String cubeName, List<Delta> deltas)
    {
        return mutableClient.mergeDeltas(appId, cubeName, deltas)
    }

    Integer mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch = ApplicationID.HEAD)
    {
        return mutableClient.mergeAcceptTheirs(appId, cubeNames, sourceBranch)
    }

    Integer mergeAcceptMine(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.mergeAcceptMine(appId, cubeNames)
    }

    void createCube(NCube ncube)
    {
        mutableClient.createCube(ncube)
    }

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.deleteCubes(appId, cubeNames)
    }

    Boolean duplicate(ApplicationID appId, ApplicationID destAppId, String cubeName, String newName)
    {
        return mutableClient.duplicate(appId, destAppId, cubeName, newName)
    }

    Integer releaseCubes(ApplicationID appId, String newSnapVer)
    {
        return mutableClient.releaseCubes(appId, newSnapVer)
    }

    void changeVersionValue(ApplicationID appId, String newSnapVer)
    {
        mutableClient.changeVersionValue(appId, newSnapVer)
    }

    void addAxis(ApplicationID appId, String cubeName, String axisName, String type, String valueType)
    {
        throw new IllegalStateException('should not get here')
    }

    void addAxis(ApplicationID appId, String cubeName, String axisName, ApplicationID refAppId, String refCubeName, String refAxisName, ApplicationID transformAppId, String transformCubeName, String transformMethodName)
    {
        throw new IllegalStateException('should not get here')
    }

    /**
     * Delete the specified axis.
     */
    void deleteAxis(ApplicationID appId, String name, String axisName)
    {
        throw new IllegalStateException('should not get here')
    }

    /**
     * Update the 'informational' part of the Axis (not the columns).
     */
    void updateAxis(ApplicationID appId, String name, String origAxisName, String axisName, boolean hasDefault, boolean isSorted, boolean fireAll)
    {
        throw new IllegalStateException('should not get here')
    }

    /**
     * Removes the reference from one axis to another.
     */
    void breakAxisReference(ApplicationID appId, String name, String axisName)
    {
        throw new IllegalStateException('should not get here')
    }

    /**
     * In-place update of a cell.  'Value' is the final (converted) object type to be stored
     * in the indicated (by colIds) cell.
     */
    Boolean updateCube(NCube ncube)
    {
        return mutableClient.updateCube(ncube)
    }

    Boolean renameCube(ApplicationID appId, String oldName, String newName)
    {
        return mutableClient.renameCube(appId, oldName, newName)
    }

    /**
     * Update / Save a single n-cube -or- create / update a batch of n-cubes, represented as a JSON
     * array [] of n-cubes.
     */
    void updateCube(ApplicationID appId, String json)
    {
        throw new IllegalStateException("Never call this method")
    }

    Object[] getTestData(ApplicationID appId, String cubeName)
    {
        return mutableClient.getTestData(appId, cubeName)
    }

    Boolean saveTests(ApplicationID appId, String cubeName, Object[] tests)
    {
        return mutableClient.saveTests(appId, cubeName, tests)
    }

    Boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        return mutableClient.updateNotes(appId, cubeName, notes)
    }

    String getNotes(ApplicationID appId, String cubeName)
    {
        return mutableClient.getNotes(appId, cubeName)
    }

    NCube getCube(ApplicationID appId, String name, boolean quiet = false)
    {
        NCube cube = mutableClient.getCube(appId, name)
        if (cube == null && !quiet)
        {
            throw new IllegalArgumentException("Unable to load cube: " + name + " for app: " + appId)
        }
        return cube
    }

    NCube loadCube(ApplicationID appId, String name)
    {
        NCube cube = mutableClient.getCube(appId, name)
        if (cube == null)
        {
            throw new IllegalArgumentException("Unable to load cube: " + name + " for app: " + appId)
        }
        return cube
    }

    NCube loadCubeById(long id)
    {
        NCube cube = mutableClient.loadCubeById(id)
        if (cube == null)
        {
            throw new IllegalArgumentException('Unable to load cube by id: ' + id)
        }
        return cube
    }

    Set<String> getReferencedCubeNames(ApplicationID appId, String cubeName)
    {
        return mutableClient.getReferencedCubeNames(appId, cubeName)
    }

    URL resolveRelativeUrl(ApplicationID appId, String relativeUrl)
    {
        return runtimeClient.getActualUrl(appId, relativeUrl, [:])
    }

    void clearCache(ApplicationID appId)
    {
        runtimeClient.clearCache(appId)
    }

    void setFakeId(String fake)
    {
        mutableClient.fakeId = fake
    }

    String getImpliedId()
    {
        mutableClient.impliedId
    }

    Boolean isAdmin(ApplicationID appId, boolean useRealId = false)
    {
        mutableClient.isAdmin(appId, useRealId)
    }

    List<AxisRef> getReferenceAxes(ApplicationID appId)
    {
        return mutableClient.getReferenceAxes(appId)
    }

    void updateReferenceAxes(List<AxisRef> axisRefs)
    {
        mutableClient.updateReferenceAxes(axisRefs)
    }

    Boolean assertPermissions(ApplicationID appId, String resource, Action action)
    {
        mutableClient.assertPermissions(appId, resource, action ?: Action.READ)
    }

    Boolean checkPermissions(ApplicationID appId, String resource, Action action)
    {
        mutableClient.checkPermissions(appId, resource, action)
    }

    String getAppLockedBy(ApplicationID appId)
    {
        mutableClient.getAppLockedBy(appId)
    }

    Boolean lockApp(ApplicationID appId)
    {
        return mutableClient.lockApp(appId)
    }

    void unlockApp(ApplicationID appId)
    {
        mutableClient.unlockApp(appId)
    }

    Integer moveBranch(ApplicationID appId, String newSnapVer)
    {
        return mutableClient.moveBranch(appId, newSnapVer)
    }

    Integer releaseVersion(ApplicationID appId, String newSnapVer)
    {
        return mutableClient.releaseVersion(appId, newSnapVer)
    }

    Boolean isCubeUpToDate(ApplicationID appId, String cubeName)
    {
        return mutableClient.isCubeUpToDate(appId, cubeName)
    }

    void clearTestDatabase()
    {
        mutableClient.clearTestDatabase()
    }

    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        mutableClient.updateAxisMetaProperties(appId, cubeName, axisName, newMetaProperties)
    }
}
