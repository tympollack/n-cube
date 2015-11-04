package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * Class used to carry the NCube meta-information
 * to the client.
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
interface NCubePersister extends NCubeReadOnlyPersister
{
    void updateCube(ApplicationID appId, NCube cube, String username)
    boolean renameCube(ApplicationID appId, String oldName, String newName, String username)
    boolean duplicateCube(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    boolean deleteCube(ApplicationID appId, String cubeName, boolean allowDelete, String username)
    void restoreCube(ApplicationID appId, String cubeName, String username)

    // Used during commit, rollback, updating branch, merging
    NCubeInfoDto commitCube(ApplicationID appId, Long cubeId, String username)
    boolean rollbackCube(ApplicationID appId, String cubeName, String username)
    NCubeInfoDto updateCube(ApplicationID appId, Long cubeId, String username)
    boolean mergeAcceptTheirs(ApplicationID appId, String cubeName, String branchSha1, String username)
    boolean mergeAcceptMine(ApplicationID appId, String cubeName, String username)
    NCubeInfoDto commitMergedCubeToHead(ApplicationID appId, NCube cube, String username)
    NCubeInfoDto commitMergedCubeToBranch(ApplicationID appId, NCube cube, String headSha1, String username)
    boolean updateBranchCubeHeadSha1(Long cubeId, String headSha1)

    int changeVersionValue(ApplicationID appId, String newVersion)
    int releaseCubes(ApplicationID appId, String newSnapVer)

    boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    boolean updateTestData(ApplicationID appId, String cubeName, String testData)

    int createBranch(ApplicationID appId)
    boolean deleteBranch(ApplicationID appId)
    boolean deleteCubes(String appName)
    long fixSha1s()
}
