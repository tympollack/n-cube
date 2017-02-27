package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
interface NCubeEditorClient extends NCubeReleaseClient
{
    public static final String BRANCH_ADDS = 'adds'
    public static final String BRANCH_DELETES = 'deletes'
    public static final String BRANCH_UPDATES = 'updates'
    public static final String BRANCH_FASTFORWARDS = 'fastforwards'
    public static final String BRANCH_REJECTS = 'rejects'
    public static final String BRANCH_RESTORES = 'restores'

    void setUserId(String user)

    boolean updateCube(NCube ncube)

    NCube loadCubeById(long id)

    void createCube(NCube ncube)

    Boolean duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName)

    Boolean assertPermissions(ApplicationID appId, String resource, Action action)

    Boolean checkPermissions(ApplicationID appId, String resource, Action action)

    void setFakeId(String fake)

    String getImpliedId()

    Boolean isAdmin(ApplicationID appId, boolean useRealId)

    String getAppLockedBy(ApplicationID appId)

    Boolean lockApp(ApplicationID appId)

    void unlockApp(ApplicationID appId)

    Integer moveBranch(ApplicationID appId, String newSnapVer)

    Integer releaseVersion(ApplicationID appId, String newSnapVer)

    Integer releaseCubes(ApplicationID appId, String newSnapVer)

    Boolean restoreCubes(ApplicationID appId, Object[] cubeNames)

    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion)

    List<String> getAppNames(String tenant)

    Map<String, List<String>> getVersions(String tenant, String app)

    Integer copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory)

    Set<String> getBranches(ApplicationID appId)

    Integer getBranchCount(ApplicationID appId)

    Boolean deleteBranch(ApplicationID appId)

    NCube mergeDeltas(ApplicationID appId, String cubeName, List<Delta> deltas)

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames)

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames, boolean allowDelete)

    void changeVersionValue(ApplicationID appId, String newVersion)

    Boolean renameCube(ApplicationID appId, String oldName, String newName)

    void getReferencedCubeNames(ApplicationID appId, String cubeName, Set<String> references)

    List<AxisRef> getReferenceAxes(ApplicationID appId)

    void updateReferenceAxes(List<AxisRef> axisRefs)

    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)

    Boolean saveTests(ApplicationID appId, String cubeName, String tests)

    void clearTestDatabase()

    List<NCubeInfoDto> getHeadChangesForBranch(ApplicationID appId)

    List<NCubeInfoDto> getBranchChangesForHead(ApplicationID appId)

    List<NCubeInfoDto> getBranchChangesForMyBranch(ApplicationID appId, String branch)

    Map<String, Object> updateBranch(ApplicationID appId)

    Map<String, Object> updateBranch(ApplicationID appId, Object[] cubeDtos)

    Map<String, Object> commitBranch(ApplicationID appId)

    Map<String, Object> commitBranch(ApplicationID appId, Object[] inputCubes)

    int rollbackCubes(ApplicationID appId, Object[] names)

    int mergeAcceptMine(ApplicationID appId, Object[] cubeNames)

    int mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames)

    int mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch)
}