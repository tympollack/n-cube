package com.cedarsoftware.service.ncube

import com.cedarsoftware.ncube.*
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.springframework.stereotype.Service

import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_ACTIVE_RECORDS_ONLY
import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_EXACT_MATCH_NAME

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
class NCubeService
{
    private NCubeClient ncubeClient
    private static final Logger LOG = LogManager.getLogger(NCubeService.class)

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

    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion)
    {
        return mutableClient.getRevisionHistory(appId, cubeName, ignoreVersion)
    }

    List<String> getAppNames(String tenant)
    {
        return mutableClient.getAppNames(tenant)
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

    Map<String, Object> commitBranch(ApplicationID appId, Object[] infoDtos)
    {
        return mutableClient.commitBranch(appId, infoDtos)
    }

    Integer rollbackCubes(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.rollbackCubes(appId, cubeNames)
    }

    Integer mergeAcceptMine(ApplicationID appId, Object[] cubeNames)
    {
        return mutableClient.mergeAcceptMine(appId, cubeNames)
    }

    Integer mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch)
    {
        return mutableClient.mergeAcceptTheirs(appId, cubeNames, sourceBranch)
    }

    Map<String, Object> updateBranch(ApplicationID appId, Object[] cubeDtos)
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

    Integer acceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch)
    {
        return mutableClient.mergeAcceptTheirs(appId, cubeNames, sourceBranch)
    }

    Integer acceptMine(ApplicationID appId, Object[] cubeNames)
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

    Boolean duplicateCube(ApplicationID appId, ApplicationID destAppId, String cubeName, String newName)
    {
        return mutableClient.duplicate(appId, destAppId, cubeName, newName)
    }

    Integer releaseCubes(ApplicationID appId, String newSnapVer)
    {
        return mutableClient.releaseCubes(appId, newSnapVer)
    }

    Boolean changeVersionValue(ApplicationID appId, String newSnapVer)
    {
        return mutableClient.changeVersionValue(appId, newSnapVer)
    }

    void addAxis(ApplicationID appId, String cubeName, String axisName, String type, String valueType)
    {
        if (StringUtilities.isEmpty(axisName))
        {
            throw new IllegalArgumentException("Axis name cannot be empty.")
        }

        NCube ncube = mutableClient.getCube(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not add axis '" + axisName + "', NCube '" + cubeName + "' not found for app: " + appId)
        }

        long maxId = -1
        Iterator<Axis> i = ncube.axes.iterator()
        while (i.hasNext())
        {
            Axis axis = i.next()
            if (axis.id > maxId)
            {
                maxId = axis.id
            }
        }
        Axis axis = new Axis(axisName, AxisType.valueOf(type), AxisValueType.valueOf(valueType), true, Axis.DISPLAY, maxId + 1)
        ncube.addAxis(axis)
        mutableClient.updateCube(ncube)
    }

    void addAxis(ApplicationID appId, String cubeName, String axisName, ApplicationID refAppId, String refCubeName, String refAxisName, ApplicationID transformAppId, String transformCubeName, String transformMethodName)
    {
        NCube nCube = mutableClient.getCube(appId, cubeName)
        if (nCube == null)
        {
            throw new IllegalArgumentException("Could not add axis '" + axisName + "', NCube '" + cubeName + "' not found for app: " + appId)
        }

        if (StringUtilities.isEmpty(axisName))
        {
            axisName = refAxisName
        }

        long maxId = -1
        Iterator<Axis> i = nCube.axes.iterator()
        while (i.hasNext())
        {
            Axis axis = i.next()
            if (axis.id > maxId)
            {
                maxId = axis.id
            }
        }

        Map args = [:]
        args[ReferenceAxisLoader.REF_TENANT] = refAppId.tenant
        args[ReferenceAxisLoader.REF_APP] = refAppId.app
        args[ReferenceAxisLoader.REF_VERSION] = refAppId.version
        args[ReferenceAxisLoader.REF_STATUS] = refAppId.status
        args[ReferenceAxisLoader.REF_BRANCH] = refAppId.branch
        args[ReferenceAxisLoader.REF_CUBE_NAME] = refCubeName  // cube name of the holder of the referring (pointing) axis
        args[ReferenceAxisLoader.REF_AXIS_NAME] = refAxisName    // axis name of the referring axis (the variable that you had missing earlier)
        args[ReferenceAxisLoader.TRANSFORM_APP] = transformAppId?.app	// Notice no target tenant.  User MUST stay within TENENT boundary
        args[ReferenceAxisLoader.TRANSFORM_VERSION] = transformAppId?.version
        args[ReferenceAxisLoader.TRANSFORM_STATUS] = transformAppId?.status
        args[ReferenceAxisLoader.TRANSFORM_BRANCH] = transformAppId?.branch
        args[ReferenceAxisLoader.TRANSFORM_CUBE_NAME] = transformCubeName
        args[ReferenceAxisLoader.TRANSFORM_METHOD_NAME] = transformMethodName
        ReferenceAxisLoader refAxisLoader = new ReferenceAxisLoader(cubeName, axisName, args)

        Axis axis = new Axis(axisName, maxId + 1, true, refAxisLoader)
        nCube.addAxis(axis)
        mutableClient.updateCube(nCube)
    }

    /**
     * Delete the specified axis.
     */
    void deleteAxis(ApplicationID appId, String name, String axisName)
    {
        NCube ncube = mutableClient.getCube(appId, name)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not delete axis '" + axisName + "', NCube '" + name + "' not found for app: " + appId)
        }

        if (ncube.numDimensions == 1)
        {
            throw new IllegalArgumentException("Could not delete axis '" + axisName + "' - at least one axis must exist on n-cube.")
        }

        ncube.deleteAxis(axisName)
        mutableClient.updateCube(ncube)
    }

    /**
     * Update the 'informational' part of the Axis (not the columns).
     */
    void updateAxis(ApplicationID appId, String name, String origAxisName, String axisName, boolean hasDefault, boolean isSorted, boolean fireAll)
    {
        NCube ncube = mutableClient.getCube(appId, name)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not update axis '" + origAxisName + "', NCube '" + name + "' not found for app: " + appId)
        }

        // Rename axis
        if (!origAxisName.equalsIgnoreCase(axisName))
        {
            ncube.renameAxis(origAxisName, axisName)
        }

        // Update default column setting (if changed)
        Axis axis = ncube.getAxis(axisName)
        if (axis.hasDefaultColumn() && !hasDefault)
        {   // If it went from having default column to NOT having default column...
            ncube.deleteColumn(axisName, null)
        }
        else if (!axis.hasDefaultColumn() && hasDefault)
        {
            if (axis.type != AxisType.NEAREST)
            {
                ncube.addColumn(axisName, null)
            }
        }

        // update preferred column order
        if (axis.type == AxisType.RULE)
        {
            axis.fireAll = fireAll
        }
        else
        {
            axis.columnOrder = isSorted ? Axis.SORTED : Axis.DISPLAY
        }

        ncube.clearSha1()
        mutableClient.updateCube(ncube)
    }

    /**
     * Removes the reference from one axis to another.
     */
    void breakAxisReference(ApplicationID appId, String name, String axisName)
    {
        NCube ncube = mutableClient.getCube(appId, name)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not break reference for '" + axisName + "', NCube '" + name + "' not found for app: " + appId)
        }

        // Update default column setting (if changed)
        ncube.breakAxisReference(axisName)
        mutableClient.updateCube(ncube)
    }

    /**
     * In-place update of a cell.  'Value' is the final (converted) object type to be stored
     * in the indicated (by colIds) cell.
     */
    Boolean updateNCube(NCube ncube)
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
        json = json.trim()
        List cubes
        if (json.startsWith("["))
        {
            cubes = getCubes(json)
        }
        else
        {
            cubes = new ArrayList()
            cubes.add(NCube.fromSimpleJson(json))
        }

        for (Object object : cubes)
        {
            NCube ncube = (NCube) object
            ncube.applicationID = appId
            try
            {
                mutableClient.updateCube(ncube)
            }
            catch (Exception e)
            {
                try
                {
                    mutableClient.createCube(ncube)
                }
                catch (Exception ex)
                {
                    throw new IllegalArgumentException("Unable to update or create cube: ${ncube.name}")
                }
            }
        }
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

    boolean isAdmin(ApplicationID appId, boolean useRealId = false)
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

    ApplicationID getApplicationID(String tenant, String app, Map<String, Object> coord)
    {
        runtimeClient.getApplicationID(tenant, app, coord)
    }

    boolean assertPermissions(ApplicationID appId, String resource, Action action)
    {
        mutableClient.assertPermissions(appId, resource, action ?: Action.READ)
    }

    boolean checkPermissions(ApplicationID appId, String resource, Action action)
    {
        mutableClient.checkPermissions(appId, resource, action)
    }

    String getAppLockedBy(ApplicationID appId)
    {
        mutableClient.getAppLockedBy(appId)
    }

    void lockApp(ApplicationID appId)
    {
        mutableClient.lockApp(appId)
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

    boolean isCubeUpToDate(ApplicationID appId, String cubeName)
    {
        Map options = [:]
        options[(SEARCH_ACTIVE_RECORDS_ONLY)] = true
        options[(SEARCH_EXACT_MATCH_NAME)] = true

        List<NCubeInfoDto> list = mutableClient.search(appId, cubeName, null, options)
        if (list.size() != 1)
        {
            return false
        }

        NCubeInfoDto branchDto = list.first()     // only 1 because we used exact match
        list = mutableClient.search(appId.asHead(), cubeName, null, options)
        if (list.size() == 0)
        {   // New n-cube - up-todate because it does not yet exist in HEAD - the branch n-cube is the Creator.
            return true
        }
        else if (list.size() != 1)
        {   // Should never happen
            return false
        }

        NCubeInfoDto headDto = list.first()     // only 1 because we used exact match
        return StringUtilities.equalsIgnoreCase(branchDto.headSha1, headDto.sha1)
    }

    void clearTestDatabase()
    {
        mutableClient.clearTestDatabase()
    }

    // =========================================== Helper methods ======================================================

    static List getCubes(String json)
    {
        String lastSuccessful = ""
        try
        {
            Object[] cubes = (Object[]) JsonReader.jsonToJava(json)
            List cubeList = new ArrayList(cubes.length)

            for (Object cube : cubes)
            {
                JsonObject ncube = (JsonObject) cube
                if (ncube.containsKey("action"))
                {
                    cubeList.add(ncube)
                    lastSuccessful = (String) ncube.get("ncube")
                }
                else
                {
                    String json1 = JsonWriter.objectToJson(ncube)
                    NCube nCube = NCube.fromSimpleJson(json1)
                    cubeList.add(nCube)
                    lastSuccessful = nCube.name
                }
            }

            return cubeList
        }
        catch (Exception e)
        {
            String s = "Failed to load n-cubes from passed in JSON, last successful cube read: " + lastSuccessful
            throw new IllegalArgumentException(s, e)
        }
    }

    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        mutableClient.updateAxisMetaProperties(appId, cubeName, axisName, newMetaProperties)
    }
}
