package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.VersionComparator
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.SystemUtilities
import com.cedarsoftware.util.io.JsonReader
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.NCubeConstants.*

/**
 * This class manages a list of NCubes.  This class is referenced
 * by NCube in one place - when it joins to other cubes, it consults
 * the NCubeManager to find the joined NCube.
 * <p/>
 * This class takes care of creating, loading, updating, releasing,
 * and deleting NCubes.  It also allows you to get a list of NCubes
 * matching a wildcard (SQL Like) string.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either eƒfetxpress or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class NCubeManagerImpl implements NCubeEditorClient
{
    // Maintain cache of 'wildcard' patterns to Compiled Pattern instance
    private final ConcurrentMap<String, Pattern> wildcards = new ConcurrentHashMap<>()
    private NCubePersister nCubePersister
    private static final Logger LOG = LogManager.getLogger(NCubeManagerImpl.class)

    // not private in case we want to tweak things for testing.
    protected volatile ConcurrentMap<String, Object> systemParams = null
    
    private final ThreadLocal<String> userId = new ThreadLocal<String>() {
        String initialValue()
        {
            Map params = systemParams
            String userId = params.user instanceof String ? params.user : System.getProperty('user.name')
            return userId?.trim()
        }
    }

    // cache key = userId + '/' + appId + '/' + resource + '/' + Action
    // cache value = Long (negative = false, positive = true, abs(value) = millis since last access)
    private Cache<String, Boolean> permCache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).maximumSize(100000).concurrencyLevel(16).build()

    private static final List CUBE_MUTATE_ACTIONS = [Action.COMMIT, Action.UPDATE]

    NCubeManagerImpl(NCubePersister persister)
    {
        nCubePersister = persister
    }

    NCubePersister getPersister()
    {
        if (nCubePersister == null)
        {
            throw new IllegalStateException('Persister not set into NCubeManager.')
        }
        return nCubePersister
    }

    Map<String, Object> getSystemParams()
    {
        final ConcurrentMap<String, Object> params = systemParams

        if (params != null)
        {
            return params
        }

        synchronized (NCubeManagerImpl.class)
        {
            if (systemParams == null)
            {
                String jsonParams = SystemUtilities.getExternalVariable(NCUBE_PARAMS)
                ConcurrentMap sysParamMap = new ConcurrentHashMap<>()

                if (StringUtilities.hasContent(jsonParams))
                {
                    try
                    {
                        sysParamMap = new ConcurrentHashMap<>((Map) JsonReader.jsonToJava(jsonParams, [(JsonReader.USE_MAPS): true] as Map))
                    }
                    catch (Exception ignored)
                    {
                        LOG.warn("Parsing of NCUBE_PARAMS failed: ${jsonParams}")
                    }
                }
                systemParams = sysParamMap
            }
        }
        return systemParams
    }

    protected void clearSysParams()
    {
        systemParams = null;
    }

    NCube getCube(ApplicationID appId, String cubeName)
    {
        return loadCube(appId, cubeName)
    }

    /**
     * Load n-cube, bypassing any caching.  This is necessary for n-cube-editor (IDE time
     * usage).  If the IDE environment is clustered, cannot be getting stale copies from
     * cache.  Any advices in the manager will be applied to the n-cube.
     * @return NCube of the specified name from the specified AppID, or null if not found.
     */
    NCube loadCube(ApplicationID appId, String cubeName)
    {
        assertPermissions(appId, cubeName)
        return loadCubeInternal(appId, cubeName)
    }

    private NCube loadCubeInternal(ApplicationID appId, String cubeName)
    {
        NCube ncube = persister.loadCube(appId, cubeName)
        return ncube
    }

    /**
     * Load the n-cube with the specified id.  This is useful in n-cube editors, where a user wants to pick
     * an older revision and load / compare it.
     * @param id long n-cube id.
     * @return NCube that has the passed in id.
     */
    NCube loadCubeById(long id)
    {
        NCube ncube = persister.loadCubeById(id)
        return ncube
    }

    /**
     * This method will clear all caches for all ApplicationIDs.
     * Do not call it for anything other than test purposes.
     */
    void clearPermissionsCache()
    {
        permCache.invalidateAll()
    }


    /**
     * Retrieve all cube names that are deeply referenced by ApplicationID + n-cube name.
     */
    void getReferencedCubeNames(ApplicationID appId, String name, Set<String> refs)
    {
        if (refs == null)
        {
            throw new IllegalArgumentException("Could not get referenced cube names, null passed in for Set to hold referenced n-cube names, app: ${appId}, n-cube: ${name}")
        }
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(name)
        NCube ncube = loadCube(appId, name)
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not get referenced cube names, n-cube: ${name} does not exist in app: ${appId}")
        }

        Map<Map, Set<String>> subCubeRefs = ncube.referencedCubeNames

        // TODO: Use explicit stack, NOT recursion

        subCubeRefs.values().each { Set<String> cubeNames ->
            cubeNames.each { String cubeName ->
                if (!refs.contains(cubeName))
                {
                    refs.add(cubeName)
                    getReferencedCubeNames(appId, cubeName, refs)
                }
            }
        }
    }

    /**
     * Restore a previously deleted n-cube.
     */
    void restoreCubes(ApplicationID appId, Object[] cubeNames)
    {
        ApplicationID.validateAppId(appId)
        appId.validateBranchIsNotHead()

        if (appId.release)
        {
            throw new IllegalArgumentException("${ReleaseStatus.RELEASE.name()} cubes cannot be restored, app: ${appId}")
        }

        if (ArrayUtilities.isEmpty(cubeNames))
        {
            throw new IllegalArgumentException('Error, empty array of cube names passed in to be restored.')
        }

        assertNotLockBlocked(appId)
        for (String cubeName : cubeNames)
        {
            assertPermissions(appId, cubeName, Action.UPDATE)
        }

        // Batch restore
        persister.restoreCubes(appId, cubeNames, getUserId())
    }

    /**
     * Get a List<NCubeInfoDto> containing all history for the given cube.
     */
    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion = false)
    {
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(cubeName)
        assertPermissions(appId, cubeName)
        List<NCubeInfoDto> revisions = persister.getRevisions(appId, cubeName, ignoreVersion)
        return revisions
    }

    /**
     * Return a List of Strings containing all unique App names for the given tenant.
     */
    List<String> getAppNames(String tenant)
    {
        return persister.getAppNames(tenant)
    }

    /**
     * Get all of the versions that exist for the given ApplicationID (tenant and app).
     * @return List<String> version numbers.
     */
    Map<String, List<String>> getVersions(String tenant, String app)
    {
        ApplicationID.validateTenant(tenant)
        ApplicationID.validateApp(app)
        return persister.getVersions(tenant, app)
    }

    /**
     * Get the lastest version for the given tenant, app, and SNAPSHOT or RELEASE.
     * @return String version number in the form "major.minor.patch" where each of the
     * values (major, minor, patch) is numeric.
     */
    String getLatestVersion(String tenant, String app, String releaseStatus)
    {
        Map<String, List<String>> versionsMap = getVersions(tenant, app)
        Set<String> versions = new TreeSet<>(new VersionComparator())
        versions.addAll(versionsMap[releaseStatus])
        return versions.first() as String
    }

    /**
     * Duplicate the given n-cube specified by oldAppId and oldName to new ApplicationID and name,
     */
    void duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName)
    {
        ApplicationID.validateAppId(oldAppId)
        ApplicationID.validateAppId(newAppId)

        newAppId.validateBranchIsNotHead()

        if (newAppId.release)
        {
            throw new IllegalArgumentException("Cubes cannot be duplicated into a ${ReleaseStatus.RELEASE} version, cube: ${newName}, app: ${newAppId}")
        }

        NCube.validateCubeName(oldName)
        NCube.validateCubeName(newName)

        if (oldName.equalsIgnoreCase(newName) && oldAppId == newAppId)
        {
            throw new IllegalArgumentException("Could not duplicate, old name cannot be the same as the new name when oldAppId matches newAppId, name: ${oldName}, app: ${oldAppId}")
        }

        assertPermissions(oldAppId, oldName, Action.READ)
        if (oldAppId != newAppId)
        {   // Only see if branch permissions are needed to be created when destination cube is in a different ApplicationID
            detectNewAppId(newAppId)
        }
        assertPermissions(newAppId, newName, Action.UPDATE)
        assertNotLockBlocked(newAppId)
        persister.duplicateCube(oldAppId, newAppId, oldName, newName, getUserId())
    }

    /**
     * Update the passed in NCube.  Only SNAPSHOT cubes can be updated.
     *
     * @param ncube      NCube to be updated.
     * @return boolean true on success, false otherwise
     */
    boolean updateCube(ApplicationID appId, NCube ncube, boolean createPermCubesIfNeeded = false)
    {
        ApplicationID.validateAppId(appId)
        if (ncube == null)
        {
            throw new IllegalArgumentException('NCube cannot be null')
        }
        NCube.validateCubeName(ncube.name)

        if (appId.release)
        {
            throw new IllegalArgumentException("${ReleaseStatus.RELEASE.name()} cubes cannot be updated, cube: ${ncube.name}, app: ${appId}")
        }

        appId.validateBranchIsNotHead()

        final String cubeName = ncube.name
        if (createPermCubesIfNeeded)
        {
            detectNewAppId(appId)
        }
        assertPermissions(appId, cubeName, Action.UPDATE)
        assertNotLockBlocked(appId)
        persister.updateCube(appId, ncube, getUserId())
        ncube.applicationID = appId
        return true
    }

    /**
     * Copy branch from one app id to another
     * @param srcAppId Branch copied from (source branch)
     * @param targetAppId Branch copied to (must not exist)
     * @return int number of n-cubes in branch (number copied - revision depth is not copied)
     */
    int copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory = false)
    {
        assertPermissions(srcAppId, null, Action.READ)
        assertPermissions(targetAppId, null, Action.UPDATE)
        ApplicationID.validateAppId(srcAppId)
        ApplicationID.validateAppId(targetAppId)
        targetAppId.validateStatusIsNotRelease()
        if (!search(targetAppId.asRelease(), null, null, [(SEARCH_ACTIVE_RECORDS_ONLY): true]).empty)
        {
            throw new IllegalArgumentException("A RELEASE version ${targetAppId.version} already exists, app: ${targetAppId}")
        }
        assertNotLockBlocked(targetAppId)
        if (targetAppId.version != '0.0.0')
        {
            detectNewAppId(targetAppId)
        }
        int rows = copyWithHistory ? persister.copyBranchWithHistory(srcAppId, targetAppId) : persister.copyBranch(srcAppId, targetAppId)
        return rows
    }

    /**
     * Merge the passed in List of Delta's into the named n-cube.
     * @param appId ApplicationID containing the named n-cube.
     * @param cubeName String name of the n-cube into which the Delta's will be merged.
     * @param deltas List of Delta instances
     * @return the NCube t
     */
    NCube mergeDeltas(ApplicationID appId, String cubeName, List<Delta> deltas)
    {
        NCube ncube = loadCube(appId, cubeName)
        if (ncube == null)
        {
            throw new IllegalArgumentException("No ncube exists with the name: ${cubeName}, no changes will be merged, app: ${appId}")
        }

        assertPermissions(appId, cubeName, Action.UPDATE)
        deltas.each { Delta delta ->
            if ([Delta.Location.AXIS, Delta.Location.AXIS_META, Delta.Location.COLUMN, Delta.Location.COLUMN_META].contains(delta.location))
            {
                String axisName
                switch (delta.location)
                {
                    case Delta.Location.AXIS:
                        axisName = ((delta.sourceVal ?: delta.destVal) as Axis).name
                        break
                    case Delta.Location.AXIS_META:
                    case Delta.Location.COLUMN:
                        axisName = delta.locId as String
                        break
                    case Delta.Location.COLUMN_META:
                        axisName = (delta.locId as Map<String, Object>).axis
                        break
                    default:
                        throw new IllegalArgumentException("Invalid properties on delta, no changes will be merged, app: ${appId}, cube: ${cubeName}")
                }
                assertPermissions(appId, cubeName + '/' + axisName, Action.UPDATE)
            }
        }

        ncube.mergeDeltas(deltas)
        updateCube(appId, ncube)
        return ncube
    }

    /**
     * Move the branch specified in the appId to the newer snapshot version (newSnapVer).
     * @param ApplicationID indicating what to move
     * @param newSnapVer String version to move cubes to
     * @return number of rows moved (count includes revisions per cube).
     */
    int moveBranch(ApplicationID appId, String newSnapVer)
    {
        ApplicationID.validateAppId(appId)
        if (ApplicationID.HEAD == appId.branch)
        {
            throw new IllegalArgumentException('Cannot move the HEAD branch')
        }
        if ('0.0.0' == appId.version)
        {
            throw new IllegalStateException(ERROR_CANNOT_MOVE_000)
        }
        if ('0.0.0' == newSnapVer)
        {
            throw new IllegalStateException(ERROR_CANNOT_MOVE_TO_000)
        }
        assertLockedByMe(appId)
        assertPermissions(appId, null, Action.RELEASE)
        int rows = persister.moveBranch(appId, newSnapVer)
        return rows
    }

    /**
     * Perform release (SNAPSHOT to RELEASE) for the given ApplicationIDs n-cubes.
     */
    int releaseVersion(ApplicationID appId, String newSnapVer)
    {
        ApplicationID.validateAppId(appId)
        assertPermissions(appId, null, Action.RELEASE)
        assertLockedByMe(appId)
        ApplicationID.validateVersion(newSnapVer)
        if ('0.0.0' == appId.version)
        {
            throw new IllegalArgumentException(ERROR_CANNOT_RELEASE_000)
        }
        if ('0.0.0' == newSnapVer)
        {
            throw new IllegalArgumentException(ERROR_CANNOT_RELEASE_TO_000)
        }
        if (search(appId.asRelease(), null, null, [(SEARCH_ACTIVE_RECORDS_ONLY):true]).size() != 0)
        {
            throw new IllegalArgumentException("A RELEASE version ${appId.version} already exists, app: ${appId}")
        }

        int rows = persister.releaseCubes(appId, newSnapVer)
        return rows
    }

    /**
     * Perform release (SNAPSHOT to RELEASE) for the given ApplicationIDs n-cubes.
     */
    int releaseCubes(ApplicationID appId, String newSnapVer)
    {
        assertPermissions(appId, null, Action.RELEASE)
        ApplicationID.validateAppId(appId)
        ApplicationID.validateVersion(newSnapVer)
        if ('0.0.0' == appId.version)
        {
            throw new IllegalArgumentException(ERROR_CANNOT_RELEASE_000)
        }
        if ('0.0.0' == newSnapVer)
        {
            throw new IllegalArgumentException(ERROR_CANNOT_RELEASE_TO_000)
        }
        if (search(appId.asVersion(newSnapVer), null, null, [(SEARCH_ACTIVE_RECORDS_ONLY):true]).size() != 0)
        {
            throw new IllegalArgumentException("A SNAPSHOT version ${appId.version} already exists, app: ${appId}")
        }
        if (search(appId.asRelease(), null, null, [(SEARCH_ACTIVE_RECORDS_ONLY):true]).size() != 0)
        {
            throw new IllegalArgumentException("A RELEASE version ${appId.version} already exists, app: ${appId}")
        }

        lockApp(appId)
        if (!JUnitTest)
        {   // Only sleep when running in production (not by JUnit)
            sleep(10000)
        }

        Set<String> branches = getBranches(appId)
        for (String branch : branches)
        {
            if (!ApplicationID.HEAD.equalsIgnoreCase(branch))
            {
                ApplicationID branchAppId = appId.asBranch(branch)
                moveBranch(branchAppId, newSnapVer)
            }
        }
        int rows = persister.releaseCubes(appId, newSnapVer)
        persister.copyBranch(appId.asRelease(), appId.asSnapshot().asHead().asVersion(newSnapVer))
        unlockApp(appId)
        return rows
    }

    private boolean isJUnitTest()
    {
        StackTraceElement[] stackTrace = Thread.currentThread().stackTrace
        List<StackTraceElement> list = Arrays.asList(stackTrace)
        for (StackTraceElement element : list)
        {
            if (element.className.startsWith('org.junit.'))
            {
                return true
            }
        }
        return false
    }

    void changeVersionValue(ApplicationID appId, String newVersion)
    {
        ApplicationID.validateAppId(appId)

        if (appId.release)
        {
            throw new IllegalArgumentException("Cannot change the version of a ${ReleaseStatus.RELEASE.name()}, app: ${appId}")
        }
        ApplicationID.validateVersion(newVersion)
        assertPermissions(appId, null, Action.RELEASE)
        assertNotLockBlocked(appId)
        persister.changeVersionValue(appId, newVersion)
    }

    boolean renameCube(ApplicationID appId, String oldName, String newName)
    {
        ApplicationID.validateAppId(appId)
        appId.validateBranchIsNotHead()

        if (appId.release)
        {
            throw new IllegalArgumentException("Cannot rename a ${ReleaseStatus.RELEASE.name()} cube, cube: ${oldName}, app: ${appId}")
        }

        assertNotLockBlocked(appId)

        NCube.validateCubeName(oldName)
        NCube.validateCubeName(newName)

        if (oldName == newName)
        {
            throw new IllegalArgumentException("Could not rename, old name cannot be the same as the new name, name: ${oldName}, app: ${appId}")
        }

        assertPermissions(appId, oldName, Action.UPDATE)
        assertPermissions(appId, newName, Action.UPDATE)

        boolean result = persister.renameCube(appId, oldName, newName, getUserId())
        return result
    }

    boolean deleteBranch(ApplicationID appId)
    {
        appId.validateBranchIsNotHead()
        assertPermissions(appId, null, Action.UPDATE)
        assertNotLockBlocked(appId)
        return persister.deleteBranch(appId)
    }

    /**
     * Delete the named NCube from the database
     *
     * @param cubeNames  Object[] of String cube names to be deleted (soft deleted)
     */
    boolean deleteCubes(ApplicationID appId, Object[] cubeNames)
    {
        appId.validateBranchIsNotHead()
        assertNotLockBlocked(appId)
        for (Object name : cubeNames)
        {
            assertPermissions(appId, name as String, Action.UPDATE)
        }
        return deleteCubes(appId, cubeNames, false)
    }

    boolean deleteCubes(ApplicationID appId, Object[] cubeNames, boolean allowDelete)
    {
        ApplicationID.validateAppId(appId)
        if (!allowDelete)
        {
            if (appId.release)
            {
                throw new IllegalArgumentException("${ReleaseStatus.RELEASE.name()} cubes cannot be hard-deleted, app: ${appId}")
            }
        }

        assertNotLockBlocked(appId)
        for (Object name : cubeNames)
        {
            assertPermissions(appId, name as String, Action.UPDATE)
        }

        if (persister.deleteCubes(appId, cubeNames, allowDelete, getUserId()))
        {
            return true
        }
        return false
    }

    boolean updateTestData(ApplicationID appId, String cubeName, String testData)
    {
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(cubeName)
        assertPermissions(appId, cubeName, Action.UPDATE)
        assertNotLockBlocked(appId)
        return persister.updateTestData(appId, cubeName, testData)
    }

    String getTestData(ApplicationID appId, String cubeName)
    {
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(cubeName)
        assertPermissions(appId, cubeName)
        return persister.getTestData(appId, cubeName)
    }

    boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(cubeName)
        assertPermissions(appId, cubeName, Action.UPDATE)
        assertNotLockBlocked(appId)
        return persister.updateNotes(appId, cubeName, notes)
    }

    String getNotes(ApplicationID appId, String cubeName)
    {
        ApplicationID.validateAppId(appId)
        NCube.validateCubeName(cubeName)
        assertPermissions(appId, cubeName)

        Map<String, Object> options = [:]
        options[SEARCH_INCLUDE_NOTES] = true
        options[SEARCH_EXACT_MATCH_NAME] = true
        List<NCubeInfoDto> infos = search(appId, cubeName, null, options)

        if (infos.empty)
        {
            throw new IllegalArgumentException("Could not fetch notes, no cube: ${cubeName} in app: ${appId}")
        }
        return infos[0].notes
    }

    Set<String> getBranches(ApplicationID appId)
    {
        appId.validate()
        assertPermissions(appId, null)
        return persister.getBranches(appId)
    }

    int getBranchCount(ApplicationID appId)
    {
        Set<String> branches = getBranches(appId)
        return branches.size()
    }

    ApplicationID getApplicationID(String tenant, String app, Map<String, Object> coord)
    {
        ApplicationID.validateTenant(tenant)
        ApplicationID.validateApp(tenant)

        if (coord == null)
        {
            coord = [:]
        }

        NCube bootCube = loadCube(ApplicationID.getBootVersion(tenant, app), SYS_BOOTSTRAP)

        if (bootCube == null)
        {
            throw new IllegalStateException("Missing ${SYS_BOOTSTRAP} cube in the 0.0.0 version for the app: ${app}")
        }

        Map copy = new HashMap(coord)
        ApplicationID bootAppId = (ApplicationID) bootCube.getCell(copy, [:])
        String version = bootAppId.version
        String status = bootAppId.status
        String branch = bootAppId.branch

        if (!tenant.equalsIgnoreCase(bootAppId.tenant))
        {
            LOG.warn("sys.bootstrap cube for tenant: ${tenant}, app: ${app} is returning a different tenant: ${bootAppId.tenant} than requested. Using ${tenant} instead.")
        }

        if (!app.equalsIgnoreCase(bootAppId.app))
        {
            LOG.warn("sys.bootstrap cube for tenant: ${tenant}, app: ${app} is returning a different app: ${bootAppId.app} than requested. Using ${app} instead.")
        }

        return new ApplicationID(tenant, app, version, status, branch)
    }

    /**
     *
     * Fetch an array of NCubeInfoDto's where the cube names match the cubeNamePattern (contains) and
     * the content (in JSON format) 'contains' the passed in content String.
     * @param appId ApplicationID on which we are working
     * @param cubeNamePattern cubeNamePattern String pattern to match cube names
     * @param content String value that is 'contained' within the cube's JSON
     * @param options map with possible keys:
     *                changedRecordsOnly - default false ->  Only searches changed records if true.
     *                activeRecordsOnly - default false -> Only searches non-deleted records if true.
     *                deletedRecordsOnly - default false -> Only searches deleted records if true.
     *                cacheResult - default false -> Cache the cubes that match this result..
     * @return List<NCubeInfoDto>
     */
    List<NCubeInfoDto> search(ApplicationID appId, String cubeNamePattern, String content, Map options)
    {
        ApplicationID.validateAppId(appId)

        if (options == null)
        {
            options = [:]
        }

        if (!options[SEARCH_EXACT_MATCH_NAME])
        {
            cubeNamePattern = handleWildCard(cubeNamePattern)
        }

        content = handleWildCard(content)

        Map permInfo = getPermInfo(appId)
        List<NCubeInfoDto> cubes = persister.search(appId, cubeNamePattern, content, options)
        if (!permInfo.skipPermCheck)
        {
            cubes.removeAll { !fastCheckPermissions(appId, it.name, Action.READ, permInfo) }
        }
        return cubes
    }

    /**
     * This API will hand back a List of AxisRef, which is a complete description of a Reference
     * Axis pointer. It includes the Source ApplicationID, source Cube Name, source Axis Name,
     * and all the referenced cube/axis and filter (cube/method) parameters.
     * @param appId ApplicationID of the cube-set from which to fetch all the reference axes.
     * @return List<AxisRef>
     */
    List<AxisRef> getReferenceAxes(ApplicationID appId)
    {
        ApplicationID.validateAppId(appId)
        assertPermissions(appId, null)

        // Step 1: Fetch all NCubeInfoDto's for the passed in ApplicationID
        List<NCubeInfoDto> list = persister.search(appId, null, null, [(SEARCH_ACTIVE_RECORDS_ONLY):true])
        List<AxisRef> refAxes = []

        for (NCubeInfoDto dto : list)
        {
            try
            {
                NCube source = persister.loadCubeById(dto.id as long)
                for (Axis axis : source.axes)
                {
                    if (axis.reference)
                    {
                        AxisRef ref = new AxisRef()
                        ref.srcAppId = appId
                        ref.srcCubeName = source.name
                        ref.srcAxisName = axis.name

                        ApplicationID refAppId = axis.referencedApp
                        ref.destApp = refAppId.app
                        ref.destVersion = refAppId.version
                        ref.destCubeName = axis.getMetaProperty(ReferenceAxisLoader.REF_CUBE_NAME)
                        ref.destAxisName = axis.getMetaProperty(ReferenceAxisLoader.REF_AXIS_NAME)

                        ApplicationID transformAppId = axis.transformApp
                        if (transformAppId)
                        {
                            ref.transformApp = transformAppId.app
                            ref.transformVersion = transformAppId.version
                            ref.transformCubeName = axis.getMetaProperty(ReferenceAxisLoader.TRANSFORM_CUBE_NAME)
                            ref.transformMethodName = axis.getMetaProperty(ReferenceAxisLoader.TRANSFORM_METHOD_NAME)
                        }

                        refAxes.add(ref)
                    }
                }
            }
            catch (Exception e)
            {
                LOG.warn("Unable to load cube: ${dto.name}, app: ${dto.applicationID}", e)
            }
        }
        return refAxes
    }

    void updateReferenceAxes(List<AxisRef> axisRefs)
    {
        Set<ApplicationID> uniqueAppIds = new HashSet()
        for (AxisRef axisRef : axisRefs)
        {
            ApplicationID srcApp = axisRef.srcAppId
            ApplicationID.validateAppId(srcApp)
            srcApp.validateBranchIsNotHead()
            assertPermissions(srcApp, axisRef.srcCubeName, Action.UPDATE)
            uniqueAppIds.add(srcApp)
            ApplicationID destAppId = new ApplicationID(srcApp.tenant, axisRef.destApp, axisRef.destVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)
            ApplicationID.validateAppId(destAppId)
            assertPermissions(destAppId, axisRef.destCubeName)

            if (axisRef.transformApp != null && axisRef.transformVersion != null)
            {
                ApplicationID transformAppId = new ApplicationID(srcApp.tenant, axisRef.transformApp, axisRef.transformVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)
                ApplicationID.validateAppId(transformAppId)
                assertPermissions(transformAppId, axisRef.transformCubeName, Action.READ)
            }
        }

        // Make sure we are not lock blocked on any of the appId's that are being updated.
        for (ApplicationID appId : uniqueAppIds)
        {
            assertNotLockBlocked(appId)
        }

        for (AxisRef axisRef : axisRefs)
        {
            axisRef.with {
                NCube ncube = persister.loadCube(srcAppId, srcCubeName)
                Axis axis = ncube.getAxis(srcAxisName)

                if (axis.reference)
                {
                    axis.setMetaProperty(ReferenceAxisLoader.REF_APP, destApp)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_VERSION, destVersion)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_CUBE_NAME, destCubeName)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_AXIS_NAME, destAxisName)
                    ApplicationID appId = new ApplicationID(srcAppId.tenant, destApp, destVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)

                    NCube target = persister.loadCube(appId, destCubeName)
                    if (target == null)
                    {
                        throw new IllegalArgumentException("""\
Cannot point reference axis to non-existing cube: ${destCubeName}. \
Source axis: ${srcAppId.cacheKey(srcCubeName)}.${srcAxisName}, \
target axis: ${destApp} / ${destVersion} / ${destCubeName}.${destAxisName}""")
                    }

                    if (target.getAxis(destAxisName) == null)
                    {
                        throw new IllegalArgumentException("""\
Cannot point reference axis to non-existing axis: ${destAxisName}. \
Source axis: ${srcAppId.cacheKey(srcCubeName)}.${srcAxisName}, \
target axis: ${destApp} / ${destVersion} / ${destCubeName}.${destAxisName}""")
                    }

                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_APP, transformApp)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_VERSION, transformVersion)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_CUBE_NAME, transformCubeName)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_METHOD_NAME, transformMethodName)

                    if (transformApp && transformVersion && transformCubeName && transformMethodName)
                    {   // If transformer cube reference supplied, verify that the cube exists
                        ApplicationID txAppId = new ApplicationID(srcAppId.tenant, transformApp, transformVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)
                        NCube transformCube = persister.loadCube(txAppId, transformCubeName)
                        if (transformCube == null)
                        {
                            throw new IllegalArgumentException("""\
Cannot point reference axis transformer to non-existing cube: ${transformCubeName}. \
Source axis: ${srcAppId.cacheKey(srcCubeName)}.${srcAxisName}, \
target axis: ${transformApp} / ${transformVersion} / ${transformCubeName}.${transformMethodName}""")
                        }

                        if (transformCube.getAxis('method') == null)
                        {
                            throw new IllegalArgumentException("""\
Cannot point reference axis transformer to non-existing axis: ${transformMethodName}. \
Source axis: ${srcAppId.cacheKey(srcCubeName)}.${srcAxisName}, \
target axis: ${transformApp} / ${transformVersion} / ${transformCubeName}.${transformMethodName}""")
                        }
                    }
                    else
                    {
                        axis.removeTransform()
                    }

                    ncube.clearSha1()   // changing meta properties does not clear SHA-1 for recalculation.
                    persister.updateCube(axisRef.srcAppId, ncube, getUserId())
                }
            }
        }
    }

    /**
     * Update an Axis meta-properties
     */
    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        NCube.transformMetaProperties(newMetaProperties)
        String resourceName = cubeName + '/' + axisName
        assertPermissions(appId, resourceName, Action.UPDATE)
        NCube ncube = loadCube(appId, cubeName)
        Axis axis = ncube.getAxis(axisName)
        axis.updateMetaProperties(newMetaProperties, cubeName, { Set<Long> colIds ->
            ncube.dropOrphans(colIds, axis.id)
        })
        ncube.clearSha1()
        updateCube(appId, ncube)
    }

    // ---------------------- Broadcast APIs for notifying other services in cluster of cache changes ------------------
    protected void broadcast(ApplicationID appId)
    {
        // Write to 'system' tenant, 'NCE' app, version '0.0.0', SNAPSHOT, cube: sys.cache
        // Separate thread reads from this table every 1 second, for new commands, for
        // example, clear cache
        appId.toString()
    }

    // --------------------------------------- Permissions -------------------------------------------------------------

    /**
     * Assert that the requested permission is allowed.  Throw a SecurityException if not.
     */
    boolean assertPermissions(ApplicationID appId, String resource, Action action = Action.READ)
    {
        if (checkPermissions(appId, resource, action))
        {
            return true
        }
        throw new SecurityException("Operation not performed.  You do not have ${action.name()} permission to ${resource}, app: ${appId}")
    }

    protected boolean assertNotLockBlocked(ApplicationID appId)
    {
        String lockedBy = getAppLockedBy(appId)
        if (lockedBy == null || lockedBy == getUserId())
        {
            return true
        }
        throw new SecurityException("Application is not locked by you, app: ${appId}")
    }

    private void assertLockedByMe(ApplicationID appId)
    {
        final ApplicationID bootAppId = getBootAppId(appId)
        final NCube sysLockCube = loadCubeInternal(bootAppId, SYS_LOCK)
        if (sysLockCube == null)
        {   // If there is no sys.lock cube, then no permissions / locking being used.
            if (JUnitTest)
            {
                return
            }
            throw new SecurityException("Application is not locked by you, no sys.lock n-cube exists in app: ${appId}")
        }

        final String lockOwner = getAppLockedBy(bootAppId)
        if (getUserId() == lockOwner)
        {
            return
        }
        throw new SecurityException("Application is not locked by you, app: ${appId}")
    }

    private ApplicationID getBootAppId(ApplicationID appId)
    {
        return new ApplicationID(appId.tenant, appId.app, '0.0.0', ReleaseStatus.SNAPSHOT.name(), ApplicationID.HEAD)
    }

    private String getPermissionCacheKey(ApplicationID appId, String resource, Action action)
    {
        final String sep = '/'
        final StringBuilder builder = new StringBuilder()
        builder.append(userId.get())
        builder.append(sep)
        builder.append(appId.tenant)
        builder.append(sep)
        builder.append(appId.app)
        builder.append(sep)
        builder.append(appId.version)
        builder.append(sep)
        builder.append(appId.branch)
        builder.append(sep)
        builder.append(resource)
        builder.append(sep)
        builder.append(action)
        return builder.toString()
    }

    private Boolean checkPermissionCache(String key)
    {
        return permCache.getIfPresent(key)
    }

    /**
     * Verify whether the action can be performed against the resource (typically cube name).
     * @param appId ApplicationID containing the n-cube being checked.
     * @param resource String cubeName or cubeName with wildcards('*' or '?') or cubeName / axisName (with wildcards).
     * @param action Action To be attempted.
     * @return boolean true if allowed, false if not.  If the permissions cubes restricting access have not yet been
     * added to the same App, then all access is granted.
     */
    boolean checkPermissions(ApplicationID appId, String resource, Action action)
    {
        String key = getPermissionCacheKey(appId, resource, action)
        Boolean allowed = checkPermissionCache(key)
        if (allowed instanceof Boolean)
        {
            return allowed
        }

        if (Action.READ == action && SYS_LOCK.equalsIgnoreCase(resource))
        {
            permCache.put(key, true)
            return true
        }

        ApplicationID bootVersion = getBootAppId(appId)
        NCube permCube = loadCubeInternal(bootVersion, SYS_PERMISSIONS)
        if (permCube == null)
        {   // Allow everything if no permissions are set up.
            permCache.put(key, true)
            return true
        }

        NCube userToRole = loadCubeInternal(bootVersion, SYS_USERGROUPS)
        if (userToRole == null)
        {   // Allow everything if no user roles are set up.
            permCache.put(key, true)
            return true
        }

        // Step 1: Get user's roles
        Set<String> roles = getRolesForUser(userToRole)

        if (!roles.contains(ROLE_ADMIN) && CUBE_MUTATE_ACTIONS.contains(action))
        {   // If user is not an admin, check branch permissions.
            NCube branchPermCube = loadCubeInternal(bootVersion.asBranch(appId.branch), SYS_BRANCH_PERMISSIONS)
            if (branchPermCube != null && !checkBranchPermission(branchPermCube, resource))
            {
                permCache.put(key, false)
                return false
            }
        }

        // Step 2: Make sure one of the user's roles allows access
        final String actionName = action.lower()
        for (String role : roles)
        {
            if (checkResourcePermission(permCube, role, resource, actionName))
            {
                permCache.put(key, true)
                return true
            }
        }

        permCache.put(key, false)
        return false
    }

    /**
     * Faster permissions check that should be used when filtering a list of n-cubes.  Before calling this
     * API, call getPermInfo(AppId) to get the 'permInfo' Map to be used in this API.
     */
    boolean fastCheckPermissions(ApplicationID appId, String resource, Action action, Map permInfo)
    {
        String key = getPermissionCacheKey(appId, resource, action)
        Boolean allowed = checkPermissionCache(key)
        if (allowed instanceof Boolean)
        {
            return allowed
        }

        if (Action.READ == action && SYS_LOCK.equalsIgnoreCase(resource))
        {
            permCache.put(key, true)
            return true
        }

        Set<String> roles = permInfo.roles as Set
        if (!roles.contains(ROLE_ADMIN) && CUBE_MUTATE_ACTIONS.contains(action))
        {   // If user is not an admin, check branch permissions.
            NCube branchPermCube = (NCube)permInfo.branchPermCube
            if (branchPermCube != null && !checkBranchPermission(branchPermCube, resource))
            {
                permCache.put(key, false)
                return false
            }
        }

        // Step 2: Make sure one of the user's roles allows access
        final String actionName = action.lower()
        NCube permCube = permInfo.permCube as NCube
        for (String role : roles)
        {
            if (checkResourcePermission(permCube, role, resource, actionName))
            {
                permCache.put(key, true)
                return true
            }
        }

        permCache.put(key, false)
        return false
    }

    private Map getPermInfo(ApplicationID appId)
    {
        Map<String, Object> info = [skipPermCheck:false] as Map
        ApplicationID bootVersion = getBootAppId(appId)
        info.bootVersion = bootVersion
        NCube permCube = loadCubeInternal(bootVersion, SYS_PERMISSIONS)
        if (permCube == null)
        {   // Allow everything if no permissions are set up.
            info.skipPermCheck = true
        }
        info.permCube = permCube

        NCube userToRole = loadCubeInternal(bootVersion, SYS_USERGROUPS)
        if (userToRole == null)
        {   // Allow everything if no user roles are set up.
            info.skipPermCheck = true
        }
        else
        {
            info.roles = getRolesForUser(userToRole)
        }

        info.branch000 = bootVersion.asBranch(appId.branch)
        info.branchPermCube = loadCubeInternal((ApplicationID)info.branch000, SYS_BRANCH_PERMISSIONS)
        return info
    }

    private boolean checkBranchPermission(NCube branchPermissions, String resource)
    {
        final List<Column> resourceColumns = getResourcesToMatch(branchPermissions, resource)
        final String userId = getUserId()
        final Column column = resourceColumns.find { branchPermissions.getCell([resource: it.value, user: userId])}
        return column != null
    }

    private boolean checkResourcePermission(NCube resourcePermissions, String role, String resource, String action)
    {
        final List<Column> resourceColumns = getResourcesToMatch(resourcePermissions, resource)
        final Column column = resourceColumns.find {resourcePermissions.getCell([(AXIS_ROLE): role, resource: it.value, action: action]) }
        return column != null
    }

    private Set<String> getRolesForUser(NCube userGroups)
    {
        Axis role = userGroups.getAxis(AXIS_ROLE)
        Set<String> groups = new HashSet()
        for (Column column : role.columns)
        {
            if (userGroups.getCell([(AXIS_ROLE): column.value, (AXIS_USER): getUserId()]))
            {
                groups.add(column.value as String)
            }
        }
        return groups
    }

    private List<Column> getResourcesToMatch(NCube permCube, String resource)
    {
        List<Column> matches = []
        Axis resourcePermissionAxis = permCube.getAxis(AXIS_RESOURCE)
        if (resource != null)
        {
            String[] splitResource = resource.split('/')
            boolean shouldCheckAxis = splitResource.length > 1
            String resourceCube = splitResource[0]
            String resourceAxis = shouldCheckAxis ? splitResource[1] : null

            for (Column resourcePermissionColumn : resourcePermissionAxis.columnsWithoutDefault)
            {
                String columnResource = resourcePermissionColumn.value
                String[] curSplitResource = columnResource.split('/')
                boolean resourceIncludesAxis = curSplitResource.length > 1
                String curResourceCube = curSplitResource[0]
                String curResourceAxis = resourceIncludesAxis ? curSplitResource[1] : null
                boolean resourceMatchesCurrentResource = doStringsWithWildCardsMatch(resourceCube, curResourceCube)

                if ((shouldCheckAxis && resourceMatchesCurrentResource && doStringsWithWildCardsMatch(resourceAxis, curResourceAxis))
                        || (!shouldCheckAxis && !resourceIncludesAxis && resourceMatchesCurrentResource))
                {
                    matches << resourcePermissionColumn
                }
            }
        }
        if (matches.size() == 0)
        {
            matches.add(resourcePermissionAxis.defaultColumn)
        }
        return matches
    }

    private boolean doStringsWithWildCardsMatch(String text, String pattern)
    {
        if (pattern == null)
        {
            return false
        }

        Pattern p = wildcards[pattern]
        if (p != null)
        {
            return p.matcher(text).matches()
        }

        String regexString = '(?i)' + StringUtilities.wildcardToRegexString(pattern)
        p = Pattern.compile(regexString)
        wildcards[pattern] = p
        return p.matcher(text).matches()
    }

    boolean isAdmin(ApplicationID appId)
    {
        NCube userCube = loadCubeInternal(getBootAppId(appId), SYS_USERGROUPS)
        if (userCube == null)
        {   // Allow everything if no permissions are set up.
            return true
        }
        return isUserInGroup(userCube, ROLE_ADMIN)
    }

    private boolean isUserInGroup(NCube userCube, String groupName)
    {
        return userCube.getCell([(AXIS_ROLE): groupName, (AXIS_USER): null]) || userCube.getCell([(AXIS_ROLE): groupName, (AXIS_USER): getUserId()])
    }

    protected void detectNewAppId(ApplicationID appId)
    {
        if (persister.doCubesExist(appId, true, 'detectNewAppId'))
        {
            addAppPermissionsCubes(appId)
            if (!appId.head)
            {
                addBranchPermissionsCube(appId)
            }
        }
    }

    private void addBranchPermissionsCube(ApplicationID appId)
    {
        ApplicationID permAppId = appId.asVersion('0.0.0')
        if (loadCubeInternal(permAppId, SYS_BRANCH_PERMISSIONS) != null)
        {
            return
        }

        String userId = getUserId()
        NCube branchPermCube = new NCube(SYS_BRANCH_PERMISSIONS)
        branchPermCube.applicationID = permAppId
        branchPermCube.defaultCellValue = false

        Axis resourceAxis = new Axis(AXIS_RESOURCE, AxisType.DISCRETE, AxisValueType.STRING, true)
        resourceAxis.addColumn(SYS_BRANCH_PERMISSIONS)
        branchPermCube.addAxis(resourceAxis)

        Axis userAxis = new Axis(AXIS_USER, AxisType.DISCRETE, AxisValueType.STRING, true)
        userAxis.addColumn(userId)
        branchPermCube.addAxis(userAxis)

        branchPermCube.setCell(true, [(AXIS_USER):userId, (AXIS_RESOURCE):SYS_BRANCH_PERMISSIONS])
        branchPermCube.setCell(true, [(AXIS_USER):userId, (AXIS_RESOURCE):null])

        persister.updateCube(permAppId, branchPermCube, userId)
        VersionControl.updateBranch(permAppId)
    }

    private void addAppPermissionsCubes(ApplicationID appId)
    {
        ApplicationID permAppId = getBootAppId(appId)
        addAppUserGroupsCube(permAppId)
        addAppPermissionsCube(permAppId)
        addSysLockingCube(permAppId)
    }

    private void addSysLockingCube(ApplicationID appId)
    {
        if (loadCubeInternal(appId, SYS_LOCK) != null)
        {
            return
        }

        NCube sysLockCube = new NCube(SYS_LOCK)
        sysLockCube.applicationID = appId
        sysLockCube.setMetaProperty(PROPERTY_CACHE, false)
        sysLockCube.addAxis(new Axis(AXIS_SYSTEM, AxisType.DISCRETE, AxisValueType.STRING, true))
        persister.updateCube(appId, sysLockCube, getUserId())
    }

    /**
     * Determine if the ApplicationID is locked.  This is an expensive call because it
     * always hits the database.  Use judiciously (obtain value before loops, etc.)
     */
    String getAppLockedBy(ApplicationID appId)
    {
        NCube sysLockCube = loadCubeInternal(getBootAppId(appId), SYS_LOCK)
        if (sysLockCube == null)
        {
            return null
        }
        return sysLockCube.getCell([(AXIS_SYSTEM):null])
    }

    /**
     * Lock the given appId so that no changes can be made to any cubes within it
     * @param appId ApplicationID to lock
     */
    boolean lockApp(ApplicationID appId)
    {
        assertPermissions(appId, null, Action.RELEASE)
        String userId = getUserId()
        ApplicationID bootAppId = getBootAppId(appId)

        String lockOwner = getAppLockedBy(appId)
        if (userId == lockOwner)
        {
            return false
        }
        if (lockOwner != null)
        {
            throw new SecurityException("Application ${appId} already locked by ${lockOwner}")
        }

        NCube sysLockCube = loadCubeInternal(bootAppId, SYS_LOCK)
        if (sysLockCube == null)
        {
            return false
        }
        sysLockCube.setCell(userId, [(AXIS_SYSTEM):null])
        persister.updateCube(bootAppId, sysLockCube, userId)
        return true
    }

    /**
     * Unlock the given appId so that changes can be made to any cubes within it
     * @param appId ApplicationID to unlock
     */
    void unlockApp(ApplicationID appId)
    {
        assertPermissions(appId, null, Action.RELEASE)
        ApplicationID bootAppId = getBootAppId(appId)
        NCube sysLockCube = loadCubeInternal(bootAppId, SYS_LOCK)
        if (sysLockCube == null)
        {
            return
        }

        String userId = getUserId()
        String lockOwner = getAppLockedBy(appId)
        if (userId != lockOwner && !isAdmin(appId))
        {
            throw new SecurityException("Application ${appId} locked by ${lockOwner}")
        }

        sysLockCube.removeCell([(AXIS_SYSTEM):null])
        persister.updateCube(bootAppId, sysLockCube, getUserId())
    }

    private void addAppUserGroupsCube(ApplicationID appId)
    {
        if (loadCubeInternal(appId, SYS_USERGROUPS) != null)
        {
            return
        }

        String userId = getUserId()
        NCube userGroupsCube = new NCube(SYS_USERGROUPS)
        userGroupsCube.applicationID = appId
        userGroupsCube.defaultCellValue = false

        Axis userAxis = new Axis(AXIS_USER, AxisType.DISCRETE, AxisValueType.STRING, true)
        userAxis.addColumn(userId)
        userGroupsCube.addAxis(userAxis)

        Axis roleAxis = new Axis(AXIS_ROLE, AxisType.DISCRETE, AxisValueType.STRING, false)
        roleAxis.addColumn(ROLE_ADMIN)
        roleAxis.addColumn(ROLE_READONLY)
        roleAxis.addColumn(ROLE_USER)
        userGroupsCube.addAxis(roleAxis)

        userGroupsCube.setCell(true, [(AXIS_USER):userId, (AXIS_ROLE):ROLE_ADMIN])
        userGroupsCube.setCell(true, [(AXIS_USER):userId, (AXIS_ROLE):ROLE_USER])
        userGroupsCube.setCell(true, [(AXIS_USER):null, (AXIS_ROLE):ROLE_USER])

        persister.updateCube(appId, userGroupsCube, userId)
    }

    private void addAppPermissionsCube(ApplicationID appId)
    {
        if (loadCubeInternal(appId, SYS_PERMISSIONS))
        {
            return
        }

        NCube appPermCube = new NCube(SYS_PERMISSIONS)
        appPermCube.applicationID = appId
        appPermCube.defaultCellValue = false

        Axis resourceAxis = new Axis(AXIS_RESOURCE, AxisType.DISCRETE, AxisValueType.STRING, true)
        resourceAxis.addColumn(SYS_PERMISSIONS)
        resourceAxis.addColumn(SYS_USERGROUPS)
        resourceAxis.addColumn(SYS_BRANCH_PERMISSIONS)
        resourceAxis.addColumn(SYS_LOCK)
        appPermCube.addAxis(resourceAxis)

        Axis roleAxis = new Axis(AXIS_ROLE, AxisType.DISCRETE, AxisValueType.STRING, false)
        roleAxis.addColumn(ROLE_ADMIN)
        roleAxis.addColumn(ROLE_READONLY)
        roleAxis.addColumn(ROLE_USER)
        appPermCube.addAxis(roleAxis)

        Axis actionAxis = new Axis(AXIS_ACTION, AxisType.DISCRETE, AxisValueType.STRING, false)
        actionAxis.addColumn(Action.UPDATE.lower(), null, null, [(Column.DEFAULT_VALUE):true as Object])
        actionAxis.addColumn(Action.READ.lower(), null, null, [(Column.DEFAULT_VALUE):true as Object])
        actionAxis.addColumn(Action.RELEASE.lower())
        actionAxis.addColumn(Action.COMMIT.lower())
        appPermCube.addAxis(actionAxis)

        appPermCube.setCell(false, [(AXIS_RESOURCE):SYS_BRANCH_PERMISSIONS, (AXIS_ROLE):ROLE_READONLY, (AXIS_ACTION):Action.UPDATE.lower()])

        appPermCube.setCell(false, [(AXIS_RESOURCE):SYS_PERMISSIONS, (AXIS_ROLE):ROLE_READONLY, (AXIS_ACTION):Action.UPDATE.lower()])
        appPermCube.setCell(true, [(AXIS_RESOURCE):SYS_PERMISSIONS, (AXIS_ROLE):ROLE_ADMIN, (AXIS_ACTION):Action.COMMIT.lower()])

        appPermCube.setCell(false, [(AXIS_RESOURCE):SYS_USERGROUPS, (AXIS_ROLE):ROLE_READONLY, (AXIS_ACTION):Action.UPDATE.lower()])
        appPermCube.setCell(true, [(AXIS_RESOURCE):SYS_USERGROUPS, (AXIS_ROLE):ROLE_ADMIN, (AXIS_ACTION):Action.COMMIT.lower()])

        appPermCube.setCell(false, [(AXIS_RESOURCE):SYS_LOCK, (AXIS_ROLE):ROLE_READONLY, (AXIS_ACTION):Action.UPDATE.lower()])
        appPermCube.setCell(true, [(AXIS_RESOURCE):SYS_LOCK, (AXIS_ROLE):ROLE_ADMIN, (AXIS_ACTION):Action.COMMIT.lower()])

        appPermCube.setCell(true, [(AXIS_RESOURCE):null, (AXIS_ROLE):ROLE_ADMIN, (AXIS_ACTION):Action.RELEASE.lower()])
        appPermCube.setCell(true, [(AXIS_RESOURCE):null, (AXIS_ROLE):ROLE_ADMIN, (AXIS_ACTION):Action.COMMIT.lower()])
        appPermCube.setCell(true, [(AXIS_RESOURCE):null, (AXIS_ROLE):ROLE_USER, (AXIS_ACTION):Action.COMMIT.lower()])
        appPermCube.setCell(false, [(AXIS_RESOURCE):null, (AXIS_ROLE):ROLE_READONLY, (AXIS_ACTION):Action.UPDATE.lower()])

        persister.updateCube(appId, appPermCube, getUserId())
    }

    /**
     * Set the user ID on the current thread
     * @param user String user Id
     */
    void setUserId(String user)
    {
        userId.set(user?.trim())
    }

    /**
     * Retrieve the user ID from the current thread
     * @return String user ID of the user associated to the requesting thread
     */
    String getUserId()
    {
        return userId.get()
    }

    /**
     * Add wild card symbol at beginning and at end of string if not already present.
     * Remove wild card symbol if only character present.
     * @return String
     */
    private String handleWildCard(String value)
    {
        if (value)
        {
            if (!value.startsWith('*'))
            {
                value = '*' + value
            }
            if (!value.endsWith('*'))
            {
                value += '*'
            }
            if ('*' == value)
            {
                value = null
            }
        }
        return value
    }
}
