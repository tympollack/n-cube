package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.CdnClassLoader
import com.cedarsoftware.util.CallableBean
import com.cedarsoftware.util.IOUtilities
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.SystemUtilities
import com.cedarsoftware.util.TrackingMap
import com.cedarsoftware.util.io.JsonObject
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic
import ncube.grv.method.NCubeGroovyController
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.regex.Pattern

import static com.cedarsoftware.ncube.NCubeConstants.CLASSPATH_CUBE
import static com.cedarsoftware.ncube.NCubeConstants.NCUBE_PARAMS
import static com.cedarsoftware.ncube.NCubeConstants.NCUBE_PARAMS_BRANCH
import static com.cedarsoftware.ncube.NCubeConstants.PROPERTY_CACHE

/**
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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
class NCubeRuntime implements NCubeMutableClient, ApplicationContextAware
{
    protected static ApplicationContext ctx
    private static final String MUTABLE_ERROR = 'Non-runtime method called:'
    private final CacheManager ncubeCacheManager
    private final CacheManager adviceCacheManager
    private final ConcurrentMap<ApplicationID, GroovyClassLoader> localClassLoaders = new ConcurrentHashMap<>()
    private final Logger LOG = LogManager.getLogger(NCubeRuntime.class)
    // not private in case we want to tweak things for testing.
    protected volatile ConcurrentMap<String, Object> systemParams = null
    protected CallableBean bean
    private boolean mutable

    NCubeRuntime(CallableBean bean, CacheManager ncubeCacheManager, CacheManager adviceCacheManager, boolean mutable)
    {
        this.bean = bean
        this.ncubeCacheManager = ncubeCacheManager
        this.adviceCacheManager = adviceCacheManager
        this.mutable = mutable
    }

    static NCubeRuntime getInstance()
    {
        NCubeRuntime bean = ctx.getBean('ncubeRuntime') as NCubeRuntime
        return bean
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
        Object[] searchResults = bean.call('ncubeController', 'search', [appId, cubeNamePattern, content, options]) as Object[]
        List<NCubeInfoDto> dtos = []
        searchResults.each { NCubeInfoDto dto -> dtos.add(dto) }
        return dtos
    }

    /**
     * Fetch an n-cube by name from the given ApplicationID.  If no n-cubes
     * are loaded, then a loadCubes() call is performed and then the
     * internal cache is checked again.  If the cube is not found, null is
     * returned.
     */
    NCube getCube(ApplicationID appId, String cubeName)
    {
        if (appId == null)
        {
            throw new IllegalArgumentException('ApplicationID cannot be null')
        }
        if (StringUtilities.isEmpty(cubeName))
        {
            throw new IllegalArgumentException('cubeName cannot be null')
        }
        return getCubeInternal(appId, cubeName)
    }

    String getTestData(ApplicationID appId, String cubeName)
    {
        String result = bean.call('ncubeController', 'updateCube', [appId, cubeName]) as String
        return result
    }

    void setUserId(String user)
    {
        // Only the Editor (READ WRITE) implementation should implement this
    }

    boolean updateCube(NCube ncube)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} updateCube")
        }
        boolean result = bean.call('ncubeController', 'updateCube', [ncube]) as boolean
        cacheCube(ncube)
        return result
    }

    NCube loadCubeById(long id)
    {
        NCube ncube = bean.call('ncubeController', 'loadCubeById', [id]) as NCube
        cacheCube(ncube)
        return ncube
    }

    void createCube(NCube ncube)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} createCube")
        }
        bean.call('ncubeController', 'createCube', [ncube])
        prepareCube(ncube)
    }

    Boolean duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} duplicate")
        }
        Boolean result = bean.call('ncubeController', 'duplicate', [oldAppId, newAppId, oldName, newName]) as Boolean
        return result
    }

    Boolean assertPermissions(ApplicationID appId, String resource, Action action)
    {
        Boolean result = bean.call('ncubeController', 'assertPermissions', [appId, resource, action]) as Boolean
        return result
    }

    Boolean checkPermissions(ApplicationID appId, String resource, Action action)
    {
        Boolean result = bean.call('ncubeController', 'checkPermissions', [appId, resource, action]) as Boolean
        return result
    }

    void setFakeId(String fake)
    {
        // TODO - does this need mutable check?
        bean.call('ncubeController', 'setFakeId', [fake])
    }

    String getImpliedId()
    {
        // TODO - does this need mutable check?
        bean.call('ncubeController', 'getImpliedId', [])
    }

    Boolean isAdmin(ApplicationID appId, boolean useRealId)
    {
        Boolean result = bean.call('ncubeController', 'isAppAdmin', [appId, useRealId]) as Boolean
        return result
    }

    String getAppLockedBy(ApplicationID appId)
    {
        String result = bean.call('ncubeController', 'getAppLockedBy', [appId]) as String
        return result
    }

    Boolean lockApp(ApplicationID appId)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} lockApp")
        }
        Boolean result = bean.call('ncubeController', 'lockApp', [appId, true]) as Boolean
        return result
    }

    void unlockApp(ApplicationID appId)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} unlockApp")
        }
        bean.call('ncubeController', 'unlockApp', [appId, false])
    }

    Integer moveBranch(ApplicationID appId, String newSnapVer)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} moveBranch")
        }
        Integer result = bean.call('ncubeController', 'moveBranch', [appId, newSnapVer]) as Integer
        return result
    }

    Integer releaseVersion(ApplicationID appId, String newSnapVer)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} releaseVersion")
        }
        Integer result = bean.call('ncubeController', 'releaseVersion', [appId, newSnapVer]) as Integer
        return result
    }

    Integer releaseCubes(ApplicationID appId, String newSnapVer)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} releaseCubes")
        }
        Integer result = bean.call('ncubeController', 'releaseCubes', [appId, newSnapVer]) as Integer
        return result
    }

    Boolean restoreCubes(ApplicationID appId, Object[] cubeNames)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} restoreCubes")
        }
        Boolean result = bean.call('ncubeController', 'restoreCubes', [appId, cubeNames]) as Boolean
        return result
    }

    List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName, boolean ignoreVersion)
    {
        List<NCubeInfoDto> result = bean.call('ncubeController', 'getRevisionHistory', [appId, cubeName, ignoreVersion]) as List
        return result
    }

    List<String> getAppNames(String tenant)
    {
        List<String> result = bean.call('ncubeController', 'getAppNames', [tenant]) as List
        return result
    }

    Map<String, List<String>> getVersions(String tenant, String app)
    {
        Map<String, List<String>> result = bean.call('ncubeController', 'getVersions', [tenant, app]) as Map
        return result
    }

    Integer copyBranch(ApplicationID srcAppId, ApplicationID targetAppId, boolean copyWithHistory)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} copyBranch")
        }
        Integer result = bean.call('ncubeController', 'copyBranch', [srcAppId, targetAppId, copyWithHistory]) as Integer
        return result
    }

    Set<String> getBranches(ApplicationID appId)
    {
        Set<String> result = bean.call('ncubeController', 'getBranches', [appId]) as Set
        return result
    }

    Integer getBranchCount(ApplicationID appId)
    {
        Integer result = bean.call('ncubeController', 'getBranchCount', [appId]) as Integer
        return result
    }

    Boolean deleteBranch(ApplicationID appId)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} deleteBranch")
        }
        Boolean result = bean.call('ncubeController', 'deleteBranch', [appId]) as Boolean
        // TODO: Remove branches cubes from cache
        return result
    }

    NCube mergeDeltas(ApplicationID appId, String cubeName, List<Delta> deltas)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} mergeDeltas")
        }
        NCube ncube = bean.call('ncubeController', 'mergeDeltas', [appId, cubeName, deltas]) as NCube
        cacheCube(ncube)
        return ncube
    }

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} deleteCubes")
        }
        Boolean result = bean.call('ncubeController', 'deleteCubes', [appId, cubeNames]) as Boolean
        // TODO: Remove deleted cubes from cache
        return result
    }

    Boolean deleteCubes(ApplicationID appId, Object[] cubeNames, boolean allowDelete)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} deleteCubes")
        }
        Boolean result = bean.call('ncubeController', 'deleteCubes', [appId, cubeNames, allowDelete]) as Boolean
        // TODO: Remove deleted cubes from cache
        return result
    }

    void changeVersionValue(ApplicationID appId, String newVersion)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} changeVersionValue")
        }
        bean.call('ncubeController', 'changeVersionValue', [appId, newVersion])
    }

    Boolean renameCube(ApplicationID appId, String oldName, String newName)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} renameCube")
        }
        Boolean result = bean.call('ncubeController', 'renameCube', [appId, oldName, newName]) as Boolean
        // TODO: remove cube that was renamed
        // TODO: cache cube with new name
        return result
    }

    void getReferencedCubeNames(ApplicationID appId, String cubeName, Set<String> references)
    {
        // TODO - no controller method
    }

    List<AxisRef> getReferenceAxes(ApplicationID appId)
    {
        List<AxisRef> result = bean.call('ncubeController', 'getReferenceAxes', [appId]) as List
        return result
    }

    void updateReferenceAxes(List<AxisRef> axisRefs)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} getReferenceAxes")
        }
        bean.call('ncubeController', 'getReferenceAxes', [axisRefs.toArray()])
        // TODO: All n-cubes touched have to be updated in the Cache
    }

    void updateAxisMetaProperties(ApplicationID appId, String cubeName, String axisName, Map<String, Object> newMetaProperties)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} updateAxisMetaProperties")
        }
        bean.call('ncubeController', 'updateAxisMetaProperties', [appId, cubeName, axisName, newMetaProperties])
        clearCache(appId)
        // TODO: Drop affected cubes from cache
    }

    Boolean saveTests(ApplicationID appId, String cubeName, String tests)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} saveTests")
        }
        Boolean result = bean.call('ncubeController', 'saveTests', [appId, cubeName, tests]) as Boolean
        return result
    }

    void clearTestDatabase()
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} clearTestDatabase")
        }
        bean.call('ncubeController', 'clearTestDatabase', []) as Boolean
    }

    List<NCubeInfoDto> getHeadChangesForBranch(ApplicationID appId)
    {
        List<NCubeInfoDto> changes = bean.call('ncubeController', 'getHeadChangesForBranch', [appId]) as List<NCubeInfoDto>
        return changes
    }

    List<NCubeInfoDto> getBranchChangesForHead(ApplicationID appId)
    {
        List<NCubeInfoDto> changes = bean.call('ncubeController', 'getBranchChangesForHead', [appId]) as List<NCubeInfoDto>
        return changes
    }

    List<NCubeInfoDto> getBranchChangesForMyBranch(ApplicationID appId, String branch)
    {
        List<NCubeInfoDto> changes = bean.call('ncubeController', 'getBranchChangesForMyBranch', [appId, branch]) as List<NCubeInfoDto>
        return changes
    }

    Map<String, Object> updateBranch(ApplicationID appId, Object[] cubeDtos = null)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} updateBranch")
        }
        Map<String, Object> map = bean.call('ncubeController', 'updateBranch', [appId, cubeDtos]) as Map<String, Object>
        clearCache(appId)
        return map
    }

    Map<String, Object> commitBranch(ApplicationID appId, Object[] inputCubes = null)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} commitBranch")
        }
        Map<String, Object> map = bean.call('ncubeController', 'commitBranch', [appId, inputCubes]) as Map<String, Object>
        clearCache(appId)
        clearCache(appId.asHead())
        return map
    }

    int rollbackCubes(ApplicationID appId, Object[] names)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} rollbackCubes")
        }
        int result = bean.call('ncubeController', 'rollbackCubes', [appId, names]) as int
        clearCache(appId)
        return result
    }

    int mergeAcceptMine(ApplicationID appId, Object[] cubeNames)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} mergeAcceptMine")
        }
        int result = bean.call('ncubeController', 'mergeAcceptMine', [appId, cubeNames]) as int
        return result
    }

    int mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames, String sourceBranch = ApplicationID.HEAD)
    {
        if (!mutable)
        {
            throw new IllegalStateException("${MUTABLE_ERROR} mergeAcceptTheirs")
        }
        int result = bean.call('ncubeController', 'mergeAcceptTheirs', [appId, cubeNames, sourceBranch]) as int
        return result
    }

    ApplicationID getBootVersion(String tenant, String app)
    {
        String branch = getSystemParams()[NCUBE_PARAMS_BRANCH]
        return new ApplicationID(tenant, app, "0.0.0", ReleaseStatus.SNAPSHOT.name(), StringUtilities.isEmpty(branch) ? ApplicationID.HEAD : branch)
    }

    //-- NCube Caching -------------------------------------------------------------------------------------------------

    /**
     * Clear the cube (and other internal caches) for a given ApplicationID.
     * This will remove all the n-cubes from memory, compiled Groovy code,
     * caches related to expressions, caches related to method support,
     * advice caches, and local classes loaders (used when no sys.classpath is
     * present).
     *
     * @param appId ApplicationID for which the cache is to be cleared.
     */
    void clearCache(ApplicationID appId)
    {
        synchronized (ncubeCacheManager)
        {
            ApplicationID.validateAppId(appId)

            // Clear NCube cache
            Cache cubeCache = ncubeCacheManager.getCache(appId.toString())
            cubeCache.clear()   // eviction will trigger removalListener, which clears other NCube internal caches

            GroovyBase.clearCache(appId)
            NCubeGroovyController.clearCache(appId)

            // Clear Advice cache
            Cache adviceCache = adviceCacheManager.getCache(appId.toString())
            adviceCache.clear()

            // Clear ClassLoader cache
            GroovyClassLoader classLoader = localClassLoaders[appId]
            if (classLoader != null)
            {
                classLoader.clearCache()
                localClassLoaders.remove(appId)
            }
        }
    }

    protected void clearCache()
    {
        ncubeCacheManager.cacheNames.each { String appIdString ->
            ApplicationID appId = ApplicationID.convert(appIdString)
            clearCache(appId)
            clearCache(appId.asRelease())
        }
    }
    
    /**
     * Add a cube to the internal cache of available cubes.
     * @param ncube NCube to add to the list.
     */
    void addCube(NCube ncube)
    {
        ApplicationID appId = ncube.applicationID
        ApplicationID.validateAppId(appId)
        validateCube(ncube)
        prepareCube(ncube)
    }

    private void cacheCube(NCube ncube)
    {
        if (!ncube.metaProperties.containsKey(PROPERTY_CACHE) || Boolean.TRUE == ncube.getMetaProperty(PROPERTY_CACHE))
        {
            Cache cubeCache = ncubeCacheManager.getCache(ncube.applicationID.toString())
            cubeCache.put(ncube.name.toLowerCase(), ncube)
        }
    }

    private NCube getCubeInternal(ApplicationID appId, String cubeName)
    {
        Cache cubeCache = ncubeCacheManager.getCache(appId.toString())
        final String lowerCubeName = cubeName.toLowerCase()

        Cache.ValueWrapper item = cubeCache.get(lowerCubeName)
        if (item != null)
        {   // pull from cache
            Object value = item.get()
            return Boolean.FALSE == value ? null : value as NCube
        }

        // now even items with metaProperties(cache = 'false') can be retrieved
        // and normal app processing doesn't do two queries anymore.
        // used to do getCubeInfoRecords() -> dto
        // and then dto -> loadCube(id)
        NCube ncube = bean.call('ncubeController', 'getCube', [appId, cubeName]) as NCube
        if (ncube == null)
        {
            cubeCache.put(lowerCubeName, false)
            return null
        }
        return prepareCube(ncube)
    }

    private NCube prepareCube(NCube cube)
    {
        applyAdvices(cube)
        cacheCube(cube)
        return cube
    }

    //-- Advice --------------------------------------------------------------------------------------------------------

    /**
     * Associate Advice to all n-cubes that match the passed in regular expression.
     */
    void addAdvice(ApplicationID appId, String wildcard, Advice advice)
    {
        ApplicationID.validateAppId(appId)
        Cache current = adviceCacheManager.getCache(appId.toString())
        current.put("${advice.name}/${wildcard}".toString(), advice)

        // Apply newly added advice to any fully loaded (hydrated) cubes.
        String regex = StringUtilities.wildcardToRegexString(wildcard)
        Pattern pattern = Pattern.compile(regex)

        if (ncubeCacheManager instanceof NCubeCacheManager)
        {
            ((NCubeCacheManager)ncubeCacheManager).applyToEntries(appId.toString(), { String key, Object value ->
                if (value instanceof NCube)
                {   // apply advice to hydrated cubes
                    NCube ncube = value as NCube
                    Axis axis = ncube.getAxis('method')
                    addAdviceToMatchedCube(advice, pattern, ncube, axis)
                }
            })
        }
    }

    private void addAdviceToMatchedCube(Advice advice, Pattern pattern, NCube ncube, Axis axis)
    {
        if (axis != null)
        {   // Controller methods
            for (Column column : axis.columnsWithoutDefault)
            {
                String method = column.value.toString()
                String classMethod = "${ncube.name}.${method}()"
                if (pattern.matcher(classMethod).matches())
                {
                    ncube.addAdvice(advice, method)
                }
            }
        }

        // Add support for run() method (inline GroovyExpressions)
        String classMethod = ncube.name + '.run()'
        if (pattern.matcher(classMethod).matches())
        {
            ncube.addAdvice(advice, 'run')
        }
    }

    /**
     * Apply existing advices loaded into the NCubeRuntime, to the passed in
     * n-cube.  This allows advices to be added first, and then let them be
     * applied 'on demand' as an n-cube is loaded later.
     * @param appId ApplicationID
     * @param ncube NCube to which all matching advices will be applied.
     */
    private void applyAdvices(NCube ncube)
    {
        if (adviceCacheManager instanceof NCubeCacheManager)
        {
            ((NCubeCacheManager)adviceCacheManager).applyToEntries(ncube.applicationID.toString(), { String key, Object value ->
                final Advice advice = value as Advice
                final String wildcard = key.replace(advice.name + '/', "")
                final String regex = StringUtilities.wildcardToRegexString(wildcard)
                final Axis axis = ncube.getAxis('method')
                addAdviceToMatchedCube(advice, Pattern.compile(regex), ncube, axis)
            })
        }
    }

    //-- Classloader / Classpath ---------------------------------------------------------------------------------------

    /**
     * Resolve the passed in String URL to a fully qualified URL object.  If the passed in String URL is relative
     * to a path in the sys.classpath, this method will perform (indirectly) the necessary HTTP HEAD requests to
     * determine which path it connects to.
     * @param url String url (relative or absolute)
     * @param input Map coordinate that the reuqested the URL (may include environment level settings that
     *              help sys.classpath select the correct ClassLoader.
     * @return URL fully qualified URL based on the passed in relative or absolute URL String.
     */
    URL getActualUrl(ApplicationID appId, String url, Map input)
    {
        ApplicationID.validateAppId(appId)
        if (StringUtilities.isEmpty(url))
        {
            throw new IllegalArgumentException("URL cannot be null or empty, attempting to resolve relative to absolute url for app: ${appId}")
        }
        String localUrl = url.toLowerCase()

        if (localUrl.startsWith('http:') || localUrl.startsWith('https:') || localUrl.startsWith('file:'))
        {   // Absolute URL
            try
            {
                return new URL(url)
            }
            catch (MalformedURLException e)
            {
                throw new IllegalArgumentException("URL is malformed: ${url}", e)
            }
        }
        else
        {
            URL actualUrl
            synchronized (url.intern())
            {
                URLClassLoader loader = getUrlClassLoader(appId, input)

                // Make URL absolute (uses URL roots added to NCubeRuntime)
                actualUrl = loader.getResource(url)
            }

            if (actualUrl == null)
            {
                String err = "Unable to resolve URL, make sure appropriate resource URLs are added to the sys.classpath cube, URL: ${url}, app: ${appId}"
                throw new IllegalArgumentException(err)
            }
            return actualUrl
        }
    }

    /**
     * Fetch the classloader for the given ApplicationID.
     */
    URLClassLoader getUrlClassLoader(ApplicationID appId, Map input)
    {
        NCube cpCube = getCube(appId, CLASSPATH_CUBE)

        if (cpCube == null)
        {   // No sys.classpath cube exists, just create regular GroovyClassLoader with no URLs set into it.
            // Scope the GroovyClassLoader per ApplicationID
            return getLocalClassloader(appId)
        }

        // duplicate input coordinate - no need to validate, that will be done inside cpCube.getCell() later.
        Map copy = new HashMap(input)

        final String envLevel = SystemUtilities.getExternalVariable('ENV_LEVEL')
        if (StringUtilities.hasContent(envLevel) && !doesMapContainKey(copy, 'env'))
        {   // Add in the 'ENV_LEVEL" environment variable when looking up sys.* cubes,
            // if there was not already an entry for it.
            copy.env = envLevel
        }
        if (!doesMapContainKey(copy, 'username'))
        {   // same as ENV_LEVEL, add if not already there.
            copy.username = System.getProperty('user.name')
        }
        Object urlCpLoader = cpCube.getCell(copy)

        if (urlCpLoader instanceof URLClassLoader)
        {
            return (URLClassLoader)urlCpLoader
        }

        throw new IllegalStateException('If the sys.classpath cube exists, it must return a URLClassLoader.')
    }

    URLClassLoader getLocalClassloader(ApplicationID appId)
    {
        GroovyClassLoader gcl = localClassLoaders[appId]
        if (gcl == null)
        {
            gcl = new CdnClassLoader(NCubeRuntime.class.classLoader)
            GroovyClassLoader classLoaderRef = localClassLoaders.putIfAbsent(appId, gcl)
            if (classLoaderRef != null)
            {
                gcl = classLoaderRef
            }
        }
        return gcl
    }

    private boolean doesMapContainKey(Map map, String key)
    {
        if (map instanceof TrackingMap)
        {
            Map wrappedMap = ((TrackingMap)map).getWrappedMap()
            return wrappedMap.containsKey(key)
        }
        return map.containsKey(key)
    }

    //-- Environment Variables -----------------------------------------------------------------------------------------

    Map<String, Object> getSystemParams()
    {
        final ConcurrentMap<String, Object> params = systemParams

        if (params != null)
        {
            return params
        }

        synchronized (NCubeRuntime.class)
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
        systemParams = null
    }

    //-- Resource APIs -------------------------------------------------------------------------------------------------

    static String getResourceAsString(String name) throws Exception
    {
        URL url = NCubeRuntime.class.getResource("/${name}")
        Path resPath = Paths.get(url.toURI())
        return new String(Files.readAllBytes(resPath), "UTF-8")
    }
    
    NCube getNCubeFromResource(ApplicationID appId, String name)
    {
        try
        {
            String json = getResourceAsString(name)
            NCube ncube = NCube.fromSimpleJson(json)
            ncube.applicationID = appId
            ncube.sha1()
            prepareCube(ncube)
            return ncube
        }
        catch (NullPointerException e)
        {
            throw new IllegalArgumentException("Could not find the file [n-cube]: ${name}, app: ${appId}", e)
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException)e
            }
            throw new RuntimeException("Failed to load cube from resource: ${name}", e)
        }
    }

    /**
     * Still used in getNCubesFromResource
     */
    private Object[] getJsonObjectFromResource(String name) throws IOException
    {
        JsonReader reader = null
        try
        {
            URL url = NCubeRuntime.class.getResource('/' + name)
            File jsonFile = new File(url.file)
            InputStream input = new BufferedInputStream(new FileInputStream(jsonFile))
            reader = new JsonReader(input, true)
            return (Object[]) reader.readObject()
        }
        finally
        {
            IOUtilities.close(reader)
        }
    }

    List<NCube> getNCubesFromResource(ApplicationID appId, String name)
    {
        String lastSuccessful = ''
        try
        {
            Object[] cubes = getJsonObjectFromResource(name)
            List<NCube> cubeList = new ArrayList<>(cubes.length)

            for (Object cube : cubes)
            {
                JsonObject ncube = (JsonObject) cube
                String json = JsonWriter.objectToJson(ncube)
                NCube nCube = NCube.fromSimpleJson(json)
                nCube.applicationID = appId
                nCube.sha1()
                prepareCube(nCube)
                lastSuccessful = nCube.name
                cubeList.add(nCube)
            }

            return cubeList
        }
        catch (Exception e)
        {
            String s = "Failed to load cubes from resource: ${name}, last successful cube: ${lastSuccessful}"
            LOG.warn(s)
            throw new RuntimeException(s, e)
        }
    }

    //-- Validation ----------------------------------------------------------------------------------------------------

    protected void validateCube(NCube cube)
    {
        if (cube == null)
        {
            throw new IllegalArgumentException('NCube cannot be null')
        }
        NCube.validateCubeName(cube.name)
    }

    void setApplicationContext(ApplicationContext applicationContext)
    {
        ctx = applicationContext
    }
}