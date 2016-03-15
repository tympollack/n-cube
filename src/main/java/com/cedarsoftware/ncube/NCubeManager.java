package com.cedarsoftware.ncube;

import com.cedarsoftware.ncube.exception.BranchMergeException;
import com.cedarsoftware.util.ArrayUtilities;
import com.cedarsoftware.util.CaseInsensitiveMap;
import com.cedarsoftware.util.CaseInsensitiveSet;
import com.cedarsoftware.util.Converter;
import com.cedarsoftware.util.IOUtilities;
import com.cedarsoftware.util.MapUtilities;
import com.cedarsoftware.util.StringUtilities;
import com.cedarsoftware.util.SystemUtilities;
import com.cedarsoftware.util.TrackingMap;
import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;
import groovy.lang.GroovyClassLoader;
import ncube.grv.method.NCubeGroovyController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 *         Licensed under the Apache License, Version 2.0 (the "License");
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
public class NCubeManager
{
    public static final String SEARCH_INCLUDE_CUBE_DATA = "includeCubeData";
    public static final String SEARCH_INCLUDE_TEST_DATA = "includeTestData";
    public static final String SEARCH_INCLUDE_NOTES = "includeNotes";
    public static final String SEARCH_DELETED_RECORDS_ONLY = "deletedRecordsOnly";
    public static final String SEARCH_ACTIVE_RECORDS_ONLY = "activeRecordsOnly";
    public static final String SEARCH_CHANGED_RECORDS_ONLY = "changedRecordsOnly";
    public static final String SEARCH_EXACT_MATCH_NAME = "exactMatchName";
    public static final String SEARCH_CACHE_RESULT = "cacheResult";

    public static final String BRANCH_UPDATES = "updates";
    public static final String BRANCH_MERGES = "merges";
    public static final String BRANCH_CONFLICTS = "conflicts";

    public static final String SYS_BOOTSTRAP = "sys.bootstrap";
    public static final String SYS_PROTOTYPE = "sys.prototype";
    public static final String CLASSPATH_CUBE = "sys.classpath";

    private static final ConcurrentMap<ApplicationID, ConcurrentMap<String, Object>> ncubeCache = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ApplicationID, ConcurrentMap<String, Advice>> advices = new ConcurrentHashMap<>();
    private static final ConcurrentMap<ApplicationID, GroovyClassLoader> localClassLoaders = new ConcurrentHashMap<>();
    public static final String NCUBE_PARAMS = "NCUBE_PARAMS";
    private static NCubePersister nCubePersister;
    private static final Logger LOG = LogManager.getLogger(NCubeManager.class);

    // not private in case we want to tweak things for testing.
    static volatile ConcurrentMap<String, Object> systemParams = null;

    /**
     * Store the Persister to be used with the NCubeManager API (Dependency Injection API)
     */
    public static void setNCubePersister(NCubePersister persister)
    {
        nCubePersister = persister;
    }

    public static NCubePersister getPersister()
    {
        if (nCubePersister == null)
        {
            throw new IllegalStateException("Persister not set into NCubeManager.");
        }
        return nCubePersister;
    }

    public static Map<String, Object> getSystemParams()
    {
        final ConcurrentMap<String, Object> params = systemParams;

        if (params != null)
        {
            return params;
        }

        synchronized(NCubeManager.class)
        {
            if (systemParams == null)
            {
                String jsonParams = SystemUtilities.getExternalVariable(NCUBE_PARAMS);
                ConcurrentMap sysParamMap = new ConcurrentHashMap<>();

                if (StringUtilities.hasContent(jsonParams))
                {
                    try
                    {
                        Map args = new HashMap();
                        args.put(JsonReader.USE_MAPS, true);
                        sysParamMap = new ConcurrentHashMap<>((Map)JsonReader.jsonToJava(jsonParams, args));
                    }
                    catch (Exception e)
                    {
                        LOG.warn("Parsing of NCUBE_PARAMS failed.  " + jsonParams);
                    }
                }
                systemParams = sysParamMap;
            }
        }
        return systemParams;
    }

    /**
     * Fetch all the n-cube names for the given ApplicationID.  This API
     * will load all cube records for the ApplicationID (NCubeInfoDtos),
     * and then get the names from them.
     *
     * @return Set<String> n-cube names.  If an empty Set is returned,
     * then there are no persisted n-cubes for the passed in ApplicationID.
     */
    public static Set<String> getCubeNames(ApplicationID appId)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_ACTIVE_RECORDS_ONLY, true);
        List<NCubeInfoDto> cubeInfos = search(appId, null, null, options);
        Set<String> names = new TreeSet<>();

        for (NCubeInfoDto info : cubeInfos)
        {
            names.add(info.name);
        }

        if (names.isEmpty())
        {   // Support tests that load cubes from JSON files...
            // can only be in there as ncubes, not ncubeDtoInfo
            for (Object value : getCacheForApp(appId).values())
            {
                if (value instanceof NCube)
                {
                    NCube cube = (NCube) value;
                    names.add(cube.getName());
                }
            }
        }
        return new CaseInsensitiveSet<>(names);
    }

    /**
     * Load n-cube, bypassing any caching.  This is necessary for n-cube-editor (IDE time
     * usage).  If the IDE environment is clustered, cannot be getting stale copies from
     * cache.  Any advices in the manager will be applied to the n-cube.
     * @return NCube of the specified name from the specified AppID, or null if not found.
     */
    public static NCube loadCube(ApplicationID appId, String cubeName)
    {
        NCube ncube = getPersister().loadCube(appId, cubeName);
        if (ncube == null)
        {
            return null;
        }
        applyAdvices(ncube.getApplicationID(), ncube);
        Map<String, Object> cubes = getCacheForApp(appId);
        cubes.put(cubeName.toLowerCase(), ncube);     // Update cache
        return ncube;
    }

    /**
     * Fetch an n-cube by name from the given ApplicationID.  If no n-cubes
     * are loaded, then a loadCubes() call is performed and then the
     * internal cache is checked again.  If the cube is not found, null is
     * returned.
     */
    public static NCube getCube(ApplicationID appId, String name)
    {
        validateAppId(appId);
        NCube.validateCubeName(name);
        Map<String, Object> cubes = getCacheForApp(appId);
        final String lowerCubeName = name.toLowerCase();

        if (cubes.containsKey(lowerCubeName))
        {   // pull from cache
            final Object cube = cubes.get(lowerCubeName);
            return Boolean.FALSE == cube ? null : ensureLoaded(cube);
        }

        // now even items with metaProperties(cache = 'false') can be retrieved
        // and normal app processing doesn't do two queries anymore.
        // used to do getCubeInfoRecords() -> dto
        // and then dto -> loadCube(id)
        NCube ncube = getPersister().loadCube(appId, name);
        if (ncube == null)
        {
            cubes.put(lowerCubeName, Boolean.FALSE);
            return null;
        }
        return prepareCube(ncube);
    }

    public static NCube loadCubeById(long id)
    {
        NCube ncube = getPersister().loadCubeById(id);
        return ncube;
    }

    static NCube ensureLoaded(Object value)
    {
        if (value instanceof NCube)
        {
            return (NCube)value;
        }

        if (value instanceof NCubeInfoDto)
        {   // Lazy load cube (make sure to apply any advices to it)
            NCubeInfoDto dto = (NCubeInfoDto) value;
            long id = (long) Converter.convert(dto.id, long.class);
            return prepareCube(getPersister().loadCubeById(id));
        }

        throw new IllegalStateException("Failed to retrieve cube from cache, value: " + value);
    }

    private static NCube prepareCube(NCube cube)
    {
        applyAdvices(cube.getApplicationID(), cube);
        String cubeName = cube.getName().toLowerCase();
        if (!cube.getMetaProperties().containsKey("cache") || Boolean.TRUE.equals(cube.getMetaProperty("cache")))
        {   // Allow cubes to not be cached by specified 'cache':false as a cube meta-property.
            getCacheForApp(cube.getApplicationID()).put(cubeName, cube);
        }
        return cube;
    }

    /**
     * Testing API (Cache validation)
     */
    public static boolean isCubeCached(ApplicationID appId, String cubeName)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        Map<String, Object> ncubes = getCacheForApp(appId);
        Object cachedItem = ncubes.get(cubeName.toLowerCase());
        return cachedItem instanceof NCube || cachedItem instanceof NCubeInfoDto;
    }

    /**
     * Fetch the classloader for the given ApplicationID.
     */
    static URLClassLoader getUrlClassLoader(ApplicationID appId, Map input)
    {
        NCube cpCube = getCube(appId, CLASSPATH_CUBE);

        if (cpCube == null)
        {   // No sys.classpath cube exists, just create regular GroovyClassLoader with no URLs set into it.
            // Scope the GroovyClassLoader per ApplicationID
            return getLocalClassloader(appId);
        }

        final String envLevel = SystemUtilities.getExternalVariable("ENV_LEVEL");
        if (StringUtilities.hasContent(envLevel) && !doesMapContainKey(input, "env"))
        {   // Add in the 'ENV_LEVEL" environment variable when looking up sys.* cubes,
            // if there was not already an entry for it.
            input.put("env", envLevel);
        }
        if (!doesMapContainKey(input, "username"))
        {   // same as ENV_LEVEL, add it in if not already there.
            input.put("username", System.getProperty("user.name"));
        }
        Object urlCpLoader = cpCube.getCell(input);

        if (urlCpLoader instanceof URLClassLoader)
        {
            return (URLClassLoader)urlCpLoader;
        }

        throw new IllegalStateException("If the sys.classpath cube exists, it must return a URLClassLoader.");
    }

    private static boolean doesMapContainKey(Map map, String key)
    {
        if (map instanceof TrackingMap)
        {
            Map wrappedMap = ((TrackingMap)map).getWrappedMap();
            return wrappedMap.containsKey(key);
        }
        return map.containsKey(key);
    }

    static URLClassLoader getLocalClassloader(ApplicationID appId)
    {
        GroovyClassLoader gcl = localClassLoaders.get(appId);
        if (gcl == null)
        {
            gcl = new GroovyClassLoader();
            GroovyClassLoader classLoaderRef = localClassLoaders.putIfAbsent(appId, gcl);
            if (classLoaderRef != null)
            {
                gcl = classLoaderRef;
            }
        }
        return gcl;
    }

    /**
     * Add a cube to the internal cache of available cubes.
     * @param ncube NCube to add to the list.
     */
    public static void addCube(ApplicationID appId, NCube ncube)
    {
        validateAppId(appId);
        validateCube(ncube);

        String cubeName = ncube.getName().toLowerCase();

        if (!ncube.getMetaProperties().containsKey("cache") || Boolean.TRUE.equals(ncube.getMetaProperty("cache")))
        {   // Allow cubes to not be cached by specified 'cache':false as a cube meta-property.
            getCacheForApp(appId).put(cubeName, ncube);
        }

        // Apply any matching advices to it
        applyAdvices(appId, ncube);
    }

    /**
     * Fetch the Map of n-cubes for the given ApplicationID.  If no
     * cache yet exists, a new empty cache is added.
     */
    static Map<String, Object> getCacheForApp(ApplicationID appId)
    {
        ConcurrentMap<String, Object> ncubes = ncubeCache.get(appId);

        if (ncubes == null)
        {
            ncubes = new ConcurrentHashMap<>();
            ConcurrentMap<String, Object> mapRef = ncubeCache.putIfAbsent(appId, ncubes);
            if (mapRef != null)
            {
                ncubes = mapRef;
            }
        }
        return ncubes;
    }

    public static void clearCacheForBranches(ApplicationID appId)
    {
        synchronized (ncubeCache)
        {
            List<ApplicationID> list = new ArrayList<>();

            for (ApplicationID id : ncubeCache.keySet())
            {
                if (id.cacheKey().startsWith(appId.branchAgnosticCacheKey()))
                {
                    list.add(id);
                }
            }

            for (ApplicationID appId1 : list)
            {
                clearCache(appId1);
            }
        }
    }

    /**
     * Clear the cube (and other internal caches) for a given ApplicationID.
     * This will remove all the n-cubes from memory, compiled Groovy code,
     * caches related to expressions, caches related to method support,
     * advice caches, and local classes loaders (used when no sys.classpath is
     * present).
     *
     * @param appId ApplicationID for which the cache is to be cleared.
     */
    public static void clearCache(ApplicationID appId)
    {
        synchronized (ncubeCache)
        {
            validateAppId(appId);

            Map<String, Object> appCache = getCacheForApp(appId);
            clearGroovyClassLoaderCache(appCache);

            appCache.clear();
            GroovyBase.clearCache(appId);
            NCubeGroovyController.clearCache(appId);

            // Clear Advice cache
            Map<String, Advice> adviceCache = advices.get(appId);
            if (adviceCache != null)
            {
                adviceCache.clear();
            }

            // Clear ClassLoader cache
            GroovyClassLoader classLoader = localClassLoaders.get(appId);
            if (classLoader != null)
            {
                classLoader.clearCache();
                localClassLoaders.remove(appId);
            }
        }
    }

    /**
     * This method will clear all caches for all ApplicationIDs.
     * Do not call it for anything other than test purposes.
     */
    public static void clearCache()
    {
        synchronized (ncubeCache)
        {
            List<ApplicationID> list = new ArrayList<>();

            for (ApplicationID appId : ncubeCache.keySet())
            {
                list.add(appId);
            }

            for (ApplicationID appId1 : list)
            {
                clearCache(appId1);
            }
        }
    }

    private static void clearGroovyClassLoaderCache(Map<String, Object> appCache)
    {
        Object cube = appCache.get(CLASSPATH_CUBE);
        if (cube instanceof NCube)
        {
            NCube cpCube = (NCube) cube;
            for (Object content : cpCube.cells.values())
            {
                if (content instanceof UrlCommandCell)
                {
                    ((UrlCommandCell)content).clearClassLoaderCache();
                }
            }
        }
    }

    /**
     * Associate Advice to all n-cubes that match the passed in regular expression.
     */
    public static void addAdvice(ApplicationID appId, String wildcard, Advice advice)
    {
        validateAppId(appId);
        ConcurrentMap<String, Advice> current = advices.get(appId);
        if (current == null)
        {
            current = new ConcurrentHashMap<>();
            ConcurrentMap<String, Advice> mapRef = advices.putIfAbsent(appId, current);
            if (mapRef != null)
            {
                current = mapRef;
            }
        }

        current.put(advice.getName() + '/' + wildcard, advice);

        // Apply newly added advice to any fully loaded (hydrated) cubes.
        String regex = StringUtilities.wildcardToRegexString(wildcard);
        Map<String, Object> cubes = getCacheForApp(appId);

        for (Object value : cubes.values())
        {
            if (value instanceof NCube)
            {   // apply advice to hydrated cubes
                NCube ncube = (NCube) value;
                Axis axis = ncube.getAxis("method");
                addAdviceToMatchedCube(advice, regex, ncube, axis);
            }
        }
    }

    private static void addAdviceToMatchedCube(Advice advice, String regex, NCube ncube, Axis axis)
    {
        if (axis != null)
        {   // Controller methods
            for (Column column : axis.getColumnsWithoutDefault())
            {
                String method = column.getValue().toString();
                String classMethod = ncube.getName() + '.' + method + "()";
                if (classMethod.matches(regex))
                {
                    ncube.addAdvice(advice, method);
                }
            }
        }
        else
        {   // Expressions
            String classMethod = ncube.getName() + ".run()";
            if (classMethod.matches(regex))
            {
                ncube.addAdvice(advice, "run");
            }
        }
    }

    /**
     * Apply existing advices loaded into the NCubeManager, to the passed in
     * n-cube.  This allows advices to be added first, and then let them be
     * applied 'on demand' as an n-cube is loaded later.
     * @param appId ApplicationID
     * @param ncube NCube to which all matching advices will be applied.
     */
    private static void applyAdvices(ApplicationID appId, NCube ncube)
    {
        final Map<String, Advice> appAdvices = advices.get(appId);

        if (MapUtilities.isEmpty(appAdvices))
        {
            return;
        }
        for (Map.Entry<String, Advice> entry : appAdvices.entrySet())
        {
            final Advice advice = entry.getValue();
            final String wildcard = entry.getKey().replace(advice.getName() + '/', "");
            final String regex = StringUtilities.wildcardToRegexString(wildcard);
            final Axis axis = ncube.getAxis("method");
            addAdviceToMatchedCube(advice, regex, ncube, axis);
        }
    }

    /**
     * Retrieve all cube names that are deeply referenced by ApplicationID + n-cube name.
     */
    public static void getReferencedCubeNames(ApplicationID appId, String name, Set<String> refs)
    {
        if (refs == null)
        {
            throw new IllegalArgumentException("Could not get referenced cube names, null passed in for Set to hold referenced n-cube names, app: " + appId + ", n-cube: " + name);
        }
        validateAppId(appId);
        NCube.validateCubeName(name);
        NCube ncube = getCube(appId, name);
        if (ncube == null)
        {
            throw new IllegalArgumentException("Could not get referenced cube names, n-cube: " + name + " does not exist in app: " + appId);
        }
        Set<String> subCubeList = ncube.getReferencedCubeNames();

        // TODO: Use explicit stack, NOT recursion

        for (String cubeName : subCubeList)
        {
            if (!refs.contains(cubeName))
            {
                refs.add(cubeName);
                getReferencedCubeNames(appId, cubeName, refs);
            }
        }
    }

    /**
     * Get List<NCubeInfoDto> of n-cube record DTOs for the given ApplicationID (branch only).  If using
     * For any cube record loaded, for which there is no entry in the app's cube cache, an entry
     * is added mapping the cube name to the cube record (NCubeInfoDto).  This will be replaced
     * by an NCube if more than the name is required.
     * one (1) character.  This is universal whether using a SQL perister or Mongo persister.
     */
    public static List<NCubeInfoDto> getBranchChangesFromDatabase(ApplicationID appId)
    {
        validateAppId(appId);
        if (appId.getBranch().equals(ApplicationID.HEAD))
        {
            throw new IllegalArgumentException("Cannot get branch changes from HEAD");
        }

        ApplicationID headAppId = appId.asHead();
        Map<String, NCubeInfoDto> headMap = new TreeMap<>();

        Map<String, Object> searchChangedRecordOptions = new HashMap<>();
        searchChangedRecordOptions.put(SEARCH_CHANGED_RECORDS_ONLY, true);
        List<NCubeInfoDto> branchList = search(appId, null, null, searchChangedRecordOptions);

        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_ACTIVE_RECORDS_ONLY, false);
        List<NCubeInfoDto> headList = search(headAppId, null, null, options);

        List<NCubeInfoDto> list = new ArrayList<>();

        //  build map of head objects for reference.
        for (NCubeInfoDto info : headList)
        {
            headMap.put(info.name, info);
        }

        // Loop through changed (added, deleted, created, restored, updated) records
        for (NCubeInfoDto info : branchList)
        {
            long revision = (long) Converter.convert(info.revision, long.class);
            NCubeInfoDto head = headMap.get(info.name);

            if (head == null)
            {
                if (revision >= 0)
                {
                    info.changeType = ChangeType.CREATED.getCode();
                    list.add(info);
                }
            }
            else if (info.headSha1 == null)
            {   //  we created this guy locally
                // someone added this one to the head already
                info.changeType = ChangeType.CONFLICT.getCode();
                list.add(info);
            }
            else
            {
                if (StringUtilities.equalsIgnoreCase(info.headSha1, head.sha1))
                {
                    if (StringUtilities.equalsIgnoreCase(info.sha1, info.headSha1))
                    {
                        // only net change could be revision deleted or restored.  check head.
                        long headRev = Long.parseLong(head.revision);

                        if (headRev < 0 != revision < 0)
                        {
                            if (revision < 0)
                            {
                                info.changeType = ChangeType.DELETED.getCode();
                            }
                            else
                            {
                                info.changeType = ChangeType.RESTORED.getCode();
                            }

                            list.add(info);
                        }
                    }
                    else
                    {
                        info.changeType = ChangeType.UPDATED.getCode();
                        list.add(info);
                    }
                }
                else
                {
                    info.changeType = ChangeType.CONFLICT.getCode();
                    list.add(info);
                }
            }
        }

        cacheCubes(appId, list);
        return list;
    }

    private static void cacheCubes(ApplicationID appId, List<NCubeInfoDto> cubes)
    {
        Map<String, Object> appCache = getCacheForApp(appId);

        for (NCubeInfoDto cubeInfo : cubes)
        {
            String key = cubeInfo.name.toLowerCase();

            if (!cubeInfo.revision.startsWith("-"))
            {
                Object cachedItem = appCache.get(key);
                if (cachedItem == null || cachedItem instanceof NCubeInfoDto)
                {   // If cube not in cache or already in cache as infoDto, overwrite it
                    appCache.put(key, cubeInfo);
                }
                else if (cachedItem instanceof NCube)
                {   // If cube is already cached, make sure the SHA1's match - if not, then cache the new cubeInfo
                    NCube ncube = (NCube) cachedItem;
                    if (!ncube.sha1().equals(cubeInfo.sha1))
                    {
                        appCache.put(key, cubeInfo);
                    }
                }
            }
        }
    }

    /**
     * Restore a previously deleted n-cube.
     */
    public static void restoreCubes(ApplicationID appId, Object[] cubeNames, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();

        if (appId.isRelease())
        {
            throw new IllegalArgumentException(ReleaseStatus.RELEASE + " cubes cannot be restored, app: " + appId);
        }

        if (ArrayUtilities.isEmpty(cubeNames))
        {
            throw new IllegalArgumentException("Error, empty array of cube names passed in to be restored.");
        }

        // Batch restore
        getPersister().restoreCubes(appId, cubeNames, username);

        // Load cache
        for (Object name : cubeNames)
        {
            if ((name instanceof String))
            {
                String cubeName = (String) name;
                NCube.validateCubeName(cubeName);
                NCube ncube = getPersister().loadCube(appId, cubeName);
                addCube(appId, ncube);
            }
            else
            {
                throw new IllegalArgumentException("Non string name given for cube to restore: " + name);
            }
        }
    }

    /**
     * Get a List<NCubeInfoDto> containing all history for the given cube.
     */
    public static List<NCubeInfoDto> getRevisionHistory(ApplicationID appId, String cubeName)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        List<NCubeInfoDto> revisions = getPersister().getRevisions(appId, cubeName);
        return revisions;
    }

    /**
     * Return a List of Strings containing all unique App names for the given tenant.
     */
    public static List<String> getAppNames(String tenant)
    {
        return getPersister().getAppNames(tenant);
    }

    /**
     * Get all of the versions that exist for the given ApplicationID (tenant and app).
     * @return List<String> version numbers.
     */
    public static Map<String, List<String>> getVersions(String tenant, String app)
    {
        ApplicationID.validateTenant(tenant);
        ApplicationID.validateApp(app);
        return getPersister().getVersions(tenant, app);
    }

    /**
     * Duplicate the given n-cube specified by oldAppId and oldName to new ApplicationID and name,
     */
    public static void duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    {
        validateAppId(oldAppId);
        validateAppId(newAppId);

        newAppId.validateBranchIsNotHead();

        if (newAppId.isRelease())
        {
            throw new IllegalArgumentException("Cubes cannot be duplicated into a " + ReleaseStatus.RELEASE + " version, cube: " + newName + ", app: " + newAppId);
        }

        NCube.validateCubeName(oldName);
        NCube.validateCubeName(newName);

        if (oldName.equalsIgnoreCase(newName) && oldAppId.equals(newAppId))
        {
            throw new IllegalArgumentException("Could not duplicate, old name cannot be the same as the new name when oldAppId matches newAppId, name: " + oldName + ", app: " + oldAppId);
        }

        getPersister().duplicateCube(oldAppId, newAppId, oldName, newName, username);

        if (CLASSPATH_CUBE.equalsIgnoreCase(newName))
        {   // If another cube is renamed into sys.classpath,
            // then the entire class loader must be dropped (and then lazily rebuilt).
            clearCache(newAppId);
        }
        else
        {
            Map<String, Object> appCache = getCacheForApp(newAppId);
            appCache.remove(newName.toLowerCase());
        }

        broadcast(newAppId);
    }

    /**
     * Update the passed in NCube.  Only SNAPSHOT cubes can be updated.
     *
     * @param ncube      NCube to be updated.
     * @return boolean true on success, false otherwise
     */
    public static boolean updateCube(ApplicationID appId, NCube ncube, String username)
    {
        validateAppId(appId);
        validateCube(ncube);

        if (appId.isRelease())
        {
            throw new IllegalArgumentException(ReleaseStatus.RELEASE + " cubes cannot be updated, cube: " + ncube.getName() + ", app: " + appId);
        }

        appId.validateBranchIsNotHead();

        final String cubeName = ncube.getName();
        getPersister().updateCube(appId, ncube, username);
        ncube.setApplicationID(appId);

        if (CLASSPATH_CUBE.equalsIgnoreCase(cubeName))
        {   // If the sys.classpath cube is changed, then the entire class loader must be dropped.  It will be lazily rebuilt.
            clearCache(appId);
        }

        addCube(appId, ncube);
        broadcast(appId);
        return true;
    }

    /**
     * Create a branch off of a SNAPSHOT for the given ApplicationIDs n-cubes.
     */
    public static int createBranch(ApplicationID appId)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();
        int rows = getPersister().createBranch(appId);
        clearCache(appId);
        broadcast(appId);
        return rows;
    }

    public static int mergeAcceptMine(ApplicationID appId, Object[] cubeNames, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();
        Map<String, Object> appCache = getCacheForApp(appId);
        int count = 0;

        for (Object cubeName : cubeNames)
        {
            String cubeNameStr = (String)cubeName;
            getPersister().mergeAcceptMine(appId, cubeNameStr, username);
            appCache.remove(cubeNameStr.toLowerCase());
            count++;
        }
        return count;
    }

    public static int mergeAcceptTheirs(ApplicationID appId, Object[] cubeNames, Object[] branchSha1, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();
        Map<String, Object> appCache = getCacheForApp(appId);
        int count = 0;

        for (int i = 0; i < cubeNames.length; i++)
        {
            String cubeNameStr = (String)cubeNames[i];
            String sha1 = (String)branchSha1[i];
            getPersister().mergeAcceptTheirs(appId, cubeNameStr, sha1, username);
            appCache.remove(cubeNameStr.toLowerCase());
            count++;
        }

        return count;
    }

    /**
     * Commit the passed in changed cube records identified by NCubeInfoDtos.
     * @return array of NCubeInfoDtos that are to be committed.
     */
    public static List<NCubeInfoDto> commitBranch(ApplicationID appId, Object[] infoDtos, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();

        ApplicationID headAppId = appId.asHead();
        Map<String, NCubeInfoDto> headMap = new TreeMap<>();
        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_ACTIVE_RECORDS_ONLY, false);
        List<NCubeInfoDto> headInfo = search(headAppId, null, null, options);

        //  build map of head objects for reference.
        for (NCubeInfoDto info : headInfo)
        {
            headMap.put(info.name, info);
        }

        List<NCubeInfoDto> dtosToUpdate = new ArrayList<>(infoDtos.length);
        List<NCubeInfoDto> dtosMerged = new ArrayList<>();

        Map<String, Map> errors = new LinkedHashMap<>();

        for (Object dto : infoDtos)
        {
            NCubeInfoDto branchCubeInfo = (NCubeInfoDto)dto;

            if (!branchCubeInfo.isChanged())
            {
                continue;
            }
            if (branchCubeInfo.sha1 == null)
            {
                branchCubeInfo.sha1 = "";
            }

            // All changes go through here.
            NCubeInfoDto headCubeInfo = headMap.get(branchCubeInfo.name);
            long infoRev = (long) Converter.convert(branchCubeInfo.revision, long.class);

            if (headCubeInfo == null)
            {   // No matching head cube, CREATE case
                if (infoRev >= 0)
                {   // Only create if the cube in the branch is active (revision number not negative)
                    dtosToUpdate.add(branchCubeInfo);
                }
            }
            else if (StringUtilities.equalsIgnoreCase(branchCubeInfo.headSha1, headCubeInfo.sha1))
            {   // HEAD cube has not changed (at least in terms of SHA-1 it could have it's revision sign changed)
                if (StringUtilities.equalsIgnoreCase(branchCubeInfo.sha1, branchCubeInfo.headSha1))
                {   // Cubes are same, but active status could be opposite (delete or restore case)
                    long headRev = (long) Converter.convert(headCubeInfo.revision, long.class);
                    if ((infoRev < 0) != (headRev < 0))
                    {
                        dtosToUpdate.add(branchCubeInfo);
                    }
                }
                else
                {   // Regular update case (branch updated cube that was not touched in HEAD)
                    dtosToUpdate.add(branchCubeInfo);
                }
            }
            else if (StringUtilities.equalsIgnoreCase(branchCubeInfo.sha1, headCubeInfo.sha1))
            {   // Branch headSha1 does not match HEAD sha1, but it's SHA-1 matches the HEAD SHA-1.
                // This means that the branch cube and HEAD cube are identical, but the HEAD was
                // different when the branch was created.
                dtosToUpdate.add(branchCubeInfo);
            }
            else
            {
                String msg;
                if (branchCubeInfo.headSha1 == null)
                {
                    msg = ". A cube with the same name was added to HEAD since your branch was created.";
                }
                else
                {
                    msg = ". The cube changed since your last update branch.";
                }
                String message = "Conflict merging " + branchCubeInfo.name + msg;
                NCube mergedCube = checkForConflicts(appId, errors, message, branchCubeInfo, headCubeInfo, false);
                if (mergedCube != null)
                {
                    NCubeInfoDto mergedDto = getPersister().commitMergedCubeToHead(appId, mergedCube, username);
                    dtosMerged.add(mergedDto);
                }
            }
        }

        if (!errors.isEmpty())
        {
            throw new BranchMergeException(errors.size() + " merge conflict(s) committing branch.  Update your branch and retry commit.", errors);
        }

        List<NCubeInfoDto> committedCubes = new ArrayList<>(dtosToUpdate.size());
        Object[] ids = new Object[dtosToUpdate.size()];
        int i=0;
        for (NCubeInfoDto dto : dtosToUpdate)
        {
            ids[i++] = dto.id;
        }

        committedCubes.addAll(getPersister().commitCubes(appId, ids, username));
        committedCubes.addAll(dtosMerged);
        clearCache(appId);
        clearCache(headAppId);
        broadcast(appId);
        return committedCubes;
    }

    private static NCube checkForConflicts(ApplicationID appId, Map<String, Map> errors, String message, NCubeInfoDto info, NCubeInfoDto head, boolean reverse)
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("message", message);
        map.put("sha1", info.sha1);
        map.put("headSha1", head != null ? head.sha1 : null);

        try
        {
            if (head != null)
            {
                long branchCubeId = (long) Converter.convert(info.id, long.class);
                long headCubeId = (long) Converter.convert(head.id, long.class);
                NCube branchCube = getPersister().loadCubeById(branchCubeId);
                NCube headCube = getPersister().loadCubeById(headCubeId);

                if (info.headSha1 != null)
                {
                    NCube baseCube = getPersister().loadCubeBySha1(appId, info.name, info.headSha1);

                    Map delta1 = DeltaProcessor.getDelta(baseCube, branchCube);
                    Map delta2 = DeltaProcessor.getDelta(baseCube, headCube);

                    if (DeltaProcessor.areDeltaSetsCompatible(delta1, delta2))
                    {
                        if (reverse)
                        {
                            DeltaProcessor.mergeDeltaSet(headCube, delta1);
                            return headCube;
                        }
                        else
                        {
                            DeltaProcessor.mergeDeltaSet(branchCube, delta2);
                            return branchCube;
                        }
                    }
                }

                List<Delta> diff = DeltaProcessor.getDeltaDescription(branchCube, headCube);
                if (diff.size() > 0)
                {
                    map.put("diff", diff);
                }
                else
                {
                    return branchCube;
                }
            }
            else
            {
                map.put("diff", null);
            }
        }
        catch (Exception e)
        {
            map.put("diff", e.getMessage());
        }
        errors.put(info.name, map);
        return null;
    }

    private static NCube attemptMerge(ApplicationID appId, Map<String, Map> errors, String message, NCubeInfoDto info, NCubeInfoDto other)
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("message", message);
        map.put("sha1", info.sha1);
        map.put("headSha1", info.headSha1);

        long branchCubeId = (long) Converter.convert(info.id, long.class);
        long otherCubeId = (long) Converter.convert(other.id, long.class);
        NCube branchCube = getPersister().loadCubeById(branchCubeId);
        NCube otherCube = getPersister().loadCubeById(otherCubeId);
        NCube<?> baseCube;

        if (info.headSha1 != null)
        {   // Treat both as based on the same HEAD cube
            baseCube = getPersister().loadCubeBySha1(appId, info.name, info.headSha1);
        }
        else
        {   // Treat both as based on the same cube with same axes, no columns and no cells
            // This causes a complete 'build-up' delta.  If they are both merge compatible,
            // then they will merge.  This allows a new cube that has not yet been committed
            // to HEAD to be merged into.
            baseCube = branchCube.duplicate(info.name);
            baseCube.clearCells();
            for (Axis axis : baseCube.getAxes())
            {
                axis.clear();
            }
        }

        Map delta1 = DeltaProcessor.getDelta(baseCube, branchCube);
        Map delta2 = DeltaProcessor.getDelta(baseCube, otherCube);

        if (DeltaProcessor.areDeltaSetsCompatible(delta1, delta2))
        {
            DeltaProcessor.mergeDeltaSet(otherCube, delta1);
            return otherCube;
        }

        List<Delta> diff = DeltaProcessor.getDeltaDescription(branchCube, otherCube);
        if (diff.size() > 0)
        {
            map.put("diff", diff);
        }
        else
        {
            return branchCube;
        }
        errors.put(info.name, map);
        return null;
    }

    /**
     * Rollback the passed in list of n-cubes.  Each one will be returned to the state is was
     * when the branch was created.  This is an insert cube (maintaining revision history) for
     * each cube passed in.
     */
    public static int rollbackCubes(ApplicationID appId, Object[] names, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();
        int count = getPersister().rollbackCubes(appId, names, username);
        clearCache(appId);
        return count;
    }

    /**
     * Update a branch cube the passed in branch.  It can be the String 'HEAD' or the name of any branch.  The
     * cube with the passed in name will have the content from a cube with the same name, in the passed in branch,
     * merged into itself and persisted.
     */
    public static Map<String, Object> updateBranchCube(ApplicationID appId, String cubeName, String branch, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();

        ApplicationID srcAppId = appId.asBranch(branch);

        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_ACTIVE_RECORDS_ONLY, false);
        options.put(SEARCH_EXACT_MATCH_NAME, true);
        List<NCubeInfoDto> records = search(appId, cubeName, null, options);
        List<NCubeInfoDto> srcRecords = search(srcAppId, cubeName, null, options);

        List<NCubeInfoDto> updates = new ArrayList<>();
        List<NCubeInfoDto> dtosMerged = new ArrayList<>();
        Map<String, Map> conflicts = new CaseInsensitiveMap<>();
        Map<String, Object> ret = new LinkedHashMap<>();

        ret.put(BRANCH_MERGES, dtosMerged);
        ret.put(BRANCH_CONFLICTS, conflicts);

        if (records.isEmpty() || srcRecords.isEmpty())
        {
            ret.put(BRANCH_UPDATES, new ArrayList());
            return ret;
        }
        if (records.size() > 1)
        {
            throw new IllegalArgumentException("Name passed in matches more than one n-cube, no update performed. Name: " + cubeName + ", app: " + appId);
        }
        if (srcRecords.size() > 1)
        {
            throw new IllegalArgumentException("Name passed in matches more than one n-cube in branch (" + branch + "), no update performed. Name: " + cubeName + ", app: " + appId);
        }

        NCubeInfoDto srcDto = srcRecords.get(0);    // Exact match, only 1
        NCubeInfoDto info = records.get(0); // Exact match, only 1

        long infoRev = (long) Converter.convert(info.revision, long.class);
        long srcRev = (long) Converter.convert(srcDto.revision, long.class);
        boolean activeStatusMatches = (infoRev < 0) == (srcRev < 0);

        if (branch.equalsIgnoreCase(ApplicationID.HEAD))
        {   // Update from HEAD branch is done differently than update from neighbor branch
            // Did branch change?
            if (!info.isChanged())
            {   // No change on branch
                if (!activeStatusMatches || !StringUtilities.equalsIgnoreCase(info.headSha1, srcDto.sha1))
                {   // 1. The active/deleted statuses don't match, or
                    // 2. HEAD has different SHA1 but branch cube did not change, safe to update branch (fast forward)
                    // In both cases, the cube was marked NOT changed in the branch, so safe to update.
                    updates.add(srcDto);
                }
            }
            else if (StringUtilities.equalsIgnoreCase(info.sha1, srcDto.sha1))
            {   // If branch is 'changed' but has same SHA-1 as head, then see if branch needs Fast-Forward
                if (!StringUtilities.equalsIgnoreCase(info.headSha1, srcDto.sha1))
                {   // Fast-Forward branch
                    // Update HEAD SHA-1 on branch directly (no need to insert)
                    getPersister().updateBranchCubeHeadSha1((Long) Converter.convert(info.id, Long.class), srcDto.sha1);
                }
            }
            else
            {
                if (!StringUtilities.equalsIgnoreCase(info.headSha1, srcDto.sha1))
                {   // Cube is different than HEAD, AND it is not based on same HEAD cube, but it could be merge-able.
                    String message = "Cube was changed in both branch and HEAD";
                    NCube cube = checkForConflicts(appId, conflicts, message, info, srcDto, true);

                    if (cube != null)
                    {
                        NCubeInfoDto mergedDto = getPersister().commitMergedCubeToBranch(appId, cube, srcDto.sha1, username);
                        dtosMerged.add(mergedDto);
                    }
                }
            }
        }
        else
        {
            if (!StringUtilities.equalsIgnoreCase(info.sha1, srcDto.sha1))
            {   // Different SHA-1's
                String message = "Cube in " + appId.getBranch() + " conflicts with cube in " + branch;
                NCube cube = attemptMerge(appId, conflicts, message, info, srcDto);

                if (cube != null)
                {
                    NCubeInfoDto mergedDto = getPersister().commitMergedCubeToBranch(appId, cube, info.headSha1, username);
                    dtosMerged.add(mergedDto);
                }
            }
        }

        List<NCubeInfoDto> finalUpdates = new ArrayList<>(updates.size());

        Object[] ids = new Object[updates.size()];
        int i=0;
        for (NCubeInfoDto dto : updates)
        {
            ids[i++] = dto.id;
        }
        finalUpdates.addAll(getPersister().pullToBranch(appId, ids, username));
        clearCache(appId);
        ret.put(BRANCH_UPDATES, finalUpdates);
        return ret;
    }

    /**
     * Update a branch from the HEAD.  Changes from the HEAD are merged into the
     * supplied branch.  If the merge cannot be done perfectly, an exception is
     * thrown indicating the cubes that are in conflict.
     */
    public static Map<String, Object> updateBranch(ApplicationID appId, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();
        appId.validateStatusIsNotRelease();

        ApplicationID headAppId = appId.asHead();

        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_ACTIVE_RECORDS_ONLY, false);
        List<NCubeInfoDto> records = search(appId, null, null, options);
        Map<String, NCubeInfoDto> branchRecordMap = new CaseInsensitiveMap<>();

        for (NCubeInfoDto info : records)
        {
            branchRecordMap.put(info.name, info);
        }

        List<NCubeInfoDto> updates = new ArrayList<>();
        List<NCubeInfoDto> dtosMerged = new ArrayList<>();
        Map<String, Map> conflicts = new CaseInsensitiveMap<>();
        List<NCubeInfoDto> headRecords = search(headAppId, null, null, options);

        for (NCubeInfoDto head : headRecords)
        {
            NCubeInfoDto info = branchRecordMap.get(head.name);

            if (info == null)
            {   // HEAD has cube that branch does not have
                updates.add(head);
                continue;
            }

            long infoRev = (long) Converter.convert(info.revision, long.class);
            long headRev = (long) Converter.convert(head.revision, long.class);
            boolean activeStatusMatches = (infoRev < 0) == (headRev < 0);

            // Did branch change?
            if (!info.isChanged())
            {   // No change on branch
                if (!activeStatusMatches || !StringUtilities.equalsIgnoreCase(info.headSha1, head.sha1))
                {   // 1. The active/deleted statuses don't match, or
                    // 2. HEAD has different SHA1 but branch cube did not change, safe to update branch (fast forward)
                    // In both cases, the cube was marked NOT changed in the branch, so safe to update.
                    updates.add(head);
                }
            }
            else if (StringUtilities.equalsIgnoreCase(info.sha1, head.sha1))
            {   // If branch is 'changed' but has same SHA-1 as head, then see if branch needs Fast-Forward
                if (!StringUtilities.equalsIgnoreCase(info.headSha1, head.sha1))
                {   // Fast-Forward branch
                    // Update HEAD SHA-1 on branch directly (no need to insert)
                    getPersister().updateBranchCubeHeadSha1((Long) Converter.convert(info.id, Long.class), head.sha1);
                }
            }
            else
            {
                if (!StringUtilities.equalsIgnoreCase(info.headSha1, head.sha1))
                {   // Cube is different than HEAD, AND it is not based on same HEAD cube, but it could be merge-able.
                    String message = "Cube was changed in both branch and HEAD";
                    NCube cube = checkForConflicts(appId, conflicts, message, info, head, true);

                    if (cube != null)
                    {
                        NCubeInfoDto mergedDto = getPersister().commitMergedCubeToBranch(appId, cube, head.sha1, username);
                        dtosMerged.add(mergedDto);
                    }
                }
            }
        }

        List<NCubeInfoDto> finalUpdates = new ArrayList<>(updates.size());

        Object[] ids = new Object[updates.size()];
        int i=0;
        for (NCubeInfoDto dto : updates)
        {
            ids[i++] = dto.id;
        }
        finalUpdates.addAll(getPersister().pullToBranch(appId, ids, username));

        clearCache(appId);

        Map<String, Object> ret = new LinkedHashMap<>();
        ret.put(BRANCH_UPDATES, finalUpdates);
        ret.put(BRANCH_MERGES, dtosMerged);
        ret.put(BRANCH_CONFLICTS, conflicts);
        return ret;
    }

    /**
     * Perform release (SNAPSHOT to RELEASE) for the given ApplicationIDs n-cubes.
     */
    public static int releaseCubes(ApplicationID appId, String newSnapVer)
    {
        validateAppId(appId);
        ApplicationID.validateVersion(newSnapVer);
        int rows = getPersister().releaseCubes(appId, newSnapVer);
        clearCacheForBranches(appId);
        //TODO:  Does broadcast need to send all branches that have changed as a result of this?
        broadcast(appId);
        return rows;
    }

    public static void changeVersionValue(ApplicationID appId, String newVersion)
    {
        validateAppId(appId);

        if (appId.isRelease())
        {
            throw new IllegalArgumentException("Cannot change the version of a " + ReleaseStatus.RELEASE + " app, app: " + appId);
        }
        ApplicationID.validateVersion(newVersion);
        getPersister().changeVersionValue(appId, newVersion);
        clearCache(appId);
        clearCache(appId.asVersion(newVersion));
        broadcast(appId);
    }

    public static boolean renameCube(ApplicationID appId, String oldName, String newName, String username)
    {
        validateAppId(appId);
        appId.validateBranchIsNotHead();

        if (appId.isRelease())
        {
            throw new IllegalArgumentException("Cannot rename a " + ReleaseStatus.RELEASE + " cube, cube: " + oldName + ", app: " + appId);
        }

        NCube.validateCubeName(oldName);
        NCube.validateCubeName(newName);

        if (oldName.equalsIgnoreCase(newName))
        {
            throw new IllegalArgumentException("Could not rename, old name cannot be the same as the new name, name: " + oldName + ", app: " + appId);
        }

        boolean result = getPersister().renameCube(appId, oldName, newName, username);

        if (CLASSPATH_CUBE.equalsIgnoreCase(oldName) || CLASSPATH_CUBE.equalsIgnoreCase(newName))
        {   // If the sys.classpath cube is renamed, or another cube is renamed into sys.classpath,
            // then the entire class loader must be dropped (and then lazily rebuilt).
            clearCache(appId);
        }
        else
        {
            Map<String, Object> appCache = getCacheForApp(appId);
            appCache.remove(oldName.toLowerCase());
            appCache.remove(newName.toLowerCase());
        }

        broadcast(appId);
        return result;
    }

    public static boolean deleteBranch(ApplicationID appId)
    {
        appId.validateBranchIsNotHead();
        return getPersister().deleteBranch(appId);
    }

    /**
     * Delete the named NCube from the database
     *
     * @param cubeNames  Object[] of String cube names to be deleted (soft deleted)
     */
    public static boolean deleteCubes(ApplicationID appId, Object[] cubeNames, String username)
    {
        appId.validateBranchIsNotHead();
        return deleteCubes(appId, cubeNames, false, username);
    }

    static boolean deleteCubes(ApplicationID appId, Object[] cubeNames, boolean allowDelete, String username)
    {
        validateAppId(appId);
        if (!allowDelete)
        {
            if (appId.isRelease())
            {
                throw new IllegalArgumentException(ReleaseStatus.RELEASE + " cubes cannot be hard-deleted, app: " + appId);
            }
        }

        if (getPersister().deleteCubes(appId, cubeNames, allowDelete, username))
        {
            Map<String, Object> appCache = getCacheForApp(appId);
            for (int i=0; i < cubeNames.length; i++)
            {
                appCache.remove(((String)cubeNames[i]).toLowerCase());
            }
            broadcast(appId);
            return true;
        }
        return false;
    }

    public static boolean updateTestData(ApplicationID appId, String cubeName, String testData)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        return getPersister().updateTestData(appId, cubeName, testData);
    }

    public static String getTestData(ApplicationID appId, String cubeName)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        return getPersister().getTestData(appId, cubeName);
    }

    public static boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        return getPersister().updateNotes(appId, cubeName, notes);
    }

    public static String getNotes(ApplicationID appId, String cubeName)
    {
        validateAppId(appId);
        NCube.validateCubeName(cubeName);
        Map<String, Object> options = new HashMap<>();
        options.put(SEARCH_INCLUDE_NOTES, true);
        options.put(SEARCH_EXACT_MATCH_NAME, true);

        List<NCubeInfoDto> infos = search(appId, cubeName, null, options);
        if (infos.isEmpty())
        {
            throw new IllegalArgumentException("Could not fetch notes, no cube: " + cubeName + " in app: " + appId);
        }
        return infos.get(0).notes;
    }

    public static Set<String> getBranches(ApplicationID appId)
    {
        appId.validate();
        return getPersister().getBranches(appId);
    }

    public static Set<String> getBranches(String tenant)
    {
        ApplicationID.validateTenant(tenant);
        return getPersister().getBranches(tenant);
    }

    public static ApplicationID getApplicationID(String tenant, String app, Map<String, Object> coord)
    {
        ApplicationID.validateTenant(tenant);
        ApplicationID.validateApp(tenant);

        if (coord == null)
        {
            coord = new HashMap<>();
        }

        NCube bootCube = getCube(ApplicationID.getBootVersion(tenant, app), SYS_BOOTSTRAP);

        if (bootCube == null)
        {
             throw new IllegalStateException("Missing " + SYS_BOOTSTRAP + " cube in the 0.0.0 version for the app: " + app);
        }

        ApplicationID bootAppId = (ApplicationID) bootCube.getCell(coord);
        String version = bootAppId.getVersion();
        String status = bootAppId.getStatus();
        String branch = bootAppId.getBranch();

        if (!tenant.equalsIgnoreCase(bootAppId.getTenant()))
        {
            LOG.warn("sys.bootstrap cube for tenant '" + tenant + "', app '" + app + "' is returning a different tenant '" + bootAppId.getTenant() + "' than requested. Using '" + tenant + "' instead.");
        }

        if (!app.equalsIgnoreCase(bootAppId.getApp()))
        {
            LOG.warn("sys.bootstrap cube for tenant '" + tenant + "', app '" + app + "' is returning a different app '" + bootAppId.getApp() + "' than requested. Using '" + app + "' instead.");
        }

        return new ApplicationID(tenant, app, version, status, branch);
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
    public static List<NCubeInfoDto> search(ApplicationID appId, String cubeNamePattern, String content, Map options)
    {
        validateAppId(appId);

        if (options == null)
        {
            options = new HashMap();
        }

        List<NCubeInfoDto> cubes = getPersister().search(appId, cubeNamePattern, content, options);
        Boolean result = (Boolean)options.get(SEARCH_CACHE_RESULT);

        if (result == null || result)
        {
            cacheCubes(appId, cubes);
        }
        return cubes;
    }

    /**
     * This API will hand back a List of AxisRef, which is a complete description of a Reference
     * Axis pointer. It includes the Source ApplicationID, source Cube Name, source Axis Name,
     * and all the referenced cube/axis and filter (cube/method) parameters.
     * @param appId ApplicationID of the cube-set from which to fetch all the reference axes.
     * @return List<AxisRef>
     */
    public static List<AxisRef> getReferenceAxes(ApplicationID appId)
    {
        validateAppId(appId);
        return getPersister().getReferenceAxes(appId);
    }

    public static void updateReferenceAxes(List<AxisRef> axisRefs, String username)
    {
        for (AxisRef axisRef : axisRefs)
        {
            ApplicationID srcApp = axisRef.getSrcAppId();
            validateAppId(srcApp);
            ApplicationID destAppId = new ApplicationID(srcApp.getTenant(), axisRef.getDestApp(), axisRef.getDestVersion(), ReleaseStatus.RELEASE.name(), ApplicationID.HEAD);
            validateAppId(destAppId);
            ApplicationID transformAppId = new ApplicationID(srcApp.getTenant(), axisRef.getTransformApp(), axisRef.getTransformVersion(), ReleaseStatus.RELEASE.name(), ApplicationID.HEAD);
            validateAppId(transformAppId);
        }
        getPersister().updateReferenceAxes(axisRefs, username);
    }

    // ----------------------------------------- Resource APIs ---------------------------------------------------------
    public static String getResourceAsString(String name) throws Exception
    {
        URL url = NCubeManager.class.getResource('/' + name);
        Path resPath = Paths.get(url.toURI());
        return new String(Files.readAllBytes(resPath), "UTF-8");
    }

    static NCube getNCubeFromResource(String name)
    {
        return getNCubeFromResource(ApplicationID.testAppId, name);
    }

    public static NCube getNCubeFromResource(ApplicationID id, String name)
    {
        try
        {
            String json = getResourceAsString(name);
            NCube ncube = NCube.fromSimpleJson(json);
            ncube.setApplicationID(id);
            ncube.sha1();
            addCube(id, ncube);
            return ncube;
        }
        catch (NullPointerException e)
        {
            throw new IllegalArgumentException("Could not find the file [n-cube]: " + name + ", app: " + id, e);
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException)e;
            }
            throw new RuntimeException("Failed to load cube from resource: " + name, e);
        }
    }

    /**
     * Still used in getNCubesFromResource
     */
    private static Object[] getJsonObjectFromResource(String name) throws IOException
    {
        JsonReader reader = null;
        try
        {
            URL url = NCubeManager.class.getResource("/" + name);
            File jsonFile = new File(url.getFile());
            InputStream in = new BufferedInputStream(new FileInputStream(jsonFile));
            reader = new JsonReader(in, true);
            return (Object[]) reader.readObject();
        }
        finally
        {
            IOUtilities.close(reader);
        }
    }

    public static List<NCube> getNCubesFromResource(String name)
    {
        String lastSuccessful = "";
        try
        {
            Object[] cubes = getJsonObjectFromResource(name);
            List<NCube> cubeList = new ArrayList<>(cubes.length);

            for (Object cube : cubes)
            {
                JsonObject ncube = (JsonObject) cube;
                String json = JsonWriter.objectToJson(ncube);
                NCube nCube = NCube.fromSimpleJson(json);
                nCube.sha1();
                addCube(nCube.getApplicationID(), nCube);
                lastSuccessful = nCube.getName();
                cubeList.add(nCube);
            }

            return cubeList;
        }
        catch (Exception e)
        {
            String s = "Failed to load cubes from resource: " + name + ", last successful cube: " + lastSuccessful;
            LOG.warn(s);
            throw new RuntimeException(s, e);
        }
    }

    /**
     * Resolve the passed in String URL to a fully qualified URL object.  If the passed in String URL is relative
     * to a path in the sys.classpath, this method will perform (indirectly) the necessary HTTP HEAD requests to
     * determine which path it connects to.
     * @param url String url (relative or absolute)
     * @param input Map coordinate that the reuqested the URL (may include environment level settings that
     *              help sys.classpath select the correct ClassLoader.
     * @return URL fully qualified URL based on the passed in relative or absolute URL String.
     */
    public static URL getActualUrl(ApplicationID appId, String url, Map input)
    {
        validateAppId(appId);
        if (StringUtilities.isEmpty(url))
        {
            throw new IllegalArgumentException("URL cannot be null or empty, attempting to resolve relative to absolute url for app: " + appId);
        }
        String localUrl = url.toLowerCase();

        if (localUrl.startsWith("http:") || localUrl.startsWith("https:") || localUrl.startsWith("file:"))
        {   // Absolute URL
            try
            {
                return new URL(url);
            }
            catch (MalformedURLException e)
            {
                throw new IllegalArgumentException("URL is malformed: " + url, e);
            }
        }
        else
        {
            URL actualUrl;
            synchronized (url.intern())
            {
                URLClassLoader loader = getUrlClassLoader(appId, input);

                // Make URL absolute (uses URL roots added to NCubeManager)
                actualUrl = loader.getResource(url);
            }

            if (actualUrl == null)
            {
                String err = "Unable to resolve URL, make sure appropriate resource URLs are added to the sys.classpath cube, URL: " +
                        url + ", app: " + appId;
                throw new IllegalArgumentException(err);
            }
            return actualUrl;
        }
    }

    // ---------------------------------------- Validation APIs --------------------------------------------------------
    static void validateAppId(ApplicationID appId)
    {
        if (appId == null)
        {
            throw new IllegalArgumentException("ApplicationID cannot be null");
        }
        appId.validate();
    }

    static void validateCube(NCube cube)
    {
        if (cube == null)
        {
            throw new IllegalArgumentException("NCube cannot be null");
        }
        NCube.validateCubeName(cube.getName());
    }

    // ---------------------- Broadcast APIs for notifying other services in cluster of cache changes ------------------
    static void broadcast(ApplicationID appId)
    {
        // Write to 'system' tenant, 'NCE' app, version '0.0.0', SNAPSHOT, cube: sys.cache
        // Separate thread reads from this table every 1 second, for new commands, for
        // example, clear cache
    }
}
