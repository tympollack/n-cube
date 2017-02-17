package com.cedarsoftware.util

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeManager
import com.cedarsoftware.ncube.NCubeRuntime
import groovy.transform.CompileStatic

import static com.cedarsoftware.util.VisualizerConstants.*

/**
 * Provides information to visualize n-cubes.
 */

@CompileStatic
class VisualizerInfo
{
    ApplicationID appId
    Map<String, Object> scope
    List<Map<String, Object>> nodes = []
    List<Map<String, Object>> edges = []

    long maxLevel
    long nodeCount
    long relInfoCount
    long defaultLevel
    String loadCellValuesLabel

    Map<String,String> allGroups
    Set<String> allGroupsKeys
    String groupSuffix
    Set<String> availableGroupsAllLevels
    Set<String> messages

    Map<String, Map<String, Set<Object>>> optionalScopeValues = new CaseInsensitiveMap()
    Map<String, Map<String, Set<Object>>> requiredScopeValues = new CaseInsensitiveMap()
    Map<String, Set<String>> requiredScopeKeys = [:]
    Map<String, Set<String>> optionalScopeKeys = [:]
    VisualizerScopeInfo scopeInfo = new VisualizerScopeInfo()

    Map<String, Object> networkOverridesBasic
    Map<String, Object> networkOverridesFull
    Map<String, Object> networkOverridesTopNode

    Map<String, List<String>> typesToAddMap = [:]

    VisualizerInfo(){}

    VisualizerInfo(ApplicationID applicationID, Map options)
    {
        appId = applicationID
        scope = options.scope as CaseInsensitiveMap
        loadConfigurations(cubeType)
    }

    protected void init(Map scope)
    {
        maxLevel = 1
        nodeCount = 1
        relInfoCount = 1
        messages = new LinkedHashSet()
        availableGroupsAllLevels = new LinkedHashSet()
        this.scope = scope as CaseInsensitiveMap ?: new CaseInsensitiveMap<>()
        scopeInfo = new VisualizerScopeInfo()
    }

    protected String getCubeType()
    {
        return CUBE_TYPE_DEFAULT
    }

    boolean addMissingMinimumScope(String scopeKey, String value, String message, Set<String> messages)
    {
        Map<String, Object> scope = scope
        boolean missingScope
        if (scope.containsKey(scopeKey))
        {
            if (!scope[scopeKey])
            {
                missingScope = true
            }
        }
        else
        {
            missingScope = true
        }

        if (missingScope)
        {
            message = message ?: "Scope for ${scopeKey} was added since required. The scope value may be changed as desired."
            messages << message
            if (value)
            {
                scope[scopeKey] = value
            }
        }
        return missingScope
    }

    NCube loadConfigurations(String cubeType)
    {
        String configJson = NCubeRuntime.getResourceAsString(JSON_FILE_PREFIX + VISUALIZER_CONFIG_CUBE_NAME + JSON_FILE_SUFFIX)
        NCube configCube = NCube.fromSimpleJson(configJson)
        configCube.applicationID = appId
        String networkConfigJson = NCubeRuntime.getResourceAsString(JSON_FILE_PREFIX + VISUALIZER_CONFIG_NETWORK_OVERRIDES_CUBE_NAME + JSON_FILE_SUFFIX)
        NCube networkConfigCube = NCube.fromSimpleJson(networkConfigJson)
        networkConfigCube.applicationID = appId

        networkOverridesBasic = networkConfigCube.getCell([(CONFIG_ITEM): CONFIG_NETWORK_OVERRIDES_BASIC, (CUBE_TYPE): cubeType]) as Map
        networkOverridesFull = networkConfigCube.getCell([(CONFIG_ITEM): CONFIG_NETWORK_OVERRIDES_FULL, (CUBE_TYPE): cubeType]) as Map
        networkOverridesTopNode = networkConfigCube.getCell([(CONFIG_ITEM): CONFIG_NETWORK_OVERRIDES_TOP_NODE, (CUBE_TYPE): cubeType]) as Map
        defaultLevel = configCube.getCell([(CONFIG_ITEM): CONFIG_DEFAULT_LEVEL, (CUBE_TYPE): cubeType]) as long
        allGroups = configCube.getCell([(CONFIG_ITEM): CONFIG_ALL_GROUPS, (CUBE_TYPE): cubeType]) as Map
        allGroupsKeys = new CaseInsensitiveSet(allGroups.keySet())
        String groupSuffix = configCube.getCell([(CONFIG_ITEM): CONFIG_GROUP_SUFFIX, (CUBE_TYPE): cubeType]) as String
        this.groupSuffix = groupSuffix ?: ''
        loadTypesToAddMap(configCube)
        loadCellValuesLabel = getLoadCellValuesLabel()
        return configCube
    }

    List getTypesToAdd(String group)
    {
        return typesToAddMap[allGroups[group]]
    }

    void loadTypesToAddMap(NCube configCube)
    {
        typesToAddMap = [:]
        Set<String> allTypes = configCube.getCell([(CONFIG_ITEM): CONFIG_ALL_TYPES, (CUBE_TYPE): cubeType]) as Set
        allTypes.each{String type ->
            Map.Entry<String, String> entry = allGroups.find{ String key, String value ->
                value == type
            }
            typesToAddMap[entry.value] = allTypes as List
        }
    }

    protected String getLoadCellValuesLabel()
    {
        'cell values'
    }

    Set<Object> getOptionalScopeValues( String cubeName, String scopeKey)
    {
        return getScopeValues(optionalScopeValues, cubeName, scopeKey)
    }

    Set<Object> getRequiredScopeValues(String cubeName, String scopeKey)
    {
        return getScopeValues(requiredScopeValues, cubeName, scopeKey)
    }

    Set<Object> getScopeValues( Map<String, Map<String, Set<Object>>> scopeValues, String cubeName, String scopeKey)
    {
        //The key to the map scopeValues is a scope key. The map contains a map of scope values by cube name.
        Map<String, Set<Object>> scopeValuesForScopeKey = scopeValues[scopeKey] as Map ?: new CaseInsensitiveMap()
        Set<Object> scopeValuesForCubeAndScopeKey = scopeValuesForScopeKey[cubeName] ?: getColumnValues(appId, cubeName, scopeKey)
        scopeValuesForScopeKey[cubeName] = scopeValuesForCubeAndScopeKey
        scopeValues[scopeKey] = scopeValuesForScopeKey
        return scopeValuesForCubeAndScopeKey
    }

    protected static Set<Object> getColumnValues(ApplicationID applicationID, String cubeName, String axisName)
    {
        NCube cube = NCubeManager.getCube(applicationID, cubeName)
        Set values = new LinkedHashSet()
        Axis axis = cube?.getAxis(axisName)
        if (axis)
        {
            for (Column column : axis.columnsWithoutDefault)
            {
                values.add(column.value)
            }
        }
        return values
    }

    void convertToSingleMessage()
    {
        messages = messages ? [messages.join(DOUBLE_BREAK)] as Set : null
    }
}