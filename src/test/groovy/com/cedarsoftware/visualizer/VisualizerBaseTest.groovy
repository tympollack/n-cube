package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.NCubeCleanupBaseTest
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.ReleaseStatus
import com.cedarsoftware.ncube.SpringAppContext
import org.junit.Ignore

import static com.cedarsoftware.visualizer.VisualizerConstants.*
import static com.cedarsoftware.visualizer.VisualizerTestConstants.*
import groovy.transform.CompileStatic
import org.junit.Before

@CompileStatic
@Ignore
class VisualizerBaseTest extends NCubeCleanupBaseTest
{
    protected static final String TEST_APP_NAME = 'test.visualizer'
    protected static final String TEST_APP_VERSION = '1.0.8'
    protected ApplicationID appId = new ApplicationID(ApplicationID.DEFAULT_TENANT, TEST_APP_NAME, TEST_APP_VERSION, ReleaseStatus.SNAPSHOT.name(), ApplicationID.HEAD)

    protected Visualizer visualizer
    protected Map returnMap
    protected VisualizerInfo visInfo
    protected Set messages
    protected Map<Long, Map<String, Object>> nodes
    protected Map<Long, Map<String, Object>> edges
    protected Map<String, Object> selectedNode

    protected static final String DEFAULT_SCOPE_DATE = DATE_TIME_FORMAT.format(new Date())

    @Before
    void setup(){
        super.setup()
        preloadCubes()
        visualizer = getVisualizer()
        returnMap = null
        visInfo = null
        messages = null
        nodes = null
        edges = null
        selectedNode = null
    }

    protected Visualizer getVisualizer()
    {
        return new Visualizer(runtimeClient)
    }

    protected VisualizerRelInfo getVisualizerRelInfo()
    {
        return new VisualizerRelInfo(runtimeClient, appId)
    }

    protected Map loadGraph(Map options, boolean hasMessages = false)
    {
        visInfo?.nodes = [:]
        visInfo?.edges = [:]
        Map returnMap = visualizer.loadGraph(appId, options)
        visInfo = returnMap.visInfo as VisualizerInfo
        messages = visInfo.messages
        if (!hasMessages)
        {
            assert !messages
        }
        nodes = visInfo.nodes as Map
        edges = visInfo.edges as Map
        return nodes[1l]
    }

    protected Map loadScopeChange(Map node, boolean hasMessages = false)
    {
        visInfo.selectedNodeId = node.id as Long
        Map returnMap = visualizer.loadScopeChange(appId, [visInfo: visInfo])
        visInfo = returnMap.visInfo as VisualizerInfo
        messages = visInfo.messages
        if (!hasMessages)
        {
            assert !messages
        }
        nodes = visInfo.nodes as Map
        edges = visInfo.edges as Map
        return nodes[node.id as Long]
    }

    protected Map loadNodeDetails(Map node, boolean hasMessages = false)
    {
        visInfo.selectedNodeId = node.id as Long
        Map returnMap = visualizer.loadNodeDetails(appId, [visInfo: visInfo])
        visInfo = returnMap.visInfo as VisualizerInfo
        messages = visInfo.messages
        if (!hasMessages)
        {
            assert !messages
        }
        nodes = visInfo.nodes as Map
        edges = visInfo.edges as Map
        return nodes[node.id as Long]
    }

    protected static void checkScopePromptTitle(Map node, String scopeKey, boolean required, String cubeNames = null, String scopeType = null)
    {
        String nodeDetails = node.details as String
        if (required)
        {
            assert nodeDetails.contains("""title="Scope key ${scopeKey} is required to load""")
        }
        else if ('additionalGraphScope' == scopeType)
        {
            assert nodeDetails.contains("Scope key ${scopeKey} is used in the in the visualization. It may be optional for some classes and required by others.")
        }
        else
        {
            assert nodeDetails.contains("""title="Scope key ${scopeKey} is optional to load""")
        }
        if (cubeNames)
        {
            assert nodeDetails.contains(cubeNames)
        }
    }

    protected static void checkScopePromptDropdown(Map node, String scopeKey, String selectedScopeValue, List<String> availableScopeValues, List<String> unavailableScopeValues, String valueClass = '', boolean isTopNode = false)
    {
        String nodeDetails = node.details as String
        String placeHolder = availableScopeValues ? SELECT_OR_ENTER_VALUE : ENTER_VALUE
        String topNodeClass = isTopNode ? DETAILS_CLASS_TOP_NODE : ''
        assert nodeDetails.contains("""<input id="${scopeKey}" value="${selectedScopeValue}" placeholder="${placeHolder}" class="scopeInput form-control ${valueClass} ${topNodeClass}""")
        if (!availableScopeValues && !unavailableScopeValues)
        {
            assert !nodeDetails.contains("""<li id="${scopeKey}:""")
            return
        }

        availableScopeValues.each{String scopeValue ->
            assert nodeDetails.contains("""<li id="${scopeKey}: ${scopeValue}" class="scopeClick ${topNodeClass}""")
        }
        unavailableScopeValues.each{String scopeValue ->
            assert !nodeDetails.contains("""<li id="${scopeKey}: ${scopeValue}" class="scopeClick ${topNodeClass}""")
        }
    }

    protected static void checkNoScopePrompt(Map node, String scopeKey = '')
    {
        String nodeDetails = node.details as String
        assert !nodeDetails.contains("""title="${scopeKey}""")
        assert !nodeDetails.contains("""<input id="${scopeKey}""")
        assert !nodeDetails.contains("""<li id="${scopeKey}""")
    }

    protected void preloadCubes()
    {
        preloadCubes(appId,
                'config/VisualizerConfig.json',
                'config/VisualizerConfig.NetworkOverrides.json',
                'config/VisualizerTypesToAdd.json'
        )
    }
}
