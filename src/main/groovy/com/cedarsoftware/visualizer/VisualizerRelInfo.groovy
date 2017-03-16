package com.cedarsoftware.visualizer

import com.cedarsoftware.ncube.ApplicationID
import com.cedarsoftware.ncube.Axis
import com.cedarsoftware.ncube.Column
import com.cedarsoftware.ncube.NCube
import com.cedarsoftware.ncube.NCubeRuntimeClient
import com.cedarsoftware.ncube.util.LongHashSet
import com.cedarsoftware.util.CaseInsensitiveMap
import groovy.transform.CompileStatic

import static com.cedarsoftware.visualizer.VisualizerConstants.SPACE
import static com.cedarsoftware.visualizer.VisualizerConstants.BREAK
import static com.cedarsoftware.visualizer.VisualizerConstants.DOUBLE_BREAK
import static com.cedarsoftware.visualizer.VisualizerConstants.RULE_NCUBE
import static com.cedarsoftware.visualizer.VisualizerConstants.NCUBE
import static com.cedarsoftware.visualizer.VisualizerConstants.DEFAULT
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_DEFAULT_VALUE
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_CELL_VALUES
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_EXPAND_ALL
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_COLLAPSE_ALL
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_SCOPE_CLICK
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_SCOPE_INPUT
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_FORM_CONTROL
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_TOP_NODE
import static com.cedarsoftware.visualizer.VisualizerConstants.DETAILS_CLASS_MISSING_VALUE

/**
 * Provides information to visualize a source cube, a target cube
 * and their relationship.
 */

@CompileStatic
class VisualizerRelInfo
{
	protected static NCubeRuntimeClient runtimeClient
	protected ApplicationID appId
	protected List<String> nodeDetailsMessages = []
	protected Map<String, Object> availableTargetScope = new CaseInsensitiveMap()
	Map<String, Set<Object>> availableScopeValues = new CaseInsensitiveMap()
	Map<String, Set<String>> scopeCubeNames = new CaseInsensitiveMap()
	protected boolean showingHidingCellValues
	protected long targetId
	protected NCube targetCube
	protected Map<String, Object> targetScope = new CaseInsensitiveMap()
	protected long targetLevel
	protected String nodeLabelPrefix = ''
	protected long sourceId
	protected NCube sourceCube
	protected Map<String, Object> sourceScope
	protected String sourceFieldName
	protected List<Long> sourceTrail = []
	protected boolean cubeLoaded
	protected boolean showCellValuesLink
	protected boolean showCellValues
	protected boolean loadAgain
	protected List<VisualizerCellInfo> cellInfo
	protected List<String> typesToAdd
	protected VisualizerHelper helper = new VisualizerHelper(runtimeClient, appId)

	VisualizerRelInfo() {}

	VisualizerRelInfo(NCubeRuntimeClient runtimeClient, ApplicationID applicationId)
	{
		this.runtimeClient = runtimeClient
		appId = applicationId
	}

	protected init(Map options, VisualizerInfo visInfo)
	{
		targetId = 1
		targetLevel = 1
		targetCube = runtimeClient.getCube(appId, options.startCubeName as String)
		addRequiredScopeKeys(visInfo)
		showCellValuesLink = true
		populateScopeDefaults(visInfo)
	}

	protected initSelectedNode(VisualizerInfo visInfo, Map selectedNode)
	{
		targetCube = runtimeClient.getCube(appId, selectedNode.cubeName as String)
		String sourceCubeName = selectedNode.sourceCubeName as String
		sourceCube = sourceCubeName ? runtimeClient.getCube(appId, sourceCubeName) : null
		sourceFieldName = selectedNode.fromFieldName as String
		sourceScope = selectedNode.sourceScope as CaseInsensitiveMap
		sourceTrail = selectedNode.sourceTrail as List
		sourceId = selectedNode.sourceId as Long
		targetId = Long.valueOf(selectedNode.id as String)
		targetLevel = Long.valueOf(selectedNode.level as String)
		showCellValues = selectedNode.showCellValues as boolean
		showCellValuesLink = selectedNode.showCellValuesLink as boolean
		cubeLoaded = selectedNode.cubeLoaded as boolean
		typesToAdd = selectedNode.typesToAdd as List
		visInfo.inputScope = new CaseInsensitiveMap(selectedNode.availableScope as Map)
		targetScope = selectedNode.scope as CaseInsensitiveMap ?:  new CaseInsensitiveMap()
		availableTargetScope = selectedNode.availableScope as CaseInsensitiveMap ?:  new CaseInsensitiveMap()
		availableScopeValues = selectedNode.availableScopeValues as CaseInsensitiveMap ?:  new CaseInsensitiveMap()
		showingHidingCellValues = selectedNode.showingHidingCellValues as boolean

		//If in the process of showing/hiding cell values, then the supplied scope, available scope values and scope
		//cube names are used to load cell values.
		if (showingHidingCellValues)
		{
			scopeCubeNames = selectedNode.scopeCubeNames as CaseInsensitiveMap ?:  new CaseInsensitiveMap()
		}
		//Else, node details are being loaded for the node or a scope change is being applied to the node. In this case,
		//clear out the supplied scope, available scope values and scope cube names. This will result in the node being
		//reloaded with updated scope information.
		else
		{
			availableTargetScope.keySet().removeAll(availableScopeValues.keySet())
			availableScopeValues = new CaseInsensitiveMap()
			scopeCubeNames = new CaseInsensitiveMap()
		}
		populateScopeDefaults(visInfo)
	}

	protected boolean loadCube(VisualizerInfo visInfo)
	{
		cellInfo = []
		cubeLoaded = true
		if (showCellValues)
		{
			Map<LongHashSet, Object> cellMap = targetCube.cellMap
			cellMap.each { LongHashSet ids, Object noExecuteCell ->
				Map<String, Object> coordinate = availableTargetScope as CaseInsensitiveMap ?: new CaseInsensitiveMap()
				coordinate.putAll(targetCube.getCoordinateFromIds(ids))
				VisualizerCellInfo visCellInfo = new VisualizerCellInfo(runtimeClient, appId, String.valueOf(targetId), coordinate)
				try
				{
					visCellInfo.cell = targetCube.getCell(coordinate)
				}
				catch (Exception e)
				{
					visCellInfo.exception = e
				}
				visCellInfo.noExecuteCell = noExecuteCell
				cellInfo << visCellInfo
			}
			cellInfo.sort { VisualizerCellInfo cellInfo ->
				cellInfo.coordinate.toString()
			}
		}
		return true
	}

	protected Set<String> getRequiredScope()
	{
		return targetCube.getRequiredScope(availableTargetScope, new CaseInsensitiveMap())
	}

	protected String getDetails(VisualizerInfo visInfo)
	{
		StringBuilder sb = new StringBuilder()

		getDetailsMap(sb, 'Scope', targetScope.sort())
		getDetailsMap(sb, 'Available scope', availableTargetScope.sort())
		getDetailsSet(sb, 'Axes', targetCube.axisNames)

		//Cell values
		if (cubeLoaded && showCellValues)
		{
			addCellValueSection(visInfo, sb)
		}
		return sb.toString()
	}

	private void addCellValueSection(VisualizerInfo visInfo, StringBuilder sb)
	{
		StringBuilder cellValuesBuilder = new StringBuilder()
		StringBuilder linkBuilder = new StringBuilder()
		sb.append("<b>Cell values</b>")
		getCellValues(visInfo, cellValuesBuilder, linkBuilder )
		sb.append(linkBuilder.toString())
		sb.append("""<pre><ul class="${DETAILS_CLASS_CELL_VALUES}">""")
		sb.append(cellValuesBuilder.toString())
		sb.append("</ul></pre>")
	}

	private void getCellValues(VisualizerInfo visInfo, StringBuilder cellValuesBuilder, StringBuilder linkBuilder)
	{
		Long id = 0l

		if (cellInfo)
		{
			cellInfo.each { VisualizerCellInfo visCellInfo ->
				visCellInfo.getCellValue(visInfo, this, id++, cellValuesBuilder)
			}

			linkBuilder.append(DOUBLE_BREAK)
			linkBuilder.append("""<a href="#" title="Expand all cell details" class="${DETAILS_CLASS_EXPAND_ALL}"">Expand all</a>""")
			linkBuilder.append("${SPACE}${SPACE}")
			linkBuilder.append("""<a href="#" title="Collapse all cell details" class="${DETAILS_CLASS_COLLAPSE_ALL}"">Collapse all</a>""")
			linkBuilder.append(BREAK)
		}
		else
		{
			cellValuesBuilder.append('none')
		}
	}

	protected static void getDetailsMap(StringBuilder sb, String title, Map<String, Object> map)
	{
		sb.append("<b>${title}</b>")
		sb.append("<pre><ul>")
		if (map)
		{
			map.each { String key, Object value ->
				sb.append("<li>${key}: ${value}</li>")
			}
		}
		else
		{
			sb.append("<li>none</li>")
		}
		sb.append("</ul></pre>${BREAK}")
	}

	protected static void getDetailsSet(StringBuilder sb, String title, Collection<String> set)
	{
		sb.append("<b>${title}</b>")
		sb.append("<pre><ul>")
		if (set)
		{
			set.each { String value ->
				sb.append("<li>${value}</li>")
			}
		}
		else
		{
			sb.append("<li>none</li>")
		}
		sb.append("</ul></pre>${BREAK}")
	}

	protected String getGroupName(VisualizerInfo visInfo = null, String cubeName = targetCube.name)
	{
		return targetCube.hasRuleAxis() ? RULE_NCUBE : NCUBE
	}

	protected static String getDotSuffix(String value)
	{
		int lastIndexOfDot = value.lastIndexOf('.')
		return lastIndexOfDot == -1 ? value : value.substring(lastIndexOfDot + 1)
	}

	protected static String getDotPrefix(String value) {
		int indexOfDot = value.indexOf('.')
		return indexOfDot == -1 ? value : value.substring(0, value.indexOf('.'))
	}

	/**
	 *  If the required scope keys have not already been loaded for this cube,
	 *  load them.
	 */
	protected void addRequiredScopeKeys(VisualizerInfo visInfo)
	{
		String cubeName = targetCube.name
		if (!visInfo.requiredScopeKeysByCube.containsKey(cubeName))
		{
			visInfo.requiredScopeKeysByCube[cubeName] = requiredScope
		}
	}

	protected Map<String, Object> createEdge(Long edgeId)
	{
		String sourceFieldName = sourceFieldName
		Map<String, Object> edge = [:]
		edge.id = edgeId
		edge.from = String.valueOf(sourceId)
		edge.to = String.valueOf(targetId)
		edge.fromName = getLabel(sourceCube.name)
		edge.toName = getLabel(targetCube.name)
		edge.fromFieldName = sourceFieldName
		edge.sourceTrail = sourceTrail
		edge.level = String.valueOf(targetLevel)
		edge.title = sourceFieldName
		return edge
	}

	protected Map<String, Object> createNode(VisualizerInfo visInfo, String group = null)
	{
		NCube targetCube = targetCube
		String targetCubeName = targetCube.name
		String sourceCubeName = sourceCube ? sourceCube.name : null
		String sourceFieldName = sourceFieldName

		Map<String, Object> node = [:]
		node.id = String.valueOf(targetId)
		node.level = String.valueOf(targetLevel)
		node.cubeName = targetCubeName
		node.sourceCubeName = sourceCubeName
		node.sourceTrail = sourceTrail
		node.sourceId = sourceId
		node.sourceScope = sourceScope
		node.scope = targetScope
		node.availableScope = availableTargetScope
		node.availableScopeValues = availableScopeValues
		node.scopeCubeNames = scopeCubeNames
		node.fromFieldName = sourceFieldName
		node.sourceDescription = sourceCubeName ? sourceDescription : null
		node.title = getCubeDisplayName(targetCubeName)
		group = group ?: getGroupName(visInfo)
		node.group = group
		node.showCellValuesLink = showCellValuesLink
		node.showCellValues = showCellValues
		node.cubeLoaded = cubeLoaded
		String label = getLabel(targetCubeName)
		node.label = nodeLabelPrefix + label
		node.detailsTitle1 = cubeDetailsTitle1
		node.detailsTitle2 = getCubeDetailsTitle2(label)
		if (node.detailsTitle1 == node.detailsTitle2){
			node.detailsTitle2 = null
		}
		node.typesToAdd = visInfo.getTypesToAdd(group)

		if (targetId == visInfo.selectedNodeId)
		{
			node.details = getDetails(visInfo)
		}

		visInfo.availableGroupsAllLevels << group - visInfo.groupSuffix
		long maxLevel = visInfo.maxLevel
		visInfo.maxLevel = maxLevel < targetLevel ? targetLevel : maxLevel
		return node
	}

	protected void populateScopeDefaults(VisualizerInfo visInfo) {}

	protected void addNodeScope(String cubeName, String scopeKey, boolean optional = false, boolean skipAvailableScopeValues = false, Map coordinate = null)
	{
		availableScopeValues = availableScopeValues ?: new CaseInsensitiveMap<String, Set<Object>>()
		scopeCubeNames = scopeCubeNames ?: new CaseInsensitiveMap<String, Set<String>>()
		addAvailableValues(cubeName, scopeKey, optional, skipAvailableScopeValues, coordinate)
		addCubeNames(scopeKey, cubeName)
	}

	private void addAvailableValues(String cubeName, String scopeKey, boolean optional, boolean skipAvailableScopeValues, Map coordinate)
	{
		Set<Object> scopeValues = availableScopeValues[scopeKey] as Set ?: new LinkedHashSet()
		if (skipAvailableScopeValues)
		{
			availableScopeValues[scopeKey] = scopeValues
		}
		else
		{
			Set scopeValuesThisCube = getColumnValues(cubeName, scopeKey, coordinate)
			if (availableScopeValues.containsKey(scopeKey))
			{
				if (optional)
				{
					availableScopeValues[scopeKey].addAll(scopeValuesThisCube)
				}
				else
				{
					availableScopeValues[scopeKey] = scopeValues.intersect(scopeValuesThisCube) as Set
				}
			}
			else
			{
				availableScopeValues[scopeKey] = scopeValuesThisCube
			}
		}
	}

	protected void addCubeNames(String scopeKey, String valueToAdd)
	{
		Set<String> values = scopeCubeNames[scopeKey] as Set ?: new LinkedHashSet()
		values << valueToAdd
		scopeCubeNames[scopeKey] = values
	}

	protected Set<Object> getColumnValues(String cubeName, String axisName, Map coordinate)
	{
		NCube cube = runtimeClient.getCube(appId, cubeName)
		return getAllColumnValues(cube, axisName)
	}

	protected static Set<Object> getAllColumnValues(NCube cube, String axisName)
	{
		Set values = new LinkedHashSet()
		Axis axis = cube.getAxis(axisName)
		if (axis)
		{
			for (Column column : axis.columns)
			{
				values.add(column.value)
			}
		}
		return values
	}

	protected String createNodeDetailsScopeMessage()
	{
		availableScopeValues =  availableScopeValues ?: new CaseInsensitiveMap<String, Set<Object>> ()
		scopeCubeNames = scopeCubeNames ?: new CaseInsensitiveMap<String, Set<String>> ()

		StringBuilder sb = new StringBuilder()
		sb.append(nodeDetailsMessageSet)
		sb.append(nodeScopeMessage)
		return sb.toString()
	}

	private StringBuilder getNodeDetailsMessageSet()
	{
		StringBuilder sb = new StringBuilder()
		if (nodeDetailsMessages)
		{
			nodeDetailsMessages.each { String message ->
				sb.append("${message}")
			}
		}
		return sb
	}

	private StringBuilder getNodeScopeMessage()
	{
		String nodeName = getLabel()
		StringBuilder sb = new StringBuilder()
		if (availableScopeValues)
		{
			Map sortedMap = availableScopeValues.sort()
			sortedMap.keySet().each { String scopeKey ->
				Set<String> cubeNames = scopeCubeNames[scopeKey]
				cubeNames.remove(null)
				Set<Object> availableValues = availableScopeValues[scopeKey]
				String requiredOrOptional = availableValues.contains(null) ? 'optional' : 'required'
				StringBuilder title = new StringBuilder("Scope key ${scopeKey} is ${requiredOrOptional} to load ${nodeName}")
				title.append(addCubeNamesList('. First encountered on the following cubes, but may also be present on others:', cubeNames))
				sb.append(getScopeMessage(scopeKey, availableValues, title, availableTargetScope[scopeKey]))
				sb.append(BREAK)
			}
		}
		else{
			sb.append("<b>No scope</b>")
			sb.append(BREAK)
		}
		return sb
	}

	protected StringBuilder getScopeMessage(String scopeKey, Set<Object> availableScopeValues, StringBuilder title, Object providedScopeValue)
	{
		StringBuilder sb = new StringBuilder()
		String caret = availableScopeValues ? """<span class="caret"></span>""" : ''
		String placeHolder = availableScopeValues ? 'Select or enter value...' : 'Enter value...'
		String topNodeClass = targetId == 1l ? DETAILS_CLASS_TOP_NODE : ''
		Object value = getValue(availableScopeValues, providedScopeValue)
		String valueClass = getClassForValue(availableScopeValues, value, providedScopeValue)

		sb.append("""<div class="input-group" title="${title}">""")
		sb.append("""<div class="input-group-btn">""")
		sb.append("""<button type="button" class="btn btn-default dropdown-toggle"  data-toggle="dropdown">${scopeKey} ${caret}</button>""")
		if (availableScopeValues)
		{
			sb.append("""<ul class="dropdown-menu">""")
			availableScopeValues.each {Object scopeValue ->
				if (scopeValue)
				{
					sb.append("""<li id="${scopeKey}: ${scopeValue}" class="${DETAILS_CLASS_SCOPE_CLICK} ${topNodeClass}" style="color: black;">${scopeValue}</li>""")
				}
				else
				{
					sb.append("""<li id="${scopeKey}: Default" class="${DETAILS_CLASS_SCOPE_CLICK} ${topNodeClass}" style="color: black;">Default</li>""")
				}
			}
			sb.append("""</ul>""")
		}
		sb.append("""</div>""")
		sb.append("""<input id="${scopeKey}" value="${value}" placeholder="${placeHolder}" class="${DETAILS_CLASS_SCOPE_INPUT} ${DETAILS_CLASS_FORM_CONTROL} ${valueClass} ${topNodeClass}" style="color: black;" type="text">""")
		sb.append("""</div>""")
		return sb
	}

	private static Object getValue(Set<Object> availableScopeValues, Object providedScopeValue)
	{
		if (availableScopeValues.contains(null))
		{
			return providedScopeValue ?: DEFAULT
		}
		else
		{
			return providedScopeValue ?: ''
		}
	}

	private static String getClassForValue(Set<Object> availableScopeValues, Object value, Object providedScopeValue)
	{
		if (DEFAULT == value && availableScopeValues.contains(null))
		{
			return DETAILS_CLASS_DEFAULT_VALUE
		}
		else if (!providedScopeValue)
		{
			return DETAILS_CLASS_MISSING_VALUE
		}
		else if (providedScopeValue && availableScopeValues && !availableScopeValues.contains(providedScopeValue))
		{
			return DETAILS_CLASS_MISSING_VALUE
		}
		else
		{
			return ''
		}
	}

	protected void setLoadAgain(VisualizerInfo visInfo, String scopeKey)
	{
		loadAgain = false
	}

	private static StringBuilder addCubeNamesList(String prefix, Set<String> cubeNames)
	{
		StringBuilder sb = new StringBuilder()
		if (cubeNames)
		{
			sb.append("${prefix}\n")
			cubeNames.each { String cubeName ->
				sb.append("  - ${cubeName}\n")
			}
		}
		return sb
	}

	protected boolean includeUnboundScopeKey(VisualizerInfo visInfo, String scopeKey)
	{
		return true
	}

	protected String getLabel(String cubeName = targetCube.name)
	{
		cubeName
	}

	protected String getCubeDisplayName(String cubeName)
	{
		return cubeName
	}

	protected String getSourceDescription()
	{
		return sourceCube.name
	}

	protected String getCubeDetailsTitle1()
	{
		return targetCube.name
	}

	protected String getCubeDetailsTitle2(String label)
	{
		return null
	}
}