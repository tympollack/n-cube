package com.cedarsoftware.util

import groovy.transform.CompileStatic


/**
 * Provides information about the scope used to visualize an n-cube.
 */

@CompileStatic
class VisualizerScopeInfo
{
	Map<String, Set<Object>> columnValuesForUnboundAxes = new CaseInsensitiveMap()
	Map<String, Set<Object>> unboundValuesForUnboundAxes = new CaseInsensitiveMap()
	Map<String, Set<String>> cubeNamesForUnboundAxes = new CaseInsensitiveMap()
	Set<String> axisNames = new CaseInsensitiveSet()
}