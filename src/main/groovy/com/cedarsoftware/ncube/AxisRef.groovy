package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * Implements an Axis of an NCube. When modeling, think of an axis as a 'condition'
 * or decision point.  An input variable (like 'X:1' in a cartesian coordinate system)
 * is passed in, and the Axis's job is to locate the column that best matches the input,
 * as quickly as possible.
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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class AxisRef implements Axis.AxisRefProvider
{
    private ApplicationID appId
    private final String cubeName
    private final String axisName
    private final String transformCube

    AxisRef(ApplicationID appId, String cubeName, String axisName, String transformCube)
    {
        this.appId = appId
        this.cubeName = cubeName
        this.axisName = axisName
        this.transformCube = transformCube
    }

    void load(Axis axis)
    {
        // Fetch the axis from NCubeManager.
        // Stuff values into the Axis
    }
}
