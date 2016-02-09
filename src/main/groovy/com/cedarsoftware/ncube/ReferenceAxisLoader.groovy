package com.cedarsoftware.ncube

import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.StringUtilities
import groovy.transform.CompileStatic

/**
 * This class loads an reference axis.  This entails loading the cube containing
 * the referenced axis, grabbing the axis columns from this cube, passing the
 * columns to an [optional] transformer cube, and then ultimately building the
 * axis from these columns.
 *
 * The source axis type, valueType, and meta properties are maintained.  Any
 * meta-properties on the referencing axis, are added to the meta-properties
 * from the referenced axis.  If both have the same key, the referring axis
 * meta-properties take priority.
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
class ReferenceAxisLoader implements Axis.AxisRefProvider
{
    private String cubeName
    private String axisName
    private Map refAxisArgs

    ReferenceAxisLoader(String containingCubeName, String sourceAxisName, Map<String , Object> refAxisArgs)
    {
        cubeName = containingCubeName
        axisName = sourceAxisName
        this.refAxisArgs = refAxisArgs
    }

    void load(Axis axis)
    {
        String srcTenant = refAxisArgs.sourceTenant
        String srcApp = refAxisArgs.sourceApp
        String srcVer = refAxisArgs.sourceVersion
        String srcStatus = refAxisArgs.sourceStatus
        String srcBranch = refAxisArgs.sourceBranch
        String srcCubeName = refAxisArgs.sourceCubeName
        String srcAxisName = refAxisArgs.sourceAxisName

        axis.setSourceAppId(new ApplicationID(srcTenant, srcApp, srcVer, srcStatus, srcBranch))
        axis.setSourceCubeName(srcCubeName)
        axis.setSourceAxisName(srcAxisName)

        String transformApp = refAxisArgs.transformApp
        String transformVer = refAxisArgs.transformVersion
        String transformStatus = refAxisArgs.transformStatus
        String transformBranch = refAxisArgs.transformBranch
        String transformCubeName = refAxisArgs.transformCubeName
        String transformMethodName = refAxisArgs.transformMethodName

        boolean hasTransformer = StringUtilities.hasContent(transformApp) &&
                StringUtilities.hasContent(transformVer) &&
                StringUtilities.hasContent(transformStatus) &&
                StringUtilities.hasContent(transformBranch)

        if (hasTransformer)
        {
            axis.setTransformAppId(new ApplicationID(srcTenant, transformApp, transformVer, transformStatus, transformBranch))
            axis.setTransformCubeName(transformCubeName)
            axis.setTransformMethodName(transformMethodName)
        }

        Map<String, Object> metaProps = new CaseInsensitiveMap<>()
        metaProps.putAll(refAxisArgs)
        metaProps.remove("isRef")
        metaProps.remove("hasDefault")
        metaProps.remove("sourceTenant")
        metaProps.remove("sourceApp")
        metaProps.remove("sourceVersion")
        metaProps.remove("sourceStatus")
        metaProps.remove("sourceBranch")
        metaProps.remove("sourceCubeName")
        metaProps.remove("sourceAxisName")
        metaProps.remove("transformApp")
        metaProps.remove("transformVersion")
        metaProps.remove("transformStatus")
        metaProps.remove("transformBranch")
        metaProps.remove("transformCubeName")
        metaProps.remove("transformMethodName")

        NCube sourceCube = NCubeManager.getCube(axis.getSourceAppId(), axis.getSourceCubeName())
        if (sourceCube == null)
        {
            throw new IllegalStateException("Unable to load cube: " + cubeName +
                    " which has a reference axis, failed to load referenced cube: " + srcCubeName + ", axis: " + srcAxisName +
                    ", source app: " + axis.getSourceAppId())
        }

        NCube transformCube = null;
        if (hasTransformer)
        {
            transformCube = NCubeManager.getCube(axis.getTransformAppId(), axis.getTransformCubeName())
            if (transformCube == null)
            {
                throw new IllegalStateException("Unable to load cube: " + cubeName +
                        " which has a reference axis, failed to load transform cube: " + transformCubeName + ", method: " +
                        transformMethodName + ", source app: " + axis.getTransformAppId())
            }
        }

        Axis refAxis = sourceCube.getAxis(srcAxisName)
        if (refAxis == null)
        {
            throw new IllegalStateException("Unable to load cube: " + cubeName +
                    ", The reference axis: " + srcAxisName + " was not found on the referenced cube: " +
                    srcCubeName + ", in app: " + axis.getSourceAppId())
        }

        Map<String, Object> input = new CaseInsensitiveMap<>()
        input.put("columns", refAxis.getColumns())
        Map<String, Object> output = new CaseInsensitiveMap<>()
        if (transformCube != null)
        {   // Allow this cube to manipulate the passed in Axis.
            transformCube.getCell(input, output)
        }
        else
        {
            output.put("columns", input.get("columns"))
        }

        axis.setName(axisName)
        axis.type = refAxis.type;
        axis.valueType = refAxis.valueType;
        axis.fireAll = refAxis.fireAll;

        // Bring over referenced axis meta properties
        for (Map.Entry<String, Object> entry : refAxis.getMetaProperties().entrySet())
        {
            axis.setMetaProperty(entry.getKey(), entry.getValue())
        }

        // Allow meta properties for reference axis - these take priority (override)
        // any meta-properties on the referenced axis.
        for (Map.Entry<String, Object> entry : metaProps.entrySet())
        {
            axis.setMetaProperty(entry.getKey(), entry.getValue())
        }

        // Bring over columns
        List<Column> columns = (List<Column>) output.get("columns")
        for (Column column : columns)
        {
            axis.addColumn(column.getValue(), column.getColumnName(), column.id)
        }
    }
}
