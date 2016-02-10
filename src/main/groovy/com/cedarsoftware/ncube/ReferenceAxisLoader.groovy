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
    public static final String SOURCE_TENANT = 'sourceTenant'
    public static final String SOURCE_APP = 'sourceApp'
    public static final String SOURCE_VERSION = 'sourceVersion'
    public static final String SOURCE_STATUS = 'sourceStatus'
    public static final String SOURCE_BRANCH = 'sourceBranch'
    public static final String SOURCE_CUBE_NAME = 'sourceCubeName'
    public static final String SOURCE_AXIS_NAME = 'sourceAxisName'

    public static final String TRANSFORM_APP = 'transformApp'
    public static final String TRANSFORM_VERSION = 'transformVersion'
    public static final String TRANSFORM_STATUS = 'transformStatus'
    public static final String TRANSFORM_BRANCH = 'transformBranch'
    public static final String TRANSFORM_CUBE_NAME = 'transformCubeName'
    public static final String TRANSFORM_METHOD_NAME = 'transformMethodName'

    private String cubeName
    private String axisName
    private Map refAxisArgs

    /**
     * @param containingCubeName String name of cube that holds the 'pointer' (referring) axis
     * @param sourceAxisName String name of the referring axis
     * @param refAxisArgs Map containing all the key-value pairs to describe the referenced n-cube + axis
     * and an optional reference to a transformation cube.<br>
     * required keys: sourceTenant, sourceApp, sourceVersion, sourceStatus, sourceBranch, sourceCubeName, sourceAxisName
     * optional keys (transformer): transformApp, transformVersion, transformStatus, transformBranch, transformCubeName, transformMethodName
     */
    ReferenceAxisLoader(String containingCubeName, String sourceAxisName, Map<String , Object> refAxisArgs)
    {
        cubeName = containingCubeName
        axisName = sourceAxisName
        this.refAxisArgs = refAxisArgs
    }

    /**
     * Axis calls this method to load itself from this AxisRefProvider.
     * @param axis Axis to 'fill-up' from the reference and 'transform' from the optional transformer.
     */
    void load(Axis axis)
    {
        String srcTenant = refAxisArgs[SOURCE_TENANT]
        String srcApp = refAxisArgs[SOURCE_APP]
        String srcVer = refAxisArgs[SOURCE_VERSION]
        String srcStatus = refAxisArgs[SOURCE_STATUS]
        String srcBranch = refAxisArgs[SOURCE_BRANCH]
        String srcCubeName = refAxisArgs[SOURCE_CUBE_NAME]
        String srcAxisName = refAxisArgs[SOURCE_AXIS_NAME]

        axis.setSourceAppId(new ApplicationID(srcTenant, srcApp, srcVer, srcStatus, srcBranch))
        axis.setSourceCubeName(srcCubeName)
        axis.setSourceAxisName(srcAxisName)

        String transformApp = refAxisArgs[TRANSFORM_APP]
        String transformVer = refAxisArgs[TRANSFORM_VERSION]
        String transformStatus = refAxisArgs[TRANSFORM_STATUS]
        String transformBranch = refAxisArgs[TRANSFORM_BRANCH]
        String transformCubeName = refAxisArgs[TRANSFORM_CUBE_NAME]
        String transformMethodName = refAxisArgs[TRANSFORM_METHOD_NAME]

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
        metaProps.remove("name")
        metaProps.remove("isRef")
        metaProps.remove("hasDefault")
        metaProps.remove(SOURCE_TENANT)
        metaProps.remove(SOURCE_APP)
        metaProps.remove(SOURCE_VERSION)
        metaProps.remove(SOURCE_STATUS)
        metaProps.remove(SOURCE_BRANCH)
        metaProps.remove(SOURCE_CUBE_NAME)
        metaProps.remove(SOURCE_AXIS_NAME)
        metaProps.remove(TRANSFORM_APP)
        metaProps.remove(TRANSFORM_VERSION)
        metaProps.remove(TRANSFORM_STATUS)
        metaProps.remove(TRANSFORM_BRANCH)
        metaProps.remove(TRANSFORM_CUBE_NAME)
        metaProps.remove(TRANSFORM_METHOD_NAME)

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
        input.put("columns", refAxis.getColumnsWithoutDefault())
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
