package com.cedarsoftware.ncube
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.fail
/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestDelta
{
    @Before
    public void setUp()
    {
        TestingDatabaseHelper.setupDatabase()
    }

    @After
    public void tearDown()
    {
        TestingDatabaseHelper.tearDownDatabase()
    }

    @Test
    void testDeltaApis()
    {
        Delta x = new Delta(Delta.Location.AXIS, Delta.Type.ADD, "foo")
        assert x.location == Delta.Location.AXIS
        assert x.type == Delta.Type.ADD
        assert x.description == 'foo'
        assert x.toString() == 'foo'
    }

    @Test
    void testBadInputToChangeSetComparator()
    {
        assert !NCube.areDeltaSetsCompatible(null, [:])
        assert !NCube.areDeltaSetsCompatible([:], null)
    }

    @Test
    void testDiscreteMergeRemoveCol()
    {
        // Delete a column from cube 1, which not only deletes the column, but also the cells pointing to it.
        NCube cube1 = getTestCube()
        NCube cube2 = getTestCube()
        NCube orig = getTestCube()

        assert '4' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'OH', rule:'process'] as Map)
        assert '5' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'GA', rule:'process'] as Map)
        assert '6' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'TX', rule:'process'] as Map)

        assert '46' == getCellIgnoreRule(cube1, [age:20, salary:85000, log:1000, state:'OH', rule:'process'] as Map)
        assert '47' == getCellIgnoreRule(cube1, [age:20, salary:85000, log:1000, state:'GA', rule:'process'] as Map)
        assert '48' == getCellIgnoreRule(cube1, [age:20, salary:85000, log:1000, state:'TX', rule:'process'] as Map)

        // Verify deletion occurred
        int count = cube1.cellMap.size()
        assert count == 48
        cube1.deleteColumn('state', 'GA')
        assert cube1.cellMap.size() == 32
        assert cube2.cellMap.size() == 48

        // Compute delta between copy of original cube and the cube with deleted column.
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 32

        count = cube2.getAxis("state").getColumns().size()
        assert cube2.getAxis('state').findColumn('OH') != null
        assert cube2.getAxis('state').findColumn('TX') != null
        assert cube2.getAxis('state').findColumn('GA') == null
        assert count == 2

        assert '4' == getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'OH', rule:'process'] as Map)
        assert '6' == getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'TX', rule:'process'] as Map)
        try
        {
            getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'GA', rule:'process'] as Map)
            fail()
        }
        catch (CoordinateNotFoundException ignored)
        { }

        assert '46' == getCellIgnoreRule(cube2, [age:20, salary:85000, log:1000, state:'OH', rule:'process'] as Map)
        assert '48' == getCellIgnoreRule(cube2, [age:20, salary:85000, log:1000, state:'TX', rule:'process'] as Map)
        try
        {
            assert '47' == getCellIgnoreRule(cube2, [age:20, salary:85000, log:1000, state:'GA', rule:'process'] as Map)
        }
        catch (CoordinateNotFoundException ignored)
        { }
    }

    @Test
    void testDiscreteMergeAddCol()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        // Verify addition occurred
        int count = cube1.cellMap.size()
        assert count == 48
        cube1.addColumn('state', 'AL')

        assert cube1.cellMap.size() == 48
        Map coord = [age: 16, salary: 60000, log: 1000, state: 'AL', rule: 'process'] as Map
        cube1.setCell('foo', coord)
        assert cube1.cellMap.size() == 49
        assert 'foo' == getCellIgnoreRule(cube1, coord)

        // Compute delta between copy of original cube and the cube with deleted column.
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)  // Other guy made no changes

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 49

        assert 'foo' == getCellIgnoreRule(cube2, [age: 16, salary: 60000, log: 1000, state: 'AL', rule:'process'] as Map)
    }

    @Test
    void testRuleMergeRemoveColumnWithName()
    {
        // Delete a column from cube 1, which not only deletes the column, but also the cells pointing to it.
        NCube cube1 = getTestCube()
        NCube cube2 = getTestCube()
        NCube orig = getTestCube()

        assert '4' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'OH', rule:'process'] as Map)
        assert '5' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'GA', rule:'process'] as Map)
        assert '6' == getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'TX', rule:'process'] as Map)

        // Verify deletion occurred
        int count = cube1.cellMap.size()
        assert count == 48
        cube1.deleteColumn('rule', 'process')
        assert cube1.cellMap.size() == 24
        assert cube2.cellMap.size() == 48

        // Compute delta between copy of original cube and the cube with deleted column.
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)  // Other guy made no changes

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 24

        count = cube2.getAxis("rule").getColumns().size()
        assert count == 1
        assert cube2.getAxis('rule').findColumn('init') != null
        assert cube2.getAxis('rule').findColumn('process') == null

        try
        {
            getCellIgnoreRule(cube1, [age:16, salary:65000, log:100, state:'OH', rule:'process'] as Map)
            fail()
        }
        catch (CoordinateNotFoundException ignored)
        { }

        assert '1' == getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'OH', rule:'init'] as Map)
        assert '2' == getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'GA', rule:'init'] as Map)
        assert '3' == getCellIgnoreRule(cube2, [age:16, salary:65000, log:100, state:'TX', rule:'init'] as Map)
    }

    @Test
    void testRuleMergeAddColumnWithName()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        GroovyExpression exp = new GroovyExpression("false", null, false)
        cube1.addColumn('rule', exp, 'jones')
        Map coord = [age:16, salary:65000, log:100, state:'OH', rule:'jones'] as Map
        cube1.setCell('alpha', coord)
        assert cube1.cellMap.size() == 49

        // Compute delta between copy of original cube and the cube with deleted column.
        // Apply this delta to the 2nd cube to force the same changes on it.
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)  // Other guy made no changes

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        assert cube2.cellMap.size() == 48
        try
        {
            cube2.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e)
        { }
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 49
        assert 'alpha' == getCellIgnoreRule(cube2, coord)
    }

    @Test
    void testRuleMergeAddColumnWithoutName()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        int numCols = cube1.getAxis('rule').getColumns().size()
        cube1.addColumn('rule', 'true')

        // Compute delta between copy of original cube and the cube with deleted column.
        // Apply this delta to the 2nd cube to force the same changes on it.
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)  // Other guy made no changes

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis axis = cube2.getAxis('rule')
        assert axis.getColumns().size() == numCols + 1
    }

    @Test
    void testDiscreteMergeAddAddUniqueColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        // Verify addition occurred
        int count = cube1.cellMap.size()
        assert count == 48
        cube1.addColumn('state', 'AL')

        Map coord = [age: 16, salary: 60000, log: 1000, state: 'AL', rule: 'process'] as Map
        cube1.setCell('foo', coord)
        assert cube1.cellMap.size() == 49
        assert 'foo' == getCellIgnoreRule(cube1, coord)

        count = cube2.cellMap.size()
        assert count == 48
        cube2.addColumn('state', 'WY')

        coord = [age: 16, salary: 60000, log: 1000, state: 'WY', rule: 'process'] as Map
        cube2.setCell('bar', coord)
        assert cube2.cellMap.size() == 49
        assert 'bar' == getCellIgnoreRule(cube2, coord)

        // Compute delta between copy of original cube
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube1)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 50

        assert 'foo' == getCellIgnoreRule(cube2, [age: 16, salary: 60000, log: 1000, state: 'AL', rule: 'process'] as Map)
        assert 'bar' == getCellIgnoreRule(cube2, [age: 16, salary: 60000, log: 1000, state: 'WY', rule: 'process'] as Map)
    }

    @Test
    void testDiscreteMergeAddAddSameColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        // Verify addition occurred
        int count = cube1.cellMap.size()
        assert count == 48
        cube1.addColumn('state', 'AL')

        Map coord = [age: 16, salary: 60000, log: 1000, state: 'AL', rule: 'process'] as Map
        cube1.setCell('foo', coord)
        assert cube1.cellMap.size() == 49
        assert 'foo' == getCellIgnoreRule(cube1, coord)

        count = cube2.cellMap.size()
        assert count == 48
        cube2.addColumn('state', 'AL')

        Map coord2 = [age: 16, salary: 60000, log: 1000, state: 'AL', rule: 'init'] as Map
        cube2.setCell('bar', coord2)
        assert cube2.cellMap.size() == 49
        assert 'bar' == getCellIgnoreRule(cube2, coord2)

        // Compute delta between copy of original cube
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube1)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        assert cube2.cellMap.size() == 50

        assert 'foo' == getCellIgnoreRule(cube2, coord)
        assert 'bar' == getCellIgnoreRule(cube2, coord2)
    }

    @Test
    void testDiscreteMergeRemoveRemoveUniqueColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        cube1.deleteColumn('state', 'OH')
        assert cube1.cellMap.size() == 32

        assert cube2.cellMap.size() == 48
        cube2.deleteColumn('state', 'GA')
        assert cube2.cellMap.size() == 32

        // Compute delta between copy of original cube
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis state = cube2.getAxis('state')
        assertNotNull state.findColumn('TX')
        assertNull state.findColumn('OH')
        assertNull state.findColumn('GA')
        assert cube2.cellMap.size() == 16
    }

    @Test
    void testDiscreteMergeRemoveRemoveSameColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        cube1.deleteColumn('state', 'OH')
        assert cube1.cellMap.size() == 32

        assert cube2.cellMap.size() == 48
        cube2.deleteColumn('state', 'OH')
        assert cube2.cellMap.size() == 32

        // Compute delta between copy of original cube
        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis state = cube2.getAxis('state');
        assertNotNull state.findColumn('TX')
        assertNotNull state.findColumn('GA')
        assertNull state.findColumn('OH')
        assert cube2.cellMap.size() == 32
    }

    @Test
    void testRuleMergeAddAddUniqueColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        assert cube2.cellMap.size() == 48
        cube1.addColumn('rule', 'true', 'Summary')
        Map coord = [age: 16, salary: 60000, log: 1000, state: 'OH', rule: 'Summary'] as Map
        cube1.setCell('99', coord)

        cube2.addColumn('rule', '1 < 2', 'Finalize')
        coord.rule = 'Finalize'
        cube2.setCell('88', coord)

        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis rule = cube2.getAxis('rule')
        assert rule.size() == 4
        coord = [age: 16, salary: 60000, log: 1000, state: 'OH', rule: 'Summary'] as Map
        assert '99' == getCellIgnoreRule(cube2, coord)
        coord.rule = 'Finalize'
        assert '88' == getCellIgnoreRule(cube2, coord)
        assert cube2.getNumCells() == 50
    }

    @Test
    void testRuleMergeAddAddSameColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        assert cube2.cellMap.size() == 48
        cube1.addColumn('rule', 'true', 'Summary')
        Map coord = [age: 16, salary: 60000, log: 1000, state: 'OH', rule: 'Summary'] as Map
        cube1.setCell('99', coord)

        cube2.addColumn('rule', '1 < 2', 'Finalize')
        coord.rule = 'Finalize'
        cube2.setCell('88', coord)

        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis rule = cube2.getAxis('rule')
        assert rule.size() == 4
        coord = [age: 16, salary: 60000, log: 1000, state: 'OH', rule: 'Summary'] as Map
        assert '99' == getCellIgnoreRule(cube2, coord)
        coord.rule = 'Finalize'
        assert '88' == getCellIgnoreRule(cube2, coord)
        assert cube2.getNumCells() == 50
    }

    @Test
    void testRuleMergeAddAddSameColumnConflict()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        assert cube1.cellMap.size() == 48
        assert cube2.cellMap.size() == 48
        cube1.addColumn('rule', 'true', 'Summary')
        Map coord = [age: 16, salary: 60000, log: 1000, state: 'OH', rule: 'Summary'] as Map
        cube1.setCell('99', coord)

        cube2.addColumn('rule', '1 < 2', 'Summary')
        coord.rule = 'Summary'
        cube2.setCell('99', coord)

        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert !compatibleChange
    }

    @Test
    void testRuleMergeRemoveRemoveUniqueColumn()
    {
        NCube<String> cube1 = (NCube<String>) getTestCube()
        NCube<String> cube2 = (NCube<String>) getTestCube()
        NCube<String> orig = (NCube<String>) getTestCube()

        cube1.deleteColumn('rule', 'init')
        cube2.deleteColumn('rule', 'process')

        Map<String, Object> delta1 = orig.getDelta(cube1)
        Map<String, Object> delta2 = orig.getDelta(cube2)

        boolean compatibleChange = NCube.areDeltaSetsCompatible(delta1, delta2)
        assert compatibleChange
        cube2.mergeDeltaSet(delta1)
        Axis rule = cube2.getAxis('rule')
        assert rule.size() == 0
    }

    @Test
    void testRuleMergeRemoveRemoveSameColumn()
    {
    }

    @Test
    void testRuleMergeAddRemoveSameColumnConflict()
    {
    }

    @Test
    void testDiscreteRangeAddColumnBoth()
    {   // Change 2 axes
    }

    @Test
    void testDiscreteRangeRemoveColumnBoth()
    {   // Change 2 axes
    }

    @Test
    void testRuleMergeAddColumnWithNoName()
    {
    }

    @Test
    void testRuleMergeRemoveColumnWithNoName()
    {
    }

    def getCellIgnoreRule(NCube ncube, Map coord)
    {
        Set<Long> idCoord = ncube.getCoordinateKey(coord)
        return ncube.getCellById(idCoord, coord, [:])
    }

    NCube getTestCube()
    {
        return NCube.fromSimpleJson(getTestCubeJson())
    }

    String getTestCubeJson()
    {
        return '''{
  "ncube": "testMerge",
  "axes": [
    {
      "name": "Age",
      "type": "RANGE",
      "valueType": "LONG",
      "preferredOrder": 0,
      "hasDefault": false,
      "fireAll": true,
      "columns": [
        {
          "id": 1000000000001,
          "value": [
            16,
            18
          ]
        },
        {
          "id": 1000000000002,
          "value": [
            18,
            22
          ]
        }
      ]
    },
    {
      "name": "Salary",
      "type": "SET",
      "valueType": "LONG",
      "preferredOrder": 0,
      "hasDefault": false,
      "fireAll": true,
      "columns": [
        {
          "id": 2000000000001,
          "value": [
            [
              60000,
              75000
            ]
          ]
        },
        {
          "id": 2000000000002,
          "value": [
            [
              75000,
              100000
            ]
          ]
        }
      ]
    },
    {
      "name": "Log",
      "type": "NEAREST",
      "valueType": "LONG",
      "preferredOrder": 0,
      "hasDefault": false,
      "fireAll": true,
      "columns": [
        {
          "id": 3000000000001,
          "type": "long",
          "value": 100
        },
        {
          "id": 3000000000002,
          "type": "long",
          "value": 1000
        }
      ]
    },
    {
      "name": "rule",
      "type": "RULE",
      "valueType": "EXPRESSION",
      "preferredOrder": 1,
      "hasDefault": false,
      "fireAll": true,
      "columns": [
        {
          "id": 4000000000001,
          "type": "exp",
          "name": "init",
          "value": "true"
        },
        {
          "id": 4000000000002,
          "type": "exp",
          "name": "process",
          "value": "true"
        }
      ]
    },
    {
      "name": "State",
      "type": "DISCRETE",
      "valueType": "STRING",
      "preferredOrder": 1,
      "hasDefault": false,
      "fireAll": true,
      "columns": [
        {
          "id": 5000000000002,
          "type": "string",
          "value": "GA"
        },
        {
          "id": 5000000000001,
          "type": "string",
          "value": "OH"
        },
        {
          "id": 5000000000003,
          "type": "string",
          "value": "TX"
        }
      ]
    }
  ],
  "cells": [
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "1"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "2"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "3"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "4"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "5"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "6"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "7"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "8"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "9"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "10"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "11"
    },
    {
      "id": [
        1000000000001,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "12"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "13"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "14"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "15"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "16"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "17"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "18"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "19"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "20"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "21"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "22"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "23"
    },
    {
      "id": [
        1000000000001,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "24"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "25"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "26"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "27"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "28"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "29"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000001,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "30"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "31"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "32"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "33"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "34"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "35"
    },
    {
      "id": [
        1000000000002,
        2000000000001,
        3000000000002,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "36"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "37"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "38"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "39"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "40"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "41"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000001,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "42"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000001
      ],
      "type": "string",
      "value": "43"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000002
      ],
      "type": "string",
      "value": "44"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000001,
        5000000000003
      ],
      "type": "string",
      "value": "45"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000001
      ],
      "type": "string",
      "value": "46"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000002
      ],
      "type": "string",
      "value": "47"
    },
    {
      "id": [
        1000000000002,
        2000000000002,
        3000000000002,
        4000000000002,
        5000000000003
      ],
      "type": "string",
      "value": "48"
    }
  ]
}'''

    }
}
