package com.cedarsoftware.ncube

import com.cedarsoftware.util.TrackingMap
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test

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
class TestInputKeysUsed
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
    void testKeyTracking()
    {
        NCube ncube = NCubeBuilder.getTrackingTestCube()
        Map input = [column: 'A', Age: 7]
        Map output = [:]
        def x = ncube.getCell(input, output)
        assert 'a1' == x
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 2
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        Set keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('Row')
    }

    @Test
    void testKeyTrackingWithContainsKeyAccess()
    {
        NCube ncube = NCubeBuilder.getTrackingTestCube()
        Map input = [column: 'B']
        Map output = [:]
        ncube.getCell(input, output)
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 3
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('smokes')
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        Set keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('Row')


        input = [column: 'B', Row: 99]
        output = [:]
        ncube.getCell(input, output)
        ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 3
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('smokes')
        assert ruleInfo.getDefaultKeysUsed().size() == 0

        input = [column: 'B', Row: 99, SMOKES: null]
        output = [rate: 0]
        ncube.getCell(input, output)
        ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 3
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('smokes')
        assert ruleInfo.getDefaultKeysUsed().size() == 0
    }

    @Test
    void testKeyTrackingInputAccessedInCode()
    {
        NCube ncube = NCubeBuilder.getTrackingTestCube()
        Map input = [column: 'A', Row: 1, Age: 7]
        Map output = [:]
        ncube.getCell(input, output)
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 4
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('age')
        assert ruleInfo.getInputKeysUsed().contains('Weight')
        assert ruleInfo.getDefaultKeysUsed().size() == 0
    }

    @Test
    void testKeyTrackingAllInputProvided()
    {
        NCube ncube = NCubeBuilder.getTrackingTestCube()
        Map input = [column: 'A', Row: 1, Age: 7, weight: 210]
        Map output = [:]
        ncube.getCell(input, output)
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 4
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('age')
        assert ruleInfo.getInputKeysUsed().contains('Weight')
        assert ruleInfo.getDefaultKeysUsed().size() == 0
    }

    @Test
    void testKeyTrackingNested()
    {
        NCube ncube = NCubeBuilder.getTrackingTestCube()
        NCube ncube2 = NCubeBuilder.getTrackingTestCubeSecondary()
        NCubeManager.addCube(ApplicationID.testAppId, ncube)
        NCubeManager.addCube(ApplicationID.testAppId, ncube2)

        Map input = [Column: 'B', Row:1, state:'OH']
        Map output = [:]
        def x = ncube.getCell(input, output)
        assert 9 == x
        RuleInfo ruleInfo = ncube.getRuleInfo(output)

        assert ruleInfo.getInputKeysUsed().size() == 5
        assert ruleInfo.getInputKeysUsed().contains('Column')
        assert ruleInfo.getInputKeysUsed().contains('Row')
        assert ruleInfo.getInputKeysUsed().contains('state')
        assert ruleInfo.getInputKeysUsed().contains('age')
        assert ruleInfo.getInputKeysUsed().contains('weight')
        assert ruleInfo.getDefaultKeysUsed().size() == 0
    }

    @Test
    void testKeyTrackingInRuleConditions()
    {
        NCube ncube = NCubeBuilder.getSimpleAutoRule()
        Map output = [:]
        ncube.getCell([rate:0], output)
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        Set keys = ruleInfo.getInputKeysUsed()
        assert keys.size() == 4
        assert keys.contains('conditions')
        assert keys.contains('AGe')
        assert keys.contains('coLOR')
        assert keys.contains('creditSCORE')
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        Set keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('conditions')

        output = [:]
        output.rate = 0.0
        ncube.getCell([age:50, creditScore:701], output)
        ruleInfo = ncube.getRuleInfo(output)
        keys = ruleInfo.getInputKeysUsed()
        assert keys.size() == 3
        assert keys.contains('conditions')
        assert keys.contains('AGe')
        assert keys.contains('creditSCORE')
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('conditions')
    }

    @Test
    void testKeyTrackingMetaPropertyWithExp()
    {
        NCube ncube = NCubeBuilder.getMetaPropWithFormula()
        Map output = [:]
        def formula = ncube.getMetaProperty("formula")
        ncube.extractMetaPropertyValue(formula, [:], output)
        RuleInfo ruleInfo = ncube.getRuleInfo(output)
        Set keys = ruleInfo.getInputKeysUsed()
        assert keys.size() == 1
        assert keys.contains("Revenue")
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        Set keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('Column')


        formula = ncube.getMetaProperty("formula")
        def profit = ncube.extractMetaPropertyValue(formula, [revenue:100, cost:40, tax:(1 - 0.2)], output)
        assert profit == 48.0
        ruleInfo = ncube.getRuleInfo(output)
        keys = ruleInfo.getInputKeysUsed()
        assert keys.size() == 3
        assert keys.contains("Revenue")
        assert keys.contains("Cost")
        assert keys.contains("Tax")
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        keysUsed =  ruleInfo.getDefaultKeysUsed()[ncube.name]
        assert keysUsed.size() == 1
        assert keysUsed.contains('Column')
    }

    @Test
    void testKeyTrackingWithSecondaryCubeWithDefaults()
    {
        NCube primary = NCubeBuilder.getCubeCallingCubeWithDefaultColumn()
        NCubeManager.addCube(ApplicationID.testAppId, primary)
        NCube secondary = NCubeBuilder.getCubeWithDefaultColumn()
        NCubeManager.addCube(ApplicationID.testAppId, secondary)
        Map input = [Axis1Primary: 'Axis1Col1', Axis2Primary: 'Axis2Col1', Axis1Secondary: 'Axis1Col1', Axis2Secondary: 'Axis2Col1']
        Map output = [:]
        primary.getCell(input, output)
        RuleInfo ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 4
        assert ruleInfo.getInputKeysUsed().contains('Axis1Primary')
        assert ruleInfo.getInputKeysUsed().contains('Axis2Primary')
        assert ruleInfo.getInputKeysUsed().contains('Axis1Secondary')
        assert ruleInfo.getInputKeysUsed().contains('Axis2Secondary')
        assert ruleInfo.getDefaultKeysUsed().size() == 0

        input.remove('Axis1Secondary')
        primary.getCell(input, output)
        ruleInfo = primary.getRuleInfo(output)
        assert ruleInfo.getInputKeysUsed().size() == 4
        assert ruleInfo.getInputKeysUsed().contains('Axis1Primary')
        assert ruleInfo.getInputKeysUsed().contains('Axis2Primary')
        assert ruleInfo.getInputKeysUsed().contains('Axis1Secondary')
        assert ruleInfo.getInputKeysUsed().contains('Axis2Secondary')
        assert ruleInfo.getDefaultKeysUsed().size() == 1
        Set keysUsed =  ruleInfo.getDefaultKeysUsed()[secondary.name]
        assert keysUsed.size() == 1
        assert ruleInfo.getInputKeysUsed().contains('Axis1Secondary')
    }
}