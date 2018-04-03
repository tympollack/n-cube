package com.cedarsoftware.ncube
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.io.JsonObject
import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import org.junit.Test

import static org.junit.Assert.assertArrayEquals
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License');
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestCellInfo extends NCubeBaseTest
{
    @Test
    void testFormatForEditing()
    {
        assertEquals('4.56', CellInfo.formatForEditing(4.56g))
//        assertEquals('0.0', CellInfo.formatForEditing(0.0g))  // JDK 1.7 returns 0.0, JDK 1.8 returns 0
        assertEquals('4.0', CellInfo.formatForEditing(new Float(4)))
        assertEquals('4.0', CellInfo.formatForEditing(new Double(4)))

        assertEquals('4.56', CellInfo.formatForEditing(new BigDecimal('4.56000')))
        assertEquals('4.56', CellInfo.formatForEditing(new BigDecimal('4.56')))

        Calendar c = Calendar.instance
        c.set(2005, 10, 21, 12, 15, 19)
        assertEquals('\"2005-11-21 12:15:19\"', CellInfo.formatForEditing(c.time))
    }

    @Test
    void testCollapseToUISupportedTypes()
    {
        CellInfo info = new CellInfo(5)
        assert 'int' == info.dataType
        info.collapseToUiSupportedTypes()
        assert 'long' == info.dataType

        info = new CellInfo(new Short((short) 5))
        assert 'short' == info.dataType
        info.collapseToUiSupportedTypes()
        assert 'long' == info.dataType

        info = new CellInfo(new Byte((byte) 5))
        assert 'byte' == info.dataType
        info.collapseToUiSupportedTypes()
        assert 'long' == info.dataType

        info = new CellInfo(new Float(5))
        assert 'float' == info.dataType
        info.collapseToUiSupportedTypes()
        assert 'double' == info.dataType

        info = new CellInfo(new BigInteger('100', 10))
        assert 'bigint' == info.dataType
        info.collapseToUiSupportedTypes()
        assert 'bigdec' == info.dataType
    }

    @Test
    void testFormatForDisplay()
    {
        assertEquals '4.56', CellInfo.formatForEditing(4.560)
        assertEquals '4.5', CellInfo.formatForEditing(4.5)

        assertEquals '4.56', CellInfo.formatForEditing(new BigDecimal('4.5600'))
        assertEquals '4', CellInfo.formatForEditing(new BigDecimal('4.00'))
        assertEquals '4', CellInfo.formatForEditing(new BigDecimal('4'))

        assertEquals '4.56', CellInfo.formatForDisplay(new BigDecimal('4.5600'))
        assertEquals '4', CellInfo.formatForDisplay(new BigDecimal('4.00'))
        assertEquals '4', CellInfo.formatForDisplay(new BigDecimal('4'))
    }

    @Test
    void testRecreate()
    {
        assertNull new CellInfo(null).recreate()

        performRecreateAssertion new StringUrlCmd('http://www.google.com', true)
        performRecreateAssertion new Double(4.56d)
        performRecreateAssertion new Float(4.56f)
        performRecreateAssertion new Short((short) 4)
        performRecreateAssertion new Long(4)
        performRecreateAssertion new Integer(4)
        performRecreateAssertion new Byte((byte) 4)
        performRecreateAssertion new BigDecimal('4.56')
        performRecreateAssertion new BigInteger('900')
        performRecreateAssertion Boolean.TRUE
        performRecreateAssertion new GroovyExpression('0', null, false)
        performRecreateAssertion new GroovyMethod('0', null, false)
        GroovyTemplate template = new GroovyTemplate('<html>${return input.age > 17}<div><%=input.state%></div></html>', null, false)
        Set<String> scopeKeys = new CaseInsensitiveSet<>()
        template.getScopeKeys(scopeKeys)
        performRecreateAssertion(template)
        assert scopeKeys.contains('age')
        assert scopeKeys.contains('AGE')
        assert scopeKeys.contains('age')
        assert scopeKeys.contains('STATE')
        performRecreateAssertion new BinaryUrlCmd('http://www.google.com', false)
        performArrayRecreateAssertion([0, 4, 5, 6] as byte[])
        performRecreateAssertion 'foo'

        //  Have to special create this because milliseconds are not saved
        Calendar c = Calendar.instance
        c.set Calendar.MILLISECOND, 0
        performRecreateAssertion c.time
    }

    @Test
    void testBooleanValue()
    {
        assertTrue CellInfo.booleanValue('true')
        assertFalse CellInfo.booleanValue('false')
    }

    @Test
    void testConstructor()
    {
        CellInfo info = new CellInfo(new Point2D(5.0, 6.0))
        assert 'point2d' == info.dataType

        info = new CellInfo(new Point3D(5.0, 6.0, 7.0))
        assert 'point3d' == info.dataType

        info = new CellInfo(new LatLon(5.5, 5.9))
        assert 'latlon' == info.dataType

        info = new CellInfo(new Range(5.5, 5.9))
        assert 'range' == info.dataType
        assertFalse info.isCached
        assertFalse info.isUrl

        RangeSet set = new RangeSet()
        set.add new Range(0, 5)
        set.add new Range(10, 20)
        set.add 50
        info = new CellInfo(set)
        assert 'rangeset' == info.dataType
        assertFalse info.isUrl
    }

    @Test
    void testConstructorWithUnrecognizedType()
    {
        try
        {
            new CellInfo(new UnrecognizedConstructorObject())
        }
        catch (IllegalArgumentException e)
        {
            assertTrue e.message.contains('Unknown cell value type')
        }
    }

    @Test
    void testParseJsonValue()
    {
        assertEquals Boolean.TRUE, CellInfo.parseJsonValue('boolean', 'true', false)
        assertEquals Boolean.FALSE, CellInfo.parseJsonValue('boolean', 'false', false)
        assertEquals 2 as byte, CellInfo.parseJsonValue('byte', '2', false)
        assertEquals 5 as short, CellInfo.parseJsonValue('short', '5', false)
        assertEquals 9L, CellInfo.parseJsonValue('long', '9', false)
        assertEquals 9, CellInfo.parseJsonValue('int', '9', false)
        assertEquals(9.87d, (Double)CellInfo.parseJsonValue('double', '9.87', false), 0.000001d)
        assertEquals(9.65f, (float)CellInfo.parseJsonValue('float', '9.65', false), 0.000001f)
    }

    @Test
    void testParseJsonValueBinaryWithOddNumberString()
    {
        try
        {
            CellInfo.parseJsonValue('binary', '0', false)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('(hex) values must have an even number of digits')
        }
    }

    @Test
    void testParseJsonValueInvalidHexString()
    {
        try
        {
            CellInfo.parseJsonValue('binary', 'GF', true)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('(hex) values must contain only the numbers 0 thru 9 and letters a thru f')
        }
    }

    @Test
    void testParseJsonValueWithInvalidBoolean()
    {
        try
        {
            CellInfo.parseJsonValue('boolean', 'yes', true)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains("must be 'true' or 'false'")
        }
    }

    @Test
    void testNullItemOnFormatForDisplay()
    {
        assertEquals 'Default', CellInfo.formatForDisplay(null)
    }

    @Test(expected = IllegalArgumentException.class)
    void testParseJsonValueException()
    {
        CellInfo.parseJsonValue(Boolean.TRUE, "http://www.foo.com", "foo", true)
    }

    @Test(expected = IllegalArgumentException.class)
    void testParseJsonValueNonUrlException()
    {
        CellInfo.parseJsonValue("blah blah blah", null, "foo", true)
    }

    @Test(expected = IllegalArgumentException.class)
    void testParseJsonValueWithUnknownType()
    {
        CellInfo.parseJsonValue(new Object(), null, "foo", true)
    }

    @Test
    void testParseJsonValueGroovyMethod()
    {
        GroovyMethod method = (GroovyMethod) CellInfo.parseJsonValue("def [5]", null, "method", true)
        assertEquals(new GroovyMethod("def [5]", null, false), method)
        CellInfo cellInfo = new CellInfo(method)
        Map map = cellInfo as Map
        assert map.type == "method"
        assert map.value instanceof String
    }

    @Test
    void testEqualsAndHashCode()
    {
        CellInfo info1 = new CellInfo([1, 2, 4, 8, 16] as byte[])
        assert info1.equals(info1)
        CellInfo info2 = new CellInfo([1, 2, 4, 8, 16] as byte[])
        assert info1.equals(info2)
        assert info1.hashCode() == info2.hashCode()

        info2 = new CellInfo([1, 2, 4, 8, 16, 32] as byte[])
        assert !info1.equals(info2)
        assert info1.hashCode() != info2.hashCode()

        info2.isUrl = true
        assert !info1.equals(info2)

        info2.isUrl = false
        info2.isCached = true
        assert !info1.equals(info2)

        info1 = new CellInfo(1.33d)
        info2 = new CellInfo(1.33f)
        assert info1 != info2
        assert info1.hashCode() != info2.hashCode()

        def s = "Hi"
        info1 = new CellInfo(s)
        assert info1 == info1

        assertFalse info1.equals(13)
    }

    @Test
    void testRangeWithStrings()
    {
        Range range = new Range('alpha', 'zooloo')
        CellInfo cellInfo = new CellInfo(range)
        assert "range" == cellInfo.getType(range)
        Map map = cellInfo as Map
        assert map.type == "range"
        assert map.value instanceof String
    }

    @Test
    void testRangeSetWithStrings()
    {
        RangeSet set = new RangeSet(new Range('alpha', 'zooloo'))
        set.add('kilo')
        CellInfo cellInfo = new CellInfo(set)
        assert "rangeset" == cellInfo.getType(set)
        Map map = cellInfo as Map
        assert map.type == "rangeset"
        assert map.value instanceof String
    }

    @TypeChecked(TypeCheckingMode.SKIP)
    public void performRecreateAssertion(Object o)
    {
        if (o instanceof Float || o instanceof Double)
        {
            assertEquals(o, new CellInfo(o).recreate(), 0.00001d)
        }
        else
        {
            assertEquals(o, new CellInfo(o).recreate())
        }
    }

    public static void performArrayRecreateAssertion(byte[] o)
    {
        assertArrayEquals o, new CellInfo(o).recreate() as byte[]
    }

    public static class UnrecognizedConstructorObject
    {
    }
}
