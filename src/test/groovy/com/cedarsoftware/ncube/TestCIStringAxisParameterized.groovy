package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameter
import org.junit.runners.Parameterized.Parameters

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

@RunWith(Parameterized.class)
class TestCIStringAxisParameterized extends TestCIStringAxis {
    @Parameter
    public String lookupValue

    @Parameter(1)
    public boolean successful

    @Parameter(2)
    public Object expected

    @Parameters(name = "coordinate: {0}")
    static Collection<Object[]> data() {
        [
                ["female", true, 1],
                ["FEMALE", true, 1],
                ["fEmAlE", true, 1],
                ["FeMaLe", true, 1],
                ["Female", true, 1],
                ["Jones", false, null],
                ["male", true, 0],
                ["MALE", true, 0],
                ["mAlE", true, 0],
                ["Male", true, 0]
        ]*.toArray()
    }

    @Test
    void findColumn() {
        def column = genderAxis.findColumn(lookupValue)
        assert successful == (column != null)
    }

    @Test
    void lookup() {
        if (successful) {
            valueFound()
        } else {
            valueNotFound()
        }
    }

    void valueFound() {
        assert expected == ncube.getCell([Gender: lookupValue])
    }

    void valueNotFound() {
        try {
            ncube.getCell([Gender: lookupValue] as Map)
            fail()
        }
        catch (CoordinateNotFoundException e) {
            assertTrue(e.message.contains("alue"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
            assertEquals(ncube.name, e.cubeName)
            assertEquals([Gender: lookupValue] as Map, e.coordinate)
            assertEquals("Gender", e.axisName)
            assertEquals(lookupValue, e.value)
        }

    }

}
