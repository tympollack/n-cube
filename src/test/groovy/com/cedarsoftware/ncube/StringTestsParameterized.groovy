package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import org.junit.Test
import org.junit.runners.Parameterized

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

trait StringTestsParameterized {
    @Parameterized.Parameter
    public String lookupValue

    @Parameterized.Parameter(1)
    public boolean successful

    @Parameterized.Parameter(2)
    public Object expected

    @Test
    void findColumn() {
        def column = genderAxis.findColumn(lookupValue)
        assert successful == (column != null)
    }

    @Test
    void getCell() {
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