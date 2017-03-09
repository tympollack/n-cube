package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail

@CompileStatic
class TestStringAxis {
    private NCube<Integer> ncube
    private Axis genderAxis

    @Test
    void happyPath() {
        assert 0 == ncube.getCell([Gender: 'Male'])
        assert 1 == ncube.getCell([Gender: 'Female'])
        assert 3 == TestNCube.countMatches(ncube.toHtml(), "<tr")
    }

    @Test
    void invalidColumn() {
        try {
            genderAxis.addColumn(new Comparable() {
                int compareTo(Object o) {
                    return 0
                }
            })
            fail()
        }
        catch (IllegalArgumentException e) {
            assert e.message.toLowerCase().contains('unsupported value type')
        }
    }

    @Test
    void valueNotFound() {
        Map coord = [Gender: "Jones"] as Map
        try {
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e) {
            assertTrue(e.message.contains("alue"))
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("found"))
            assertTrue(e.message.contains("axis"))
            assertEquals(ncube.name, e.cubeName)
            assertEquals(coord, e.coordinate)
            assertEquals("Gender", e.axisName)
            assertEquals("Jones", e.value)
        }

    }

    @Test
    void findNullOnStringAxis() {
        Map coord = [Gender: null] as Map
        try {
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e) {
            assert e.message.toLowerCase().contains('null')
            assert e.message.toLowerCase().contains('not found on axis')
            assert ncube.name == e.cubeName
            assert coord == e.coordinate
            assert "Gender" == e.axisName
            assert !e.value
        }

    }

    @Test
    void findIllegalValueOnStringAxis() {
        Map coord = [Gender: 8] as Map
        try {
            ncube.getCell(coord)
            fail()
        }
        catch (CoordinateNotFoundException e) {
            assert e.message.toLowerCase().contains('value')
            assert e.message.toLowerCase().contains('not found on axis')
            assert ncube.name == e.cubeName
            assert coord == e.coordinate
            assert "Gender" == e.axisName
            assert 8 == e.value
        }

    }

    @Test
    void nullCoordinate() {
        try {
            ncube.getCell((Map) null)
            fail()
        }
        catch (IllegalArgumentException e) {
            assert e.message.toLowerCase().contains('null')
            assert e.message.toLowerCase().contains('passed in for coordinate map')
        }

    }

    @Test
    void zeroLengthCoordinate() {
        Map coord = [:] as Map
        try {
            ncube.getCell(coord)
            fail()
        }
        catch (InvalidCoordinateException e) {
            assert e.message.toLowerCase().contains('required scope')
            assert ncube.name == e.cubeName
            assert !e.coordinateKeys
            assert e.requiredKeys.contains('Gender')
        }
    }

    @Test
    void coordinateTableDimensionMismatch() {
        Map coord = [State: 'OH'] as Map
        try {
            ncube.getCell(coord)
            fail()
        }
        catch (InvalidCoordinateException e) {
            assert e.message.toLowerCase().contains('required scope')
            assert ncube.name == e.cubeName
            assert e.coordinateKeys.contains('State')
            assert e.requiredKeys.contains('Gender')
        }
    }

    @Before
    void setUp() {
        ncube = new NCube<Integer>("SingleStringAxis")
        genderAxis = NCubeBuilder.getGenderAxis(false)
        ncube.addAxis(genderAxis)
        ncube.setCell(0, [Gender: 'Male'])
        ncube.setCell(1, [Gender: 'Female'])
    }
}
