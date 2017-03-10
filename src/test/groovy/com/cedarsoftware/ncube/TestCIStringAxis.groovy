package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.exception.InvalidCoordinateException
import groovy.transform.CompileStatic
import org.junit.Before
import org.junit.Test

import static org.junit.Assert.fail

@CompileStatic
class TestCIStringAxis {
    public NCube<Integer> ncube
    public Axis genderAxis


    @Test
    void caseDiffers() {
        assert 0 == ncube.getCell([Gender: 'male'])
        assert 1 == ncube.getCell([Gender: 'female'])
        assert 3 == TestNCube.countMatches(ncube.toHtml(), "<tr")
    }

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

    @Test
    void findColumnMatch() {
        Column column = genderAxis.findColumn('Male')
        assert 'Male' == column.value
    }

    @Test
    void findColumnCaseMiss() {
        Column column = genderAxis.findColumn('Jones')
        assert null == column
    }

    @Test
    void findColumnCaseDiffers() {
        Column column = genderAxis.findColumn('male')
        assert 'Male' == column.value
    }

    @Before
    void setUp() {
        ncube = new NCube<Integer>("SingleStringAxis")
        genderAxis = NCubeBuilder.getCaseInsensitiveAxis(false)
        ncube.addAxis(genderAxis)
        ncube.setCell(0, [Gender: 'Male'])
        ncube.setCell(1, [Gender: 'Female'])
    }
}
