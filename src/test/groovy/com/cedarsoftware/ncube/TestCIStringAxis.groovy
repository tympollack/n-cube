package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Before

@CompileStatic
class TestCIStringAxis {
    public NCube<Integer> ncube
    public Axis genderAxis


    @Before
    void setUp() {
        ncube = new NCube<Integer>("SingleStringAxis")
        genderAxis = NCubeBuilder.getCaseInsensitiveAxis(false)
        ncube.addAxis(genderAxis)
        ncube.setCell(0, [Gender: 'Male'])
        ncube.setCell(1, [Gender: 'Female'])
    }
}
