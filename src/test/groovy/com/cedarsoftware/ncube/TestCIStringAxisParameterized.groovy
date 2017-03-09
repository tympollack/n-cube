package com.cedarsoftware.ncube

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(Parameterized.class)
class TestCIStringAxisParameterized extends TestCIStringAxis implements StringTestsParameterized {
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
}
