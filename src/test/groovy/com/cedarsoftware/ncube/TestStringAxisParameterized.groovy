package com.cedarsoftware.ncube

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(Parameterized.class)
class TestStringAxisParameterized extends TestStringAxis implements StringTestsParameterized {

    @Parameters(name = "coordinate: {0}")
    static Collection<Object[]> data() {
        [
                ["female", false, null],
                ["FEMALE", false, null],
                ["fEmAlE", false, null],
                ["FeMaLe", false, null],
                ["Female", true, 1],
                ["Jones", false, null],
                ["male", false, null],
                ["MALE", false, null],
                ["mAlE", false, null],
                ["Male", true, 0]
        ]*.toArray()
    }


}
