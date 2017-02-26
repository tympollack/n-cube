package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

@CompileStatic
@RunWith(SpringRunner.class)
@ContextConfiguration(locations = ['/config/beans.xml'])
@ActiveProfiles(profiles = ['runtime', 'test-mutable'])
class TestStuff
{
    @Before
    void setUp()
    {
        TestingDatabaseHelper.initDatabase()
    }

    @After
    void tearDown()
    {
        TestingDatabaseHelper.clearDatabase()
    }

    @Test
    void testStuff()
    {
        println 'test'
    }

    @Test
    void testStuff2()
    {
        println 'test'
    }
}