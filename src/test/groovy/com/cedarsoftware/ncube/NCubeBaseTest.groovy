package com.cedarsoftware.ncube

import com.cedarsoftware.util.EnvelopeException
import groovy.transform.CompileStatic
import org.junit.runner.RunWith
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringRunner

import static org.junit.Assert.assertTrue

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
@RunWith(SpringRunner.class)
@ContextConfiguration(locations = ['/config/beans.xml'])
@ActiveProfiles(profiles = ['runtime', 'test-mutable'])
class NCubeBaseTest
{
    static NCubeRuntimeClient getRuntimeClient()
    {
        return SpringAppContext.runtime
    }

    static NCubeMutableClient getMutableClient()
    {
        return SpringAppContext.mutableClient
    }
    
    static void assertEnvelopeExceptionContains(EnvelopeException e, String... contains)
    {
        assertContainsIgnoreCase(e.envelopeData as String, contains)
    }

    static void assertContainsIgnoreCase(String source, String... contains)
    {
        String lowerSource = source.toLowerCase()
        for (String contain : contains)
        {
            int idx = lowerSource.indexOf(contain.toLowerCase())
            assertTrue("'${contain}' not found in '${lowerSource}'", idx >= 0)
            lowerSource = lowerSource.substring(idx)
        }
    }

    static boolean checkContainsIgnoreCase(String source, String... contains)
    {
        String lowerSource = source.toLowerCase()
        for (String contain : contains)
        {
            int idx = lowerSource.indexOf(contain.toLowerCase())
            if (idx == -1)
            {
                return false
            }
            lowerSource = lowerSource.substring(idx)
        }
        return true
    }
}