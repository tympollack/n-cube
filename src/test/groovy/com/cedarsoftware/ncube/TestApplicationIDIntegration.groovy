package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Test

import static org.junit.Assert.assertEquals

/**
 * ApplicationID Tests
 *
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
class TestApplicationIDIntegration extends NCubeBaseTest2
{
    @Test
    void testGetBootstrapVersion()
    { // TODO - should this be moved to TestNCubeRuntime?
        NCubeRuntime runtime = mutableClient as NCubeRuntime
        System.setProperty("NCUBE_PARAMS", '{}')
        runtime.clearSysParams()

        ApplicationID id = runtime.getBootVersion('foo', 'bar')
        assertEquals 'foo', id.tenant
        assertEquals 'bar', id.app
        assertEquals '0.0.0', id.version
        assertEquals 'SNAPSHOT', id.status
        assertEquals 'HEAD', id.branch

        System.setProperty("NCUBE_PARAMS", '{"branch":"qux"}')
        runtime.clearSysParams()

        id = runtime.getBootVersion('foo', 'bar')
        assertEquals 'foo', id.tenant
        assertEquals 'bar', id.app
        assertEquals '0.0.0', id.version
        assertEquals 'SNAPSHOT', id.status
        assertEquals 'qux', id.branch
    }
}
