package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.Assert
import org.junit.Test

import java.lang.reflect.Constructor
import java.lang.reflect.Modifier

import static org.junit.Assert.fail

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
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
class TestBinaryUrlCmd extends NCubeBaseTest
{
    @Test
    void testDefaultConstructorIsPrivateForSerialization()
    {
        Class c = BinaryUrlCmd.class
        Constructor<BinaryUrlCmd> con = c.getDeclaredConstructor()
        Assert.assertEquals(Modifier.PRIVATE, con.modifiers & Modifier.PRIVATE)
        con.accessible = true
        Assert.assertNotNull con.newInstance()
    }

    @Test
    void testSimpleFetchException() {

        try {
            NCube cube = NCubeBuilder.getTestNCube2D true
            BinaryUrlCmd cmd = new BinaryUrlCmd('/foo' +
                    '', false)
            def args = [ncube: cube]

            cmd.simpleFetch(args)
            fail();
        }
        catch (IllegalStateException e)
        {
            assertContainsIgnoreCase(e.message, 'invalid url', 'unable', 'sys.classpath', '/foo', 'test.Age-Gender')
        }
    }
}
