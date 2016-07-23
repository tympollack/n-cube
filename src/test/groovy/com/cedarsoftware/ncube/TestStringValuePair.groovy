package com.cedarsoftware.ncube

import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotEquals

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
class TestStringValuePair
{
    @Test
    void testEquals()
    {
        StringValuePair one = new StringValuePair('foo', null)
        StringValuePair two = new StringValuePair('FOO', null)
        assert one.equals(two)

        one = new StringValuePair('Foo', new CellInfo("string", "hey", false, false))
        two = new StringValuePair('FOO', new CellInfo("string", "hey", false, false))
        assert one.equals(two)

        one = new StringValuePair('Foo', new CellInfo("string", "hey", false, false))
        two = new StringValuePair('FOO', new CellInfo("string", "hi", false, false))
        assert !one.equals(two)
    }
}
