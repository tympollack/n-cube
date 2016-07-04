package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.LongHashSet
import org.junit.Test

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
class TestLongHashSet
{
    @Test
    void testLongHashset()
    {
        LongHashSet set1 = new LongHashSet()
        LongHashSet set2 = new LongHashSet()
        set1.add(1L)
        set1.add(20L)
        set1.add(300L)
        set1.add(4000L)
        set1.add(50000L)
        set1.add(600000L)
        set1.add(7000000L)
        set1.add(80000000L)
        set1.add(900000000L)

        set2.add(900000000L)
        set2.add(80000000L)
        set2.add(4000L)
        set2.add(7000000L)
        set2.add(600000L)
        set2.add(1L)
        set2.add(20L)
        set2.add(300L)
        set2.add(50000L)

        assert set1.hashCode() == set2.hashCode()
        assert set1 == set2
    }

    @Test
    void testRemoveFirst()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        set.remove(7)

        assert set.size() == 2
        assert set.contains(8)
        assert set.contains(9)

        set.remove(8)
        assert set.size() == 1
        assert set.contains(9)

        set.remove(9)
        assert set.size() == 0
    }

    @Test
    void testRemoveMiddle()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        set.remove(8)
        assert set.size() == 2
        assert set.contains(7)
        assert set.contains(9)
    }

    @Test
    void testRemoveLast()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        set.remove(9)
        assert set.size() == 2
        assert set.contains(7)
        assert set.contains(8)

        set.remove(8)
        assert set.size() == 1
        assert set.contains(7)

        set.remove(7)
        assert set.size() == 0
    }

    @Test
    void testToArray()
    {
        LongHashSet set = new LongHashSet()
        set.add(7)
        set.add(8)
        set.add(9)
        Object[] nums = set.toArray()
        assert nums[0] == 7
        assert nums[1] == 8
        assert nums[2] == 9
    }
}
