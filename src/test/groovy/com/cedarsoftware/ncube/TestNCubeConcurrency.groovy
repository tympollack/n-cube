package com.cedarsoftware.ncube

import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
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
class TestNCubeConcurrency
{
    @Before
    public void initialize()
    {
        TestingDatabaseHelper.setupDatabase()
    }

    @After
    public void tearDown()
    {
        TestingDatabaseHelper.tearDownDatabase()
    }

    @Test
    void testConcurrencyWithDifferentFiles()
    {
        def test1 = { concurrencyTest('StringFromRemoteUrlBig') }
        def test2 = { concurrencyTest('StringFromLocalUrl') }
        def test3 = { concurrencyTest('BinaryFromRemoteUrl') }
        def test4 = { concurrencyTest('BinaryFromLocalUrl') }
        Thread t1 = new Thread(test1)
        Thread t2 = new Thread(test2)
        Thread t3 = new Thread(test3)
        Thread t4 = new Thread(test4)
        t1.name = 'test 1'
        t1.daemon = true

        t2.name = 'test 2'
        t2.daemon = true

        t3.name = 'test 3'
        t3.daemon = true

        t4.name = 'test 4'
        t4.daemon = true

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()
    }

    private static void concurrencyTest(final String site)
    {
        int numThreads = 8
        int timeToRun = 3000;
        Thread[] threads = new Thread[numThreads]
        long[] iter = new long[numThreads]
        NCube n1 = NCubeManager.getNCubeFromResource('urlContent.json')
        Map map = new ConcurrentHashMap()
        AtomicInteger count = new AtomicInteger(0)

        for (int i = 0; i < numThreads; i++)
        {
            final int index = i

            def run = {
                try
                {
                    long start = System.currentTimeMillis()
                    while (System.currentTimeMillis() - start < timeToRun)
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            map.put(n1.getCell([sites:site] as Map), true)
                            count.incrementAndGet()
                        }
                        iter[index]++
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace()
                }
            }
            threads[i] = new Thread(run)
            threads[i].name = 'NCubeConcurrencyTest' + i
            threads[i].daemon = true
        }

        // Start all at the same time (more concurrent that starting them during construction)
        for (int i = 0; i < numThreads; i++)
        {
            threads[i].start()
        }

        for (int i = 0; i < numThreads; i++)
        {
            threads[i].join()
        }

        if ('test 4' == Thread.currentThread().name)
        {   // byte[] not cached, will each be added as different instance as Map key (BinaryFromLocalUrl)
            assert map.size() > 1
        }
    }

    @Test
    void testCacheFlag() throws IOException
    {
        NCube n1 = NCubeManager.getNCubeFromResource('urlContent.json')
        def items = new IdentityHashMap()
        def set = new LinkedHashSet()

        def cell = n1.getCell([sites:'StringFromRemoteUrlBig'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'StringFromRemoteUrlBig'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 1
        assert set.size() == 1

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'StringFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'StringFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 2        // Different at the Identity level, therefore IdentityHashSet creates another entry
        assert set.size() == 1          // Matches as .equals() therefore LinkedHashSet does not create another entry

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'BinaryFromRemoteUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'BinaryFromRemoteUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 1
        assert set.size() == 1

        items.clear()
        set.clear()
        cell = n1.getCell([sites:'BinaryFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        cell = n1.getCell([sites:'BinaryFromLocalUrl'] as Map)
        items.put(cell, Boolean.TRUE)
        set.add(cell)
        assert items.size() == 2
        assert set.size() == 2
    }
}
