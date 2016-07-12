package com.cedarsoftware.ncube.util

import groovy.transform.CompileStatic

import java.nio.ByteBuffer

/**
 * Special Set instance that hashes the Set<Long> column IDs with excellent dispersion.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License");
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class LongHashSet implements Set<Long>
{
    private byte[] elems = (byte[]) null

    LongHashSet()
    { }

    LongHashSet(Collection<? extends Long> col)
    {
        elems = new byte[col.size() * 5]
        int idx = 0
        for (item in col)
        {
            setElem(idx++, item)
        }
    }

    int size()
    {
        return elems == null ? 0i : elems.length / 5.0d
    }

    boolean isEmpty()
    {
        return size() == 0i
    }

    boolean contains(Object item)
    {
        if (isEmpty())
        {
            return false
        }

        int len = size()
        for (int i=0; i < len; i++)
        {
            if (getElem(i) == item)
            {
                return true
            }
        }
        return false
    }

    Iterator iterator()
    {
        Iterator it = new Iterator() {
            private int currentIndex = 0

            public boolean hasNext()
            {
                if (elems == null)
                {
                    return false
                }
                return currentIndex < LongHashSet.this.size()
            }

            public Long next()
            {
                return getElem(currentIndex++)
            }

            public void remove()
            {
                throw new UnsupportedOperationException()
            }
        }
        return it
    }

    Object[] toArray()
    {
        if (isEmpty())
        {
            return [] as Object[]
        }

        final int len = size()
        Object[] array = new Object[len]

        for (int i=0; i < len; i++)
        {
            array[i] = getElem(i)
        }
        return array
    }

    boolean add(Long o)
    {
        if (elems == null)
        {
            elems = new byte[5]
            setElem(0, o)
            return true
        }
        else
        {
            int origSize = size()
            byte[] newElems = new byte[(origSize + 1i) * 5i]
            System.arraycopy(elems, 0, newElems, 0, origSize * 5)
            elems = newElems
            setElem(origSize, o)
            return size() != origSize
        }
    }

    boolean remove(Object o)
    {
        if (isEmpty())
        {
            return false
        }
        int len = size()

        for (int i=0; i < len; i++)
        {
            if (getElem(i) == o)
            {
                byte[] newElems = new byte[(len - 1) * 5]
                System.arraycopy(elems, (i + 1) * 5, elems, i * 5, (len - i - 1) * 5)
                System.arraycopy(elems, 0, newElems, 0, (len - 1) * 5)
                elems = newElems
                return true
            }
        }
        return false
    }

    boolean addAll(Collection<? extends Long> col)
    {
        int origSize = size()
        for (o in col)
        {
            add(o)
        }
        return size() != origSize
    }

    void clear()
    {
        elems = (byte[]) null
    }

    boolean removeAll(Collection col)
    {
        int origSize = size()
        for (o in col)
        {
            remove(o)
        }
        return size() != origSize
    }

    boolean retainAll(Collection col)
    {
        int origSize = size()
        Set<Long> keep = new LinkedHashSet<Long>()
        for (item in col)
        {
            if (contains(item))
            {
                keep.add(item as Long)
            }
        }
        elems = new byte[keep.size() * 5]
        int idx = 0
        for (item in keep)
        {
            setElem(idx++, item as Long)
        }
        return size() != origSize
    }

    boolean containsAll(Collection col)
    {
        for (item in col)
        {
            if (!contains(item))
            {
                return false
            }
        }
        return true
    }

    Object[] toArray(Object[] a)
    {
        return toArray()
    }

    boolean equals(Object other)
    {
        if (other == this)
        {
            return true
        }
        if (!(other instanceof Set))
        {
            return false
        }

        Set that = other as Set
        if (that.size() != size())
        {
            return false
        }

        if (isEmpty())
        {
            return true
        }

        int len = size()
        for (int i=0; i < len; i++)
        {
            if (!that.contains(getElem(i)))
            {
                return false
            }
        }
        return true
    }

    int hashCode()
    {
        int h = 0

        // This must be an order insensitive hash
        int len = size()
        for (int i=0; i < len; i++)
        {
            long x = getElem(i)
            // do not change the formula below.  It is been hand crafted and tested for performance.
            // If this does not hash well, ncube breaks down in performance.  The BigCube tests are
            // greatly slowed down as proper hashing is vital or cells will be really slow to access
            // when there are a lot of them in the ncube.

            // Original hash function  (John)
//            h += (int)(x * 347 ^ (x >>> 32) * 7)

            // Better (from Stack overflow)
//            x = ((x >> 16) ^ x) * 0x45d9f3b
//            x = ((x >> 16) ^ x) * 0x45d9f3b
//            x = ((x >> 16) ^ x)
//            h += (int) x

            // Even better (from Google)
            x ^= x >> 23
            x *= 0x2127599bf4325c37L
            x ^= x >> 47
            h += (int) x
        }
        return h
    }

    void setElem(int index, Long value)
    {
        byte[] bytes = longToBytes(value)
        System.arraycopy(bytes, 0, elems, index * 5, 5)
    }

    long getElem(int i)
    {
        byte[] bytes = new byte[5]
        System.arraycopy(elems, i * 5, bytes, 0, 5)
        return bytesToLong(bytes)
    }

    static long bytesToLong(final byte[] bytes)
    {
        return bytes[4] * 1000000000000L | (bytes[3] & 0xff) | ((bytes[2] & 0xff) << 8) | ((bytes[1] & 0xff) << 16) | ((bytes[0] & 0xff) << 24)
    }

    static byte[] longToBytes(long value)
    {
        long axisId = (long) (value / 1000000000000.0D)
        int columnId = (int) (value % 1000000000000.0D)
        byte[] bytes = new byte[5]
        bytes[4] = (byte)axisId
        bytes[3] = (byte)(columnId & 0x000000ff)
        bytes[2] = (byte)(columnId & 0x0000ff00) >> 8
        bytes[1] = (byte)(columnId & 0x00ff0000) >> 16
        bytes[0] = (byte)(columnId & 0xff000000) >> 24
        return bytes
    }
}
