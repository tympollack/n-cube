package com.cedarsoftware.util

import com.cedarsoftware.ncube.NCubeManagerImpl
import groovy.transform.CompileStatic

import java.lang.reflect.Method

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br><br>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br><br>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either eƒfetxpress or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
class ReflectiveClient implements CallableBean
{
    // TODO - add method cache

    Object call(String bean, String method, List args) throws Throwable
    {
        Class[] types
        if (args == null)
        {
            types = null
        }
        else
        {
            int len = args.size()
            types = new Class[len]
            for (int i = 0; i < len; i++)
            {
                types[i] = args[i].class
            }
        }
        //TODO - get this from spring
        NCubeManagerImpl manager = new NCubeManagerImpl()
        Method managerMethod = ReflectionUtils.getMethod(manager.class, method, types)
        return managerMethod.invoke(manager, args.toArray())
    }
}