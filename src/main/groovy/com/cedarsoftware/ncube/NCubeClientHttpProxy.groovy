package com.cedarsoftware.ncube

import com.cedarsoftware.util.JsonHttpClient
import groovy.transform.CompileStatic
import org.aopalliance.aop.Advice

import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy

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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
class NCubeClientHttpProxy implements Advice
{
    private JsonHttpClient jsonHttpClient
    private NCubeRuntimeClient ncubeClient

    NCubeClientHttpProxy(JsonHttpClient httpClient)
    {
        jsonHttpClient = httpClient
        ncubeClient = (NCubeRuntimeClient) Proxy.newProxyInstance(NCubeClientHttpProxy.class.classLoader, [NCubeRuntimeClient.class] as Class[], new ClientAdvice())
    }

    NCubeRuntimeClient getNcubeClient()
    {
        return ncubeClient
    }

    private class ClientAdvice implements InvocationHandler
    {
        Object invoke(Object proxy, Method method, Object[] args) throws Throwable
        {
            if (args == null)
            {
                args = []
            }
            Object ret = jsonHttpClient.call('ncubeController', method.name, Arrays.asList(args))
            return ret
        }
    }
}