package com.cedarsoftware.util

import groovy.transform.CompileStatic
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
class ReflectiveClient implements CallableBean, ApplicationContextAware
{
    private static ApplicationContext ctx

    Object call(String beanName, String methodName, List args)
    {
        Object bean = ctx.getBean(beanName)
        boolean emptyArgs = args.size() == 0
        Class[] arguments = new Class[args.size()]
        args.eachWithIndex { Object arg, int i ->
            arguments[i] = arg.class
        }
        arguments = emptyArgs ? null : arguments
        Object[] values = emptyArgs ? null : args as Object[]
        Method method = ReflectionUtils.getMethod(bean.class, methodName, arguments)
        return method.invoke(bean, values)
    }

    void setApplicationContext(ApplicationContext applicationContext)
    {
        ctx = applicationContext
    }
}