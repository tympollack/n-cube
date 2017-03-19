package com.cedarsoftware.ncube

import com.cedarsoftware.controller.NCubeController
import com.cedarsoftware.servlet.JsonCommandServlet
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.web.DispatcherServletAutoConfiguration
import org.springframework.boot.web.servlet.FilterRegistrationBean
import org.springframework.boot.web.servlet.ServletRegistrationBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.ImportResource
import org.springframework.web.filter.GenericFilterBean
import org.springframework.web.filter.HiddenHttpMethodFilter
import org.springframework.web.filter.HttpPutFormContentFilter
import org.springframework.web.filter.RequestContextFilter

/**
 * This class defines allowable actions against persisted n-cubes
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

@Configuration
@ImportResource("classpath:config/beans.xml")
@SpringBootApplication(exclude = [DataSourceAutoConfiguration.class, DispatcherServletAutoConfiguration.class])
class NCubeApplication
{
    private static final Logger LOG = LogManager.getLogger(NCubeController.class)

    static void main(String[] args)
    {
        try
        {
            SpringApplication.run(NCubeApplication.class, args)
        }
        catch (Throwable t)
        {
            LOG.error('Exception occurred', t)
        }
        finally
        {
            LOG.info('NCUBE server started.')
        }
    }

    @Bean
    ServletRegistrationBean servletRegistrationBean()
    {
        return new ServletRegistrationBean(new JsonCommandServlet(), "/ncube/cmd/*")
    }

    @Bean
    FilterRegistrationBean filterRegistrationBean1()
    {
        GenericFilterBean filter = new HiddenHttpMethodFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.addUrlPatterns("/piss-off/*")
        registration.enabled = false
        return registration
    }

    @Bean
    FilterRegistrationBean filterRegistrationBean2()
    {
        GenericFilterBean filter = new HttpPutFormContentFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.addUrlPatterns("/piss-off/*")
        registration.enabled = false
        return registration
    }

    @Bean
    FilterRegistrationBean filterRegistrationBean3()
    {
        GenericFilterBean filter = new RequestContextFilter()
        FilterRegistrationBean registration = new FilterRegistrationBean(filter)
        registration.addUrlPatterns("/piss-off/*")
        registration.enabled = false
        return registration
    }
}
