package com.cedarsoftware.nce

import com.cedarsoftware.controller.NCubeControllerAdvice

/**
 * Spring Context Configuration
 *
 * @author John DeRegnaucourt
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License");
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ContextConfiguration
{
    @Bean
    public NCubeControllerAdvice ncubeControllerAdvice()
    {
        return new NCubeControllerAdvice()
    }

//    @Bean
//    public DataSource dataSource()
//    {
//        final BasicDataSource ds = new BasicDataSource();
//        ds.setDriverClassName("org.h2.Driver");
//        ds.setUrl("jdbc:h2:~/workspace/h2/spring-noxmal;DB_CLOSE_ON_EXIT=FALSE;TRACE_LEVEL_FILE=4;AUTO_SERVER=TRUE");
//        ds.setUsername("sa");
//        return ds;
//    }

}