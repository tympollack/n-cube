package com.cedarsoftware.ncube

import groovy.transform.CompileStatic;

/**
 * Process a String that is specified at a URL.
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
class StringUrlCmd extends ContentCmdCell
{
    //  Private constructor only for serialization.
    private StringUrlCmd() {}

    StringUrlCmd(String url, boolean cache)
    {
         super(null, url, cache)
    }

    protected Object proxyFetch(Map args)
    {
        byte[] bytes = (byte[])super.proxyFetch(args)
        return bytes == null ? null : new String(bytes)
    }
}
