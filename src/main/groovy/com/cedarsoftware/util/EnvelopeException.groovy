package com.cedarsoftware.util

import groovy.transform.CompileStatic

/**
 * Sub-class of RuntimeException for extra clarification if needed.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
public class EnvelopeException extends RuntimeException
{
    private final Map<String, Object> envelope

    // placeholder later for change log or something like that.
    public EnvelopeException(String message, Map<String, Object> envelope)
    {
        super(message)
        this.envelope = envelope
    }

    public Map<String, Object> getEnvelope()
    {
        return envelope
    }
}
