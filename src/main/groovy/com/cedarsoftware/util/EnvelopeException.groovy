package com.cedarsoftware.util

import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
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
class EnvelopeException extends RuntimeException
{
    private final Map<String, Object> envelope

    // placeholder later for change log or something like that.
    EnvelopeException(String message, Map<String, Object> envelope)
    {
        super(message)
        this.envelope = envelope
    }

    Map<String, Object> getEnvelope()
    {
        return envelope
    }

    Object getEnvelopeData()
    {
        return envelope.data
    }

    String toString()
    {
        String body = 'Error returned no data'
        if (envelope.data instanceof String)
        {
            String data = envelope.data as String
            envelope.data = data.replaceAll('<hr.+?>', '\n')
            body = envelope.data
        }
        else if (envelope.data instanceof Map)
        {
            Map data = envelope.data as Map
            if (data.containsKey('_message'))
            {
                body = data['_message']
            }
            else
            {
                body = "Data map contained no _message key. Map keys: ${data.keySet().toString()}"
            }
        }
        else
        {
            Object obj = envelope.data
            if (obj != null)
            {
                body = "Data included: ${obj.class.name} - ${obj.toString()}"
            }
        }

        String ret = """${message}
${body}"""
        return ret
    }
}