package com.cedarsoftware.util

import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import groovy.transform.CompileStatic
import org.apache.http.HttpHost
import org.apache.http.HttpResponse
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.AuthCache
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.BasicAuthCache
import org.apache.http.impl.client.BasicCookieStore
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

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
class JsonHttpProxy implements CallableBean
{
    private final CloseableHttpClient httpClient
    private final CredentialsProvider credsProvider
    private final AuthCache authCache
    private final HttpHost httpHost
    private final String context
    private final String username
    private final String password
    private final int numConnections
    private static final Logger LOG = LogManager.getLogger(JsonHttpProxy.class)

    JsonHttpProxy(String scheme, String hostname, int port, String context, String username = null, String password = null, int numConnections = 6)
    {
        httpHost = new HttpHost(hostname, port, scheme)
        this.context = context
        this.username = username
        this.password = password
        this.numConnections = numConnections
        httpClient = createClient()

        credsProvider = new BasicCredentialsProvider()
        AuthScope authScope = new AuthScope(httpHost.hostName, httpHost.port)
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password)
        credsProvider.setCredentials(authScope, credentials)
        authCache = new BasicAuthCache()
        authCache.put(httpHost, new BasicScheme())
    }

    /**
     * Creates the client object with the proxy and cookie store for later use.
     *
     * @return A {@link CloseableHttpClient} with the GAIG proxy
     */
    protected CloseableHttpClient createClient()
    {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
        cm.maxTotal = numConnections // Max total connection
        cm.defaultMaxPerRoute = numConnections // Default max connection per route
        cm.setMaxPerRoute(new HttpRoute(httpHost), numConnections) // Max connections per route

        HttpClientBuilder builder = HttpClientBuilder.create()
        builder.connectionManager = cm
        builder.defaultCookieStore = new BasicCookieStore()
        CloseableHttpClient httpClient = builder.build()
        return httpClient
    }

    Object call(String bean, String method, List args)
    {
        String jsonArgs = JsonWriter.objectToJson(args.toArray())

        LOG.info("${bean}.${method}(${jsonArgs})")
        println("${bean}.${method}(${jsonArgs})")
        long start = System.nanoTime()

        HttpClientContext clientContext = HttpClientContext.create()
        clientContext.credentialsProvider = credsProvider
        clientContext.authCache = authCache

        HttpPost request = new HttpPost("${httpHost.toURI()}/${context}/cmd/${bean}/${method}")
        request.entity = new StringEntity(jsonArgs, ContentType.APPLICATION_JSON)
        HttpResponse response = httpClient.execute(request, clientContext)
        String json = EntityUtils.toString(response.entity)
        EntityUtils.consume(response.entity)

        long stop = System.nanoTime()
        LOG.info("    ${Math.round((stop - start) / 1000000.0d)}ms - ${json}")
        println("    ${Math.round((stop - start) / 1000000.0d)}ms - ${json}")

        Map envelope = JsonReader.jsonToJava(json) as Map
        if (envelope.status == false)
        {
            if (envelope.data instanceof String)
            {
                String data = envelope.data as String
                try
                {
                    Object obj = JsonReader.jsonToJava(data)
                    if (obj instanceof Exception)
                    {
                        throw obj as Exception
                    }
                }
                catch (Exception ignored)
                {
                }
            }
            throw new EnvelopeException('REST call indicated failure', envelope)
        }
        return envelope.data
    }
}