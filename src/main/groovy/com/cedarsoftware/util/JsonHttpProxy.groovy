package com.cedarsoftware.util

import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.StandardHttpRequestRetryHandler
import org.springframework.util.FastByteArrayOutputStream

import com.cedarsoftware.servlet.JsonCommandServlet
import com.cedarsoftware.util.io.JsonReader
import com.cedarsoftware.util.io.JsonWriter
import com.cedarsoftware.util.io.MetaUtils
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
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.BasicAuthCache
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.PropertySource

import javax.servlet.http.Cookie
import javax.servlet.http.HttpServletRequest
import java.util.zip.Deflater

import static com.cedarsoftware.ncube.NCubeConstants.LOG_ARG_LENGTH
import static org.apache.http.HttpHeaders.*
import static org.apache.http.entity.ContentType.APPLICATION_JSON

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
@PropertySource(value='classpath:application.properties')
class JsonHttpProxy implements CallableBean
{
    @Value("#{\${ncube.proxy.cookiesToInclude:'JSESSIONID'}.split(',')}")
    private List<String> cookiesToInclude

    private final CloseableHttpClient httpClient
    private CredentialsProvider credsProvider
    private AuthCache authCache
    private final HttpHost httpHost
    private final HttpHost proxyHost
    private final String context
    private final String username
    private final String password
    private final int numConnections
    private static final Logger LOG = LoggerFactory.getLogger(JsonHttpProxy.class)

    JsonHttpProxy(HttpHost httpHost, String context, String username = null, String password = null, int numConnections = 100)
    {
        this.httpHost = httpHost
        proxyHost = null
        this.context = context
        this.username = username
        this.password = password
        this.numConnections = numConnections
        httpClient = createClient()
        createAuthCache()
    }

    JsonHttpProxy(HttpHost httpHost, HttpHost proxyHost, String context, String username = null, String password = null, int numConnections = 100)
    {
        this.httpHost = httpHost
        this.proxyHost = proxyHost
        this.context = context
        this.username = username
        this.password = password
        this.numConnections = numConnections
        httpClient = createClient()
        createAuthCache()
    }

    /**
     * Creates the client object with the proxy and cookie store for later use.
     *
     * @return A {@link CloseableHttpClient} 
     */
    protected CloseableHttpClient createClient()
    {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
        cm.maxTotal = numConnections // Max total connection
        cm.defaultMaxPerRoute = numConnections // Default max connection per route
        cm.setMaxPerRoute(new HttpRoute(httpHost), numConnections) // Max connections per route

        RequestConfig.Builder configBuilder = RequestConfig.custom()
        configBuilder.connectTimeout = 10 * 1000
        configBuilder.connectionRequestTimeout = 10 * 1000
        configBuilder.socketTimeout = 420 * 1000
        RequestConfig config = configBuilder.build()

        HttpClientBuilder builder = HttpClientBuilder.create()
        builder.defaultRequestConfig = config
        builder.connectionManager = cm
        builder.disableCookieManagement()
        builder.retryHandler = new StandardHttpRequestRetryHandler(5, true)

        if (proxyHost)
        {
            builder.proxy = proxyHost
        }

        CloseableHttpClient httpClient = builder.build()
        return httpClient
    }

    Object call(String bean, String methodName, List args)
    {
        Object[] params = args.toArray()
        FastByteArrayOutputStream stream = new FastByteArrayOutputStream(1024)
        JsonWriter writer = new JsonWriter(new AdjustableGZIPOutputStream(stream, Deflater.BEST_SPEED))
        writer.write(params)
        writer.flush()
        writer.close()

        if (LOG.debugEnabled)
        {
            LOG.debug("${bean}.${MetaUtils.getLogMessage(methodName, params, LOG_ARG_LENGTH)}")
        }

        HttpClientContext clientContext = HttpClientContext.create()
        HttpPost request = new HttpPost("${httpHost.toURI()}/${context}/cmd/${bean}/${methodName}")
        if (username && password)
        {
            clientContext.credentialsProvider = credsProvider
            clientContext.authCache = authCache
        }
        else
        {
            assignCookieHeader(request)
        }
        request.setHeader(USER_AGENT, 'ncube')
        request.setHeader(ACCEPT, APPLICATION_JSON.mimeType)
        request.setHeader(ACCEPT_ENCODING, 'gzip, deflate')
        request.setHeader(CONTENT_TYPE, "application/json; charset=UTF-8")
        request.setHeader(CONTENT_ENCODING, 'gzip')
        request.entity = new ByteArrayEntity(stream.toByteArrayUnsafe(), 0, stream.size())

        HttpResponse response = httpClient.execute(request, clientContext)
        request.entity = null
        boolean parsedJsonOk = false
        try
        {
            JsonReader reader = new JsonReader(new BufferedInputStream(response.entity.content))
            Map envelope = reader.readObject() as Map
            reader.close()
            parsedJsonOk = true
            
            if (envelope.exception != null)
            {
                throw envelope.exception
            }
            if (!envelope.status)
            {
                String msg
                if (envelope.data instanceof String)
                {
                    msg = envelope.data
                }
                else if (envelope.data != null)
                {
                    msg = envelope.data.toString()
                }
                else
                {
                    msg = 'no extra info provided.'
                }
                throw new RuntimeException("REST call [${bean}.${methodName}] indicated failure on server: ${msg}")
            }
            return envelope.data
        }
        catch (ThreadDeath t)
        {
            throw t
        }
        catch (Throwable e)
        {
            if (!parsedJsonOk)
            {
                LOG.warn("Failed to process response (code: ${response.statusLine.statusCode}) from server with call: ${bean}.${MetaUtils.getLogMessage(methodName, args.toArray(), LOG_ARG_LENGTH)}, headers: ${request.allHeaders}")
            }
            throw e
        }
    }

    private void assignCookieHeader(HttpPost proxyRequest)
    {
        HttpServletRequest servletRequest = JsonCommandServlet.servletRequest.get()
        if (servletRequest instanceof HttpServletRequest)
        {
            Cookie[] cookies = servletRequest.cookies
            if (cookies == null)
            {
                return
            }
            StringJoiner joiner = new StringJoiner("; ")
            for (Cookie cookie: cookies)
            {
                if (cookiesToInclude.contains(cookie.name))
                {
                    joiner.add("${cookie.name}=${cookie.value}")
                }
            }
            if (joiner.length())
            {
                proxyRequest.setHeader('Cookie', joiner.toString())
            }
        }
    }

    private void createAuthCache()
    {
        if (username && password)
        {
            credsProvider = new BasicCredentialsProvider()
            AuthScope authScope = new AuthScope(httpHost.hostName, httpHost.port)
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password)
            credsProvider.setCredentials(authScope, credentials)
            authCache = new BasicAuthCache()
            authCache.put(httpHost, new BasicScheme())
        }
    }
}
