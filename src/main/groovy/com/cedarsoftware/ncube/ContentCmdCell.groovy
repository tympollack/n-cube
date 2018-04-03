package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.CdnRouter
import com.cedarsoftware.util.IOUtilities
import com.cedarsoftware.util.UrlUtilities
import groovy.transform.CompileStatic
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.springframework.util.FastByteArrayOutputStream

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.ConcurrentHashMap

/**
 * This class represents any cell that needs to return content from a URL.
 * For example, String or Binary content.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
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
@CompileStatic
abstract class ContentCmdCell extends UrlCommandCell
{
    private static Map<String, String> extToMimeType = new ConcurrentHashMap<>()
    private static final Logger LOG = LoggerFactory.getLogger(ContentCmdCell.class)

    static
    {
        extToMimeType['.css'] = 'text/css'
        extToMimeType['.html'] = 'text/html'
        extToMimeType['.js'] = 'application/javascript"'
        extToMimeType['.xml'] = 'application/xml'
        extToMimeType['.json'] = 'application/json'
        extToMimeType['.jpg'] = 'image/jpeg'
        extToMimeType['.png'] = 'image/png'
        extToMimeType['.gif'] = 'image/gif'
        extToMimeType['.bmp'] = 'image/bmp'
    }

    //  constructor only for serialization.
    ContentCmdCell() {}

    ContentCmdCell(String cmd, String url, boolean cacheContent)
    {
        super(cmd, url, cacheContent)
    }

    protected def fetchResult(Map<String, Object> ctx)
    {
        Object data;

        if (url == null)
        {
            data = cmd
        }
        else
        {
            data = fetchContentFromUrl(ctx)
        }

        return executeInternal(data, ctx)
    }

    protected def executeInternal(Object data, Map<String, Object> ctx)
    {
        return data
    }

    protected def fetchContentFromUrl(Map ctx)
    {
        Map input = getInput(ctx)
        if (input.containsKey(CdnRouter.HTTP_REQUEST) && input.containsKey(CdnRouter.HTTP_RESPONSE))
        {
            return proxyFetch(ctx)
        }
        else
        {
            return simpleFetch(ctx)
        }
    }

    protected def simpleFetch(Map ctx)
    {
        NCube cube = getNCube(ctx)
        URL u = getActualUrl(ctx)

        // Try to load twice.
        for (int i=0; i < 2; i++)
        {
            try
            {
                return grab(u)
            }
            catch (Exception e)
            {
                final String className = getClass().simpleName
                String errorMsg = 'url: ' + url + ', n-cube: ' + cube.name + ', app: ' + cube.applicationID
                if (i == 1)
                {   // Note: Error is not marked - it will be retried in the future
                    String msg = 'Unable to load content from ' + errorMsg
                    LOG.warn(className + ': failed 2nd attempt [will retry on future attempts] unable to fetch contents, ' + errorMsg)
                    throw new IllegalStateException(msg, e)
                }
                else
                {
                    LOG.warn(className + ': retrying fetch, ' + errorMsg)
                    Thread.sleep(150)
                }
            }
        }
        // Will never happen - loop will throw exception if 2nd attempt fails
        return null
    }

    protected Object grab(URL u)
    {
        return UrlUtilities.getContentFromUrlAsString(u, true)
    }

    protected Object proxyFetch(Map ctx)
    {
        Map input = getInput(ctx)
        HttpServletRequest request = (HttpServletRequest) input[CdnRouter.HTTP_REQUEST]
        HttpServletResponse response = (HttpServletResponse) input[CdnRouter.HTTP_RESPONSE]
        HttpURLConnection conn = null
        URL actualUrl = null

        try
        {
            actualUrl = getActualUrl(ctx)
            HttpURLConnection.followRedirects = true
            URLConnection connection = actualUrl.openConnection()
            if (!(connection instanceof HttpURLConnection))
            {   // Handle a "file://" URL
                connection.connect()
                addFileHeader(actualUrl, response)
                return transferFromServer(connection, response)
            }
            conn = (HttpURLConnection) connection
            conn.allowUserInteraction = false
            conn.requestMethod = "GET"
            conn.doOutput = true
            conn.doInput = true
            conn.readTimeout = 20000
            conn.connectTimeout = 10000

            setupRequestHeaders(conn, request)
            conn.connect()
            // Note, could transfer content from requestor on (info besides header)
            // Note, could transfer POST requests

            int resCode = conn.responseCode

            if (resCode <= HttpServletResponse.SC_PARTIAL_CONTENT)
            {
                transferResponseHeaders(conn, response)
                return transferFromServer(conn, response)
            }
            else
            {
                UrlUtilities.readErrorResponse(conn)
                response.sendError(resCode, conn.responseMessage)
                return null
            }
        }
        catch (SocketTimeoutException e)
        {
            try
            {
                LOG.warn("Socket time out occurred fetching: " + actualUrl, e)
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "File not found: " + actualUrl.toString())
            }
            catch (IOException ignore) { }
        }
        catch (Exception e)
        {
            try
            {
                LOG.error("Error occurred fetching: " + actualUrl, e)
                UrlUtilities.readErrorResponse(conn)
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.message)
            }
            catch (IOException ignored) { }
        }
        return null
    }

    private Object transferFromServer(URLConnection conn, HttpServletResponse response) throws IOException
    {
        InputStream input = null
        OutputStream out = null
        try
        {
            input = new BufferedInputStream(conn.inputStream, 32768)
            if (cacheable)
            {
                input = new CachingInputStream(input)
            }
            out = response.outputStream
            IOUtilities.transfer(input, out)

            return cacheable ? ((CachingInputStream) input).streamCache : null     // must call .getStreamCache() with CompileStatic
        }
        finally
        {
            IOUtilities.close(input)
            IOUtilities.close(out)
        }
    }

    private static void setupRequestHeaders(URLConnection c, HttpServletRequest request)
    {
        Enumeration headerNames = request.headerNames
        while (headerNames.hasMoreElements())
        {
            String key = (String) headerNames.nextElement()
            String value = request.getHeader(key)
            c.setRequestProperty(key, value)
        }
    }

    private static void transferResponseHeaders(URLConnection c, HttpServletResponse response)
    {
        Map<String, List<String>> headerFields = c.headerFields
        Set<Map.Entry<String, List<String>>> entries = headerFields.entrySet()

        for (Map.Entry<String, List<String>> entry : entries)
        {
            if (entry.value != null && entry.key != null)
            {
                for (String s : entry.value)
                {
                    if (!"X-Frame-Options".equalsIgnoreCase(s))
                    {
                        response.addHeader(entry.key, s)
                    }
                }
            }
        }
    }

    private static String getExtension(String urlPath)
    {
        int index = urlPath == null ? -1 : urlPath.lastIndexOf(EXTENSION_SEPARATOR as int)
        return index == -1 ? null : urlPath.substring(index).intern()
    }

    static void addFileHeader(URL actualUrl, HttpServletResponse response)
    {
        if (actualUrl == null)
        {
            return
        }

        String ext = getExtension(actualUrl.toString().toLowerCase())
        String mime = extToMimeType[ext]

        if (mime == null)
        {
            return
        }

        response.addHeader("content-type", mime)
    }

    static class CachingInputStream extends FilterInputStream
    {
        FastByteArrayOutputStream streamCache = new FastByteArrayOutputStream()

        /**
         * Creates a {@code FilterInputStream}
         * by assigning the  argument {@code in}
         * to the field {@code this.in} so as
         * to remember it for later use.
         * @param in the underlying input stream, or {@code null} if
         *           this instance is to be created without an underlying stream.
         */
        protected CachingInputStream(InputStream input)
        {
            super(input)
        }

        int read(byte[] b, int off, int len) throws IOException
        {
            int count = super.read(b, off, len)
            if (count != -1)
            {
                streamCache.write(b, off, count)
            }
            return count
        }

        int read() throws IOException
        {
            int result = super.read()
            if (result != -1)
            {
                streamCache.write(result)
            }
            return result
        }

        byte[] getStreamCache()
        {
            return streamCache.toByteArrayUnsafe()
        }
    }
}
