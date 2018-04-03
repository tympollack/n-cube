package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.CdnRouter
import com.cedarsoftware.ncube.util.CdnRoutingProvider
import groovy.transform.CompileStatic
import org.junit.Test
import org.mockito.Mockito
import org.springframework.util.FastByteArrayOutputStream

import javax.servlet.ReadListener
import javax.servlet.ServletInputStream
import javax.servlet.ServletOutputStream
import javax.servlet.WriteListener
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import static com.cedarsoftware.ncube.NCubeAppContext.ncubeRuntime
import static org.mockito.Matchers.anyString
import static org.mockito.Mockito.doThrow
import static org.mockito.Mockito.eq
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify
import static org.mockito.Mockito.when

/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the 'License')
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an 'AS IS' BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class TestCdnRouter extends NCubeCleanupBaseTest
{
    @Test
    void testRoute()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setDefaultCdnRoutingProvider()

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        byte[] bytes = ((DumboOutputStream) out).bytes
        String s = new String(bytes)
        assert 'CAFEBABE' == s
    }

    @Test
    void test500()
    {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class)
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class)

        when(request.servletPath).thenReturn '/dyn/view/500'
        setupMockRequestHeaders(request)
        setupMockResponseHeaders(response)

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn(out)
        when(request.inputStream).thenReturn(input)

        setDefaultCdnRoutingProvider()

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route(request, response)

        verify(response, times(1)).sendError(500, 'Invalid URL in cell (unable to resolve against sys.classpath), url: tests/does/not/exist/index.html, cube: CdnRouterTest, app: NONE/DEFAULT_APP/999.99.9/SNAPSHOT/TEST/')
    }

    @Test
    void testInvalidVersion()
    {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class)
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class)

        when(request.servletPath).thenReturn '/foo/bar'

        setDefaultCdnRoutingProvider()

        new CdnRouter().route request, response
        verify(response, times(1)).sendError 400, 'CdnRouter - Invalid ServletPath request: /foo/bar'
    }

    @Test
    void testNullServletPathThoughItMayBeImpossibleToReproduceInTomcat()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn null

        setDefaultCdnRoutingProvider()

        new CdnRouter().route request, response
        verify(response, times(1)).sendError 400, 'CdnRouter - Invalid ServletPath request: null'
    }

    @Test
    void testInvalidCubeName()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/404'
        setupMockRequestHeaders(request)
        setupMockResponseHeaders(response)

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        new CdnRouter().route request, response

        verify(response, times(1)).sendError 500, 'CdnRouter - Error occurred: Could not load routing cube using app: NONE/DEFAULT_APP/999.99.9/SNAPSHOT/TEST/, cube name: foo'
    }

    @Test
    void test404()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/404'
        setupMockRequestHeaders request

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setDefaultCdnRoutingProvider()

        ApplicationID appId = new ApplicationID(ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ApplicationID.DEFAULT_STATUS, ApplicationID.TEST_BRANCH)
        ncubeRuntime.getUrlClassLoader(appId, [:])
        createRuntimeCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')

        CdnRouter router = new CdnRouter()
        router.route(request, response)

        verify(response, times(1)).sendError(eq(404), anyString())
    }

    @Test
    void testCdnRouterErrorHandleNoCubeName()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", null, true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.cubeName\' in the Map coordinate.'
    }

    @Test
    void testCdnRouterErrorHandleNoTenant()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider null, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.tenant\' in the Map coordinate.'
    }

    @Test
    void testCdnRouterErrorHandleNoApp()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, null, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.app\' in the Map coordinate.'
    }

    @Test
    void testCdnRouterErrorHandleNoVersion()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, null, ReleaseStatus.SNAPSHOT.name(), "TEST", 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.version\' in the Map coordinate.'
    }

    @Test
    void testCdnRouterErrorHandleNoStatus()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, null, "TEST", 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.status\' in the Map coordinate.'
    }

    @Test
    void testCdnRouterErrorHandleNoBranch()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setCdnRoutingProvider ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), null, 'foo', true

        ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')
        CdnRouter router = new CdnRouter()
        router.route request, response
        verify(response, times(1)).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - CdnRoutingProvider did not set up \'router.branch\' in the Map coordinate.'
    }

    private static class TestCdnRoutingProvider implements CdnRoutingProvider
    {
        final String tenant
        final String app
        final String version
        final String status
        final String branch
        final String cubeName
        final boolean isAuthorized

        TestCdnRoutingProvider(String tenant, String app, String version, String status, String branch, String cubeName, boolean isAuthorized)
        {
            this.tenant = tenant
            this.app = app
            this.version = version
            this.status = status
            this.branch = branch
            this.cubeName = cubeName
            this.isAuthorized = isAuthorized
        }

        void setupCoordinate(Map coord)
        {
            coord[CdnRouter.TENANT] = tenant
            coord[CdnRouter.APP] = app
            coord[CdnRouter.CUBE_VERSION] = version
            coord[CdnRouter.STATUS] = status
            coord[CdnRouter.BRANCH] = branch
            coord[CdnRouter.CUBE_NAME] = cubeName
        }

        boolean isAuthorized(String type)
        {
            return isAuthorized
        }
    }
    private static void setCdnRoutingProvider(String tenant, String app, String version, String status, String branch, String cubeName, boolean isAuthorized)
    {
        CdnRouter.cdnRoutingProvider = new TestCdnRoutingProvider(tenant, app, version, status, branch, cubeName, isAuthorized)
    }

    private static void setDefaultCdnRoutingProvider()
    {
        setCdnRoutingProvider(ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", 'CdnRouterTest', true)
    }

    @Test
    void testNotAuthorized()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/index'
        when(request.requestURL).thenReturn new StringBuffer('http://www.foo.com/dyn/view/index')

        setCdnRoutingProvider(ApplicationID.DEFAULT_TENANT, ApplicationID.DEFAULT_APP, ApplicationID.DEFAULT_VERSION, ReleaseStatus.SNAPSHOT.name(), "TEST", 'CdnRouterTest', false)

        new CdnRouter().route request, response
        verify(response, times(1)).sendError 401, 'CdnRouter - Unauthorized access, request: http://www.foo.com/dyn/view/index'
    }


    @Test
    void testContentTypeTransfer()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/xml'

        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()
        when(request.inputStream).thenReturn input
        when(response.outputStream).thenReturn out

        setDefaultCdnRoutingProvider()

        createRuntimeCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')

        CdnRouter router = new CdnRouter()
        router.route(request, response)
        byte[] bytes = ((DumboOutputStream) out).bytes
        String s = new String(bytes)
        assert '<cedarsoftware><jdereg name="john"/></cedarsoftware>' == s
        verify(response, times(1)).addHeader 'Content-Type', 'application/xml'
        verify(response, times(1)).addHeader 'Content-Length', '52'
    }

    private static void setupMockResponseHeaders(HttpServletResponse response)
    {
        when(response.containsHeader('Content-Length')).thenReturn true
        when(response.containsHeader('Last-Modified')).thenReturn true
        when(response.containsHeader('Expires')).thenReturn true
        when(response.containsHeader('Content-Encoding')).thenReturn true
        when(response.containsHeader('Content-Type')).thenReturn true
        when(response.containsHeader('Cache-Control')).thenReturn true
        when(response.containsHeader('Etag')).thenReturn true
    }

    private static void setupMockRequestHeaders(HttpServletRequest request)
    {
        Vector<String> v = new Vector<String>()
        v.add('Accept')
        v.add('Accept-Encoding')
        v.add('Accept-Language')
        v.add('User-Agent')
        v.add('Cache-Control')

        when(request.headerNames).thenReturn v.elements()
        when(request.getHeader('Accept')).thenReturn 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        when(request.getHeader('User-Agent')).thenReturn 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.102 Safari/537.36'
        when(request.getHeader('Accept-Encoding')).thenReturn 'gzip,deflate'
        when(request.getHeader('Accept-Language')).thenReturn 'n-US,en;q=0.8'
        when(request.getHeader('Cache-Control')).thenReturn 'max-age=60'
    }

    @Test
    void testExceptionOnException()
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenThrow new RuntimeException('foo')
        doThrow(new IOException('bar')).when(response).sendError HttpServletResponse.SC_INTERNAL_SERVER_ERROR, 'CdnRouter - Error occurred: Failure'

        setDefaultCdnRoutingProvider()
        new CdnRouter().route request, response
    }

    @Test
    void testFileContentTypeTransfer()
    {
        cdnRouteFile 'file', false
    }

    @Test
    void testFileContentTypeCacheTransfer()
    {
        cdnRouteFile 'cachedFile', true
    }

    private void cdnRouteFile(String logicalFileName, boolean mustMatch) throws IOException
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        when(request.servletPath).thenReturn '/dyn/view/' + logicalFileName
        setupMockRequestHeaders request
        setupMockResponseHeaders response

        ServletOutputStream out = new DumboOutputStream()
        ServletInputStream input = new DumboInputStream()

        when(response.outputStream).thenReturn out
        when(request.inputStream).thenReturn input

        setDefaultCdnRoutingProvider()

        NCube cube = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouterTest.json')

        new CdnRouter().route request, response
        byte[] bytes = ((DumboOutputStream) out).bytes
        String s = new String(bytes)
        assert '<html></html>' == s

        verify(response, times(1)).addHeader 'content-type', 'text/html'

        def coord = ['content.type':'view', 'content.name':logicalFileName] as Map
        String one = (String) cube.getCell(coord)
        String two = (String) cube.getCell(coord)

        if (mustMatch)
        {
            assert one.is(two)
        }
        else
        {
            assert !one.is(two)
        }
    }

    @Test
    void testDefaultRoute()
    {
        NCube router = ncubeRuntime.getNCubeFromResource(ApplicationID.testAppId, 'cdnRouter.json')

        Axis axis = router.getAxis('content.name')
        assert 5 == axis.columns.size()

        Map coord = new HashMap()
        coord['content.name'] = 'Glock'

        String answer = (String) router.getCell(coord)
        assert 6 == axis.columns.size()
        assert '<html>Glock</html>' == answer

        answer = (String) router.getCell(coord)
        assert 6 == axis.columns.size()
        assert '<html>Glock</html>' == answer

        coord.put('content.name', 'Smith n Wesson')
        answer = (String) router.getCell(coord)
        assert 7 == axis.columns.size()
        assert '<html>Smith n Wesson</html>' == answer
    }

    @Test
    void testWithNoProvider() throws IOException
    {
        HttpServletRequest request = Mockito.mock HttpServletRequest.class
        HttpServletResponse response = Mockito.mock HttpServletResponse.class

        CdnRouter.cdnRoutingProvider = null
        new CdnRouter().route request, response

        verify(response, times(1)).sendError 500, 'CdnRouter - CdnRoutingProvider has not been set into the CdnRouter.'
    }

    static class DumboOutputStream extends ServletOutputStream
    {
        FastByteArrayOutputStream bao = new FastByteArrayOutputStream()

        byte[] getBytes()
        {
            try
            {
                bao.flush()
            }
            catch (IOException ignored)
            {
            }
            return bao.toByteArrayUnsafe()
        }

        void write(int b) throws IOException
        {
            bao.write(b)
        }

        boolean isReady()
        {
            return false
        }

        void setWriteListener(WriteListener writeListener) {

        }
    }

    static class DumboInputStream extends ServletInputStream
    {
        ByteArrayInputStream bao = new ByteArrayInputStream(new byte[0])

        int read() throws IOException
        {
            return bao.read()
        }

        boolean isFinished()
        {
            return false
        }

        boolean isReady()
        {
            return false
        }

        void setReadListener(ReadListener readListener) {

        }
    }

}