package com.cedarsoftware.ncube.util

import com.cedarsoftware.ncube.NCubeBaseTest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component

import java.util.regex.Pattern

@Component
class ServletWebServerListener implements ApplicationListener<ServletWebServerInitializedEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(ServletWebServerListener.class)
    private static Pattern leadingSlash = ~/^[\/]?/
    private static Pattern trailingSlash = ~/[\/]?$/

    @Value('${server.servlet.contextPath}')
    private String contextPath

    // allow for testing against a remote URL instead of embedded Tomcat (assuming static files available)
    @Value('${ncube.tests.baseRemoteUrl:}')
    private String baseRemoteUrl

    @Override
    void onApplicationEvent(ServletWebServerInitializedEvent event) {
        if (baseRemoteUrl)
        {
            NCubeBaseTest.baseRemoteUrl = baseRemoteUrl - leadingSlash
        }
        else
        {
            String host = 'localhost'
            int port = event.webServer.port
            String context = contextPath - leadingSlash - trailingSlash
            NCubeBaseTest.baseRemoteUrl = "http://${host}:${port}/${context}"
        }

        LOG.info("baseRemoteUrl set to: ${NCubeBaseTest.baseRemoteUrl}")
    }
}
