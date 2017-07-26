package com.cedarsoftware.ncube.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.embedded.EmbeddedServletContainerInitializedEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component

import java.util.regex.Pattern

/**
 * Created by gmorefield on 7/24/17.
 */
@Component
class EmbeddedServletContainerListener implements ApplicationListener<EmbeddedServletContainerInitializedEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedServletContainerListener.class)
//    public static int port

    @Value('${server.contextPath}')
    private String contextPath

    static String hostStringAndContext

    @Override
    void onApplicationEvent(EmbeddedServletContainerInitializedEvent event) {
//        EmbeddedServletContainerListener.port = event.embeddedServletContainer.port

        String host = 'localhost'
        try
        {
            host = InetAddress.localHost.hostName
        }
        catch(UnknownHostException ignored)
        { }

        int port = event.embeddedServletContainer.port

        Pattern leadingSlash = ~/^[\/]?/
        Pattern trailingSlash = ~/[\/]?$/
        String context = contextPath - leadingSlash - trailingSlash

        hostStringAndContext = "http://${host}:${port}/${context}"
        LOG.info("EmbeddedServletContainer configured to listen at: ${hostStringAndContext}")
    }
}
