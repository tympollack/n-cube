package com.cedarsoftware.ncube
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.util.concurrent.atomic.AtomicBoolean
/**
 * @author John DeRegnaucourt (jdereg@gmail.com)
 * @author Ken Partlow (kpartlow@gmail.com)
 * <br>
 * Copyright (c) Cedar Software LLC
 * <br><br>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <br><br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br><br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@CompileStatic
abstract class UrlCommandCell implements CommandCell
{
    private static final Logger LOG = LogManager.getLogger(UrlCommandCell.class)
    private String cmd
    private volatile transient String errorMsg = null
    private final String url
    private int hash
    public static final char EXTENSION_SEPARATOR = '.'
    private AtomicBoolean hasBeenCached = new AtomicBoolean(false)
    protected Object cache
    // would prefer this was a final
    private boolean cacheable

    //  Private constructor only for serialization.
    protected UrlCommandCell() { url = null }

    UrlCommandCell(String cmd, String url, boolean cacheable)
    {
        if (cmd == null && url == null)
        {
            throw new IllegalArgumentException("Both 'cmd' and 'url' cannot be null")
        }

        if (cmd != null && cmd.isEmpty())
        {   // Because of this, cmdHash() never has to worry about an empty ("") command (when url is null)
            throw new IllegalArgumentException("'cmd' cannot be empty")
        }

        this.cmd = cmd
        this.url = url
        this.cacheable = cacheable
        this.hash = cmd == null ? url.hashCode() : cmd.hashCode()
    }

    String getUrl()
    {
        return url
    }

    boolean isCacheable()
    {
        return cacheable
    }

    void clearClassLoaderCache()
    {
        if (cache == null)
        {
            return
        }

        synchronized (GroovyBase.class)
        {
            if (cache == null)
            {
                return
            }

            // classpath case, lets clear all classes before setting to null.
            if (cache instanceof GroovyClassLoader)
            {
                ((GroovyClassLoader)cache).clearCache()
            }
            cache = null
        }
    }

    protected URL getActualUrl(Map<String, Object> ctx)
    {
        for (int i=0; i < 2; i++)
        {   // Try URL resolution twice (HTTP HEAD called for connecting relative URLs to sys.classpath)
            try
            {
                return NCubeManager.getActualUrl(getNCube(ctx).getApplicationID(), url, getInput(ctx))
            }
            catch(Exception e)
            {
                NCube cube = getNCube(ctx)
                if (i == 1)
                {   // Note: Error is marked, it will not be retried in the future
                    setErrorMessage("Invalid URL in cell (malformed or cannot resolve given classpath): " + getUrl() + ", cube: " + cube.getName() + ", app: " + cube.applicationID)
                    LOG.warn(getClass().getSimpleName() + ': failed 2nd attempt [will NOT retry in future] getActualUrl() - unable to resolve against sys.classpath, url: ' + getUrl() + ", cube: " + cube.getName() + ", app: " + cube.applicationID)
                    throw new IllegalStateException(getErrorMessage(), e)
                }
                else
                {
                    LOG.warn(getClass().getSimpleName() + ': retrying getActualUrl() - unable to resolve against sys.classpath, url: ' + getUrl() + ", cube: " + cube.getName() + ", app: " + cube.applicationID)
                    Thread.sleep(100)
                }
            }
        }
    }

    static NCube getNCube(Map<String, Object> ctx)
    {
        return (NCube) ctx.ncube
    }

    static Map getInput(Map<String, Object> ctx)
    {
        return (Map) ctx.input
    }

    static Map getOutput(Map<String, Object> ctx)
    {
        return (Map) ctx.output
    }

    boolean equals(Object other)
    {
        if (!(other instanceof UrlCommandCell))
        {
            return false
        }

        UrlCommandCell that = (UrlCommandCell) other

        if (cmd != null)
        {
            return cmd.equals(that.cmd)
        }

        return url.equals(that.getUrl())
    }

    int hashCode()
    {
        return this.hash
    }

    String getCmd()
    {
        return cmd
    }

    String toString()
    {
        return url == null ? cmd : url
    }

    void failOnErrors()
    {
        if (errorMsg != null)
        {
            throw new IllegalStateException(errorMsg)
        }
    }

    void setErrorMessage(String msg)
    {
        errorMsg = msg
    }

    String getErrorMessage()
    {
        return errorMsg
    }

    int compareTo(CommandCell cmdCell)
    {
        String cmd1 = cmd == null ? '' : cmd
        String cmd2 = cmdCell.getCmd() == null ? '' : cmdCell.getCmd()

        int comp = cmd1.compareTo(cmd2)

        if (comp == 0)
        {
            String url1 = url == null ? '' : url
            String url2 = cmdCell.getUrl() == null ? '' : cmdCell.getUrl()
            return url1.compareTo(url2)
        }

        return comp
    }

    void getCubeNamesFromCommandText(Set<String> cubeNames)
    {
    }

    void getScopeKeys(Set<String> scopeKeys)
    {
    }

    Object execute(Map<String, Object> ctx)
    {
        failOnErrors()

        if (!isCacheable())
        {
            return fetchResult(ctx)
        }

        if (hasBeenCached.get())
        {
            return cache
        }

        cache = fetchResult(ctx)
        hasBeenCached.set(true)
        return cache
    }

    protected abstract Object fetchResult(Map<String, Object> ctx)
}
