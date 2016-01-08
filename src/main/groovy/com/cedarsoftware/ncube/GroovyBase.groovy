package com.cedarsoftware.ncube
import com.cedarsoftware.util.EncryptionUtilities
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.UrlUtilities
import groovy.transform.CompileStatic
import ncube.grv.exp.NCubeGroovyExpression
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.regex.Matcher
/**
 * Base class for Groovy CommandCells.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
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
public abstract class GroovyBase extends UrlCommandCell
{
    private static final Logger LOG = LogManager.getLogger(GroovyBase.class)
    protected transient String cmdHash
    private volatile transient Class runnableCode = null
    private static final ConcurrentMap<ApplicationID, ConcurrentMap<String, Class>>  compiledClasses = new ConcurrentHashMap<>()

    //  Private constructor only for serialization.
    protected GroovyBase() {}

    public GroovyBase(String cmd, String url, boolean cache)
    {
        super(cmd, url, cache)
    }

    public Class getRunnableCode()
    {
        return runnableCode
    }

    public void setRunnableCode(Class runnableCode)
    {
        this.runnableCode = runnableCode
    }

    protected Object fetchResult(Map<String, Object> ctx)
    {
        Object data = null

        if (getUrl() == null)
        {
            data = getCmd()
        }

        prepare(data, ctx)
        Object result = executeInternal(ctx)
        if (isCacheable())
        {
            // Remove the compiled class from Groovy's internal cache after executing it.
            // This is because the cell is marked as cacheable, so there is no need to
            // hold a reference to the compiled class.  Also remove our reference
            // (runnableCode = null). Internally, the class, constructor, and run() method
            // are not cached when the cell is marked cache:true.
            ClassLoader cl = getRunnableCode().getClassLoader().getParent()
            if (cl instanceof GroovyClassLoader)
            {
                GroovyClassLoader gcl = (GroovyClassLoader) cl
                GroovySystem.getMetaClassRegistry().removeMetaClass(getRunnableCode())
                try
                {
                    Method remove = GroovyClassLoader.class.getDeclaredMethod("removeClassCacheEntry", String.class)
                    remove.setAccessible(true)
                    remove.invoke(gcl, getRunnableCode().getName())
                }
                catch (Exception e)
                {
                    LOG.warn("Unable to remove cached GroovyExpression from GroovyClassLoader", e)
                }
            }
            setRunnableCode(null)
        }
        return result
    }

    protected abstract String buildGroovy(Map<String, Object> ctx, String theirGroovy)

    static void clearCache(ApplicationID appId)
    {
        Map<String, Class> compiledMap = getCache(appId, compiledClasses)
        compiledMap.clear()
    }

    protected static <T> Map<String, T> getCache(ApplicationID appId, ConcurrentMap<ApplicationID, ConcurrentMap<String, T>> container)
    {
        ConcurrentMap<String, T> map = container[appId]

        if (map == null)
        {
            map = new ConcurrentHashMap<>()
            ConcurrentMap mapRef = container.putIfAbsent(appId, map)
            if (mapRef != null)
            {
                map = mapRef
            }
        }
        return map
    }

    protected Object executeInternal(Map<String, Object> ctx)
    {
        try
        {
            final NCubeGroovyExpression exp = (NCubeGroovyExpression) getRunnableCode().newInstance()
            exp.input = getInput(ctx)
            exp.output = getOutput(ctx)
            exp.ncube = getNCube(ctx)
            return invokeRunMethod(exp, ctx)
        }
        catch (InvocationTargetException e)
        {
            throw e.getTargetException()
        }
    }

    /**
     * Fetch constructor (from cache, if cached) and instantiate GroovyExpression
     */
    protected abstract Object invokeRunMethod(NCubeGroovyExpression instance, Map<String, Object> ctx) throws Throwable

    /**
     * Conditionally compile the passed in command.  If it is already compiled, this method
     * immediately returns.  Insta-check because it is just a ref == null check.
     */
    public void prepare(Object data, Map<String, Object> ctx)
    {
        if (getRunnableCode() != null)
        {   // If the code for the cell has already been compiled, return the compiled class.
            return
        }

        computeCmdHash(data, ctx)
        NCube cube = getNCube(ctx)
        Map<String, Class> compiledMap = getCache(cube.applicationID, compiledClasses)

        if (compiledMap.containsKey(cmdHash))
        {   // Already been compiled, re-use class (different cell, but has identical source or URL as other expression).
            setRunnableCode(compiledMap[cmdHash])
            return
        }

        synchronized(GroovyBase.class)
        {
            if (compiledMap.containsKey(cmdHash))
            {   // Already been compiled, re-use class (different cell, but has identical source or URL as other expression).
                setRunnableCode(compiledMap[cmdHash])
                return
            }
            Class groovyCode = compile(ctx)
            setRunnableCode(groovyCode)
            if (!isCacheable())
            {
                compiledMap[cmdHash] = getRunnableCode()
            }
        }
    }

    /**
     * Compute SHA1 hash for this CommandCell.  The tricky bit here is that the command can be either
     * defined inline or via a URL.  If defined inline, then the command hash is SHA1(command text).  If
     * defined through a URL, then the command hash is SHA1(command URL + GroovyClassLoader URLs.toString).
     */
    private void computeCmdHash(Object data, Map<String, Object> ctx)
    {
        String content
        if (getUrl() == null)
        {
            content = data != null ? data.toString() : ""
        }
        else
        {   // specified via URL, add classLoader URL strings to URL for SHA-1 source.
            NCube cube = getNCube(ctx)
            GroovyClassLoader gcLoader = (GroovyClassLoader) NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            URL[] urls = gcLoader.getURLs()
            StringBuilder s = new StringBuilder()
            for (URL url : urls)
            {
                s.append(url.toString())
                s.append('.')
            }
            s.append(getUrl())
            content = s.toString()
        }
        cmdHash = EncryptionUtilities.calculateSHA1Hash(StringUtilities.getBytes(content, "UTF-8"))
    }

    protected Class compile(Map<String, Object> ctx)
    {
        NCube cube = getNCube(ctx)
        String url = getUrl()
        boolean isUrlUsed = StringUtilities.hasContent(url)
        if (isUrlUsed && url.endsWith(".groovy"))
        {
            // If a class exists already with the same name as the groovy file (substituting slashes for dots),
            // then attempt to find and return that class without going through the resource location and parsing
            // code. This can be useful, for example, if a build process pre-builds and load coverage enhanced
            // versions of the classes.
            try
            {
                String className = url.substring(0, url.indexOf(".groovy"))
                className = className.replace('/', '.')
                return Class.forName(className)
            }
            catch (Exception ignored)
            { }
        }

        String grvSrcCode
        GroovyClassLoader gcLoader

        if (cube.getName().toLowerCase().startsWith("sys."))
        {   // No URLs allowed, nor code from sys.classpath when executing these cubes
            gcLoader = (GroovyClassLoader)NCubeManager.getLocalClassloader(cube.applicationID)
            grvSrcCode = getCmd()
        }
        else if (isUrlUsed)
        {
            gcLoader = (GroovyClassLoader)NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            URL groovySourceUrl = getActualUrl(ctx)
            grvSrcCode = StringUtilities.createString(UrlUtilities.getContentFromUrl(groovySourceUrl, true), "UTF-8")
        }
        else
        {
            gcLoader = (GroovyClassLoader)NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            grvSrcCode = getCmd()
        }
        String groovySource = expandNCubeShortCuts(buildGroovy(ctx, grvSrcCode))
//        Thread.currentThread().setContextClassLoader(gcLoader)
        return gcLoader.parseClass(groovySource)
    }

    static String expandNCubeShortCuts(String groovy)
    {
        Matcher m = Regexes.groovyAbsRefCubeCellPattern.matcher(groovy)
        String exp = m.replaceAll('$1getFixedCubeCell(\'$2\',$3)')

        m = Regexes.groovyAbsRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1getFixedCubeCell(\'$2\',$3)')

        m = Regexes.groovyAbsRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1getFixedCell($2)')

        m = Regexes.groovyAbsRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1getFixedCell($2)')

        m = Regexes.groovyRelRefCubeCellPattern.matcher(exp)
        exp = m.replaceAll('$1getRelativeCubeCell(\'$2\',$3)')

        m = Regexes.groovyRelRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1getRelativeCubeCell(\'$2\',$3)')

        m = Regexes.groovyRelRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1getRelativeCell($2)')

        m = Regexes.groovyRelRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1getRelativeCell($2)')
        return exp
    }

    public void getCubeNamesFromCommandText(final Set<String> cubeNames)
    {
        getCubeNamesFromText(cubeNames, getCmd())
    }

    static void getCubeNamesFromText(final Set<String> cubeNames, final String text)
    {
        if (StringUtilities.isEmpty(text))
        {
            return
        }

        Matcher m = Regexes.groovyAbsRefCubeCellPattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyAbsRefCubeCellPatternA.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyRelRefCubeCellPattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyRelRefCubeCellPatternA.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyExplicitCubeRefPattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyExplicitRunRulePattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyExplicitJumpPattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }
    }

    /**
     * Find all occurrences of 'input.variableName' in the Groovy code
     * and add the variableName as a scope (key).
     * @param scopeKeys Set to add required scope keys to.
     */
    public void getScopeKeys(Set<String> scopeKeys)
    {
        Matcher m = Regexes.inputVar.matcher(getCmd())
        while (m.find())
        {
            scopeKeys.add(m.group(2))
        }
    }

    public static Set<String> getImports(String text, StringBuilder newGroovy)
    {
        Matcher m = Regexes.importPattern.matcher(text)
        Set<String> importNames = new LinkedHashSet<>()
        while (m.find())
        {
            importNames.add(m.group(0))  // based on Regex pattern - if pattern changes, this could change
        }

        m.reset()
        newGroovy.append(m.replaceAll(''))
        return importNames
    }
}
