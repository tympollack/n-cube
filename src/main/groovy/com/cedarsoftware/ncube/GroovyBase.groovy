package com.cedarsoftware.ncube

import com.cedarsoftware.util.EncryptionUtilities
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.UrlUtilities
import groovy.transform.CompileStatic
import ncube.grv.exp.NCubeGroovyExpression
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.Phases
import org.codehaus.groovy.control.SourceUnit
import org.codehaus.groovy.tools.GroovyClass

import java.lang.reflect.InvocationTargetException
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
abstract class GroovyBase extends UrlCommandCell
{
    private static final Logger LOG = LogManager.getLogger(GroovyBase.class)
    protected transient String cmdHash
    private volatile transient Class runnableCode = null
    /**
     * This cache is 'per ApplicationID'.  This allows different applications to define the same
     * class (URL to groovy), yet have different source code for that class.
     */
    private static final ConcurrentMap<ApplicationID, ConcurrentMap<String, Class>>  compiledClasses = new ConcurrentHashMap<>()
    private static final TEMP_DIR = System.getProperty("java.io.tmpdir")

    static
    {
        new File("${TEMP_DIR}/src/main/groovy/").mkdirs()
        new File("${TEMP_DIR}/target/classes/").mkdirs()
    }

    //  Private constructor only for serialization.
    protected GroovyBase() {}

    GroovyBase(String cmd, String url, boolean cache)
    {
        super(cmd, url, cache)
    }

    /**
     * @return Class the compiled Class the code within this cell was compiled to (it could have been found
     * and re-used if the code is the same as another cell).  If the cell is marked cache=true, then this
     * method will return null. This is because the cell retains the cached value of the executed code and
     * therefore the class reference is no longer needed.
     */
    protected Class getRunnableCode()
    {
        return runnableCode
    }

    protected void setRunnableCode(Class runnableCode)
    {
        this.runnableCode = runnableCode
    }

    protected Object fetchResult(Map<String, Object> ctx)
    {
        Object data = null

        if (url == null)
        {
            data = cmd
        }

        prepare(data, ctx)
        Object result = executeInternal(ctx)
        if (cacheable)
        {   // Remove the compiled class from the cell's cache.
            // This is because the cell is marked as cacheable meaning the result of the
            // execution is cached, so there is no need to hold a reference to the compiled class.
            setRunnableCode(null)
        }
        return result
    }

    protected abstract String buildGroovy(Map<String, Object> ctx, String theirGroovy)

    protected static void clearCache(ApplicationID appId)
    {
        Map<String, Class> compiledMap = getCache(appId)
        compiledMap.clear()
    }

    protected static Map<String, Class> getCache(ApplicationID appId)
    {
        ConcurrentMap<String, Class> map = compiledClasses[appId]

        if (map == null)
        {
            map = new ConcurrentHashMap<>()
            ConcurrentMap mapRef = compiledClasses.putIfAbsent(appId, map)
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
            Class code = getRunnableCode()
            if (code == null)
            {
                NCube ncube = getNCube(ctx)
                throw new IllegalStateException("Code cleared while cell was executing, n-cube: ${ncube.name}, app: ${ncube.applicationID}")
            }
            final NCubeGroovyExpression exp = (NCubeGroovyExpression) code.newInstance()
            exp.input = getInput(ctx)
            exp.output = getOutput(ctx)
            exp.ncube = getNCube(ctx)
            return invokeRunMethod(exp, ctx)
        }
        catch (InvocationTargetException e)
        {
            throw e.targetException
        }
    }

    /**
     * Fetch constructor (from cache, if cached) and instantiate GroovyExpression
     */
    protected abstract Object invokeRunMethod(NCubeGroovyExpression instance, Map<String, Object> ctx) throws Throwable

    /**
     * Conditionally compile the passed in command.  If it is already compiled, this method
     * immediately returns.  Insta-check because it is just a reference check.
     */
    void prepare(Object data, Map<String, Object> ctx)
    {
        // check L1 cache
        if (getRunnableCode() != null)
        {   // If the code for the cell has already been compiled, do nothing.
            return
        }

        computeCmdHash(data, ctx)
        Map<String, Class> compiledMap = getCache(getNCube(ctx).applicationID)

        // check L2 cache
        if (compiledMap.containsKey(cmdHash))
        {   // Already been compiled, re-use class (different cell, but has identical source or URL as other expression).
            setRunnableCode(compiledMap[cmdHash])
            return
        }

        // Pre-compiled check (e.g. source code was pre-compiled and instrumented for coverage)
        Map ret = getClassLoaderAndSource(ctx)
        if (ret.gclass instanceof Class)
        {   // Found class matching URL fileName.groovy already in JVM
            setRunnableCode(ret.gclass as Class)
            return
        }

        // check L3 cache
        GroovyClassLoader gcLoader = ret.loader as GroovyClassLoader
        String groovySource = ret.source as String
        String sha1Source = EncryptionUtilities.calculateSHA1Hash(groovySource.bytes)
        File byteCode = new File("${TEMP_DIR}/target/classes/${sha1Source}.class")

        if (byteCode.exists())
        {
            synchronized (sha1Source.intern())
            {
                Class root = null
                new File("${TEMP_DIR}/target/classes/").eachFileMatch(~/^${sha1Source}.*\.class/) { File file ->
                    boolean isRoot = file.name.indexOf('$') == -1
                    byte[] fileBytes = file.bytes
                    Class clazz = defineClass(sha1Source, ret.loader as GroovyClassLoader, compiledMap, fileBytes, isRoot)
                    if (NCubeGroovyExpression.isAssignableFrom(clazz) && root == null)
                    {
                        root = clazz
                    }
                }
                if (root != null)
                {
                    setRunnableCode(root)
                    compiledMap[cmdHash] = root
                }
            }
            return
        }

        // Newly encountered source - compile the source and store it in L1, L2, and L3 caches
        compile(gcLoader, groovySource, sha1Source, ctx)
    }

    protected Class compile(GroovyClassLoader gcLoader, String groovySource, String sha1Source, Map<String, Object> ctx)
    {
        Map<String, Class> compiledMap = getCache(getNCube(ctx).applicationID)

        CompilerConfiguration compilerConfiguration = new CompilerConfiguration()
        compilerConfiguration.targetBytecode = '1.8'
        compilerConfiguration.debug = false
        compilerConfiguration.defaultScriptExtension = '.groovy'
        compilerConfiguration.optimizationOptions = [(CompilerConfiguration.INVOKEDYNAMIC): Boolean.TRUE]

        SourceUnit sourceUnit = new SourceUnit("ncube.grv.exp.N_${cmdHash}", groovySource, compilerConfiguration, gcLoader, null)

        CompilationUnit compilationUnit = new CompilationUnit()
        compilationUnit.addSource(sourceUnit)
        compilationUnit.configure(compilerConfiguration)
        compilationUnit.compile(Phases.CLASS_GENERATION)    // concurrent compile!

        List classes = compilationUnit.classes
        Class generatedClass = defineClasses(compiledMap, classes, gcLoader, groovySource, sha1Source)
        return generatedClass
    }

    protected Class defineClasses(Map<String, Class> compiledMap, List classes, GroovyClassLoader gcLoader, String groovySource, String sha1Source)
    {
        Map<GroovyClass, Class> classMap = [:]
        Class root = null

        synchronized(sha1Source.intern())
        {
            Class clazz = compiledMap[cmdHash]
            if (clazz != null)
            {   // Another thread defined and persisted the class while this thread was blocked...
                return clazz
            }

            // Step 1: Add the compiled class(es) to the ClassLoader
            for (int i = 0; i < classes.size(); i++)
            {
                GroovyClass gclass = classes[i] as GroovyClass
                boolean isRoot = gclass.name.indexOf('$') == -1
                clazz = defineClass(sha1Source, gcLoader, compiledMap, gclass.bytes, isRoot);
                classMap[gclass] = clazz
            }

            // Step 2: Write the classes to persisted cache.
            // This is performed second so that another thread does not find the bytes on disk from another thread,
            // before it had defined the classes.
            for (int i = 0; i < classes.size(); i++)
            {
                GroovyClass gclass = (GroovyClass) classes[i]
//                String className = getClassName(gclass.bytes)

                int dollarPos = gclass.name.indexOf('$')

                if (dollarPos == -1)
                {
                    clazz = classMap[gclass]
                    if (NCubeGroovyExpression.isAssignableFrom(clazz))
                    {
                        // persist main class file
                        File byteCode = new File("${TEMP_DIR}/target/classes/${sha1Source}.class")
                        if (!byteCode.exists())
                        {
                            byteCode.append(gclass.bytes)
                        }
                        if (root == null)
                        {
                            root = clazz
                        }
                    }
                }
                else
                {   // persister inner class(es)
                    File byteCode = new File("${TEMP_DIR}/target/classes/${sha1Source}${gclass.name.substring(dollarPos)}.class")
                    if (!byteCode.exists())
                    {
                        byteCode.append(gclass.bytes)
                    }
                }
            }

            if (root != null)
            {
                compiledMap[cmdHash] = root
                setRunnableCode(root)
            }
        }
        return compiledMap[cmdHash]
    }

    protected Class defineClass(String sha1Source, GroovyClassLoader gcLoader, Map<String, Class> cache, byte[] byteCode, boolean isRoot)
    {
        synchronized (sha1Source.intern())
        {
            Class clazz
            if (isRoot)
            {
                clazz = cache[cmdHash]
                if (clazz != null)
                {   // Another thread defined the class while this thread was blocked...
                    return clazz
                }
            }

            clazz = gcLoader.defineClass(null, byteCode)
            return clazz
        }
    }

    static String getClassName(byte[] byteCode) throws Exception
    {
        InputStream is = new ByteArrayInputStream(byteCode)
        DataInputStream dis = new DataInputStream(is)
        dis.readLong() // skip header and class version
        int cpcnt = (dis.readShort() & 0xffff) - 1i
        int[] classes = new int[cpcnt]
        String[] strings = new String[cpcnt]
        for (int i=0; i < cpcnt; i++)
        {
            int t = dis.read()
            if (t == 7)
            {
                classes[i] = dis.readShort() & 0xffff
            }
            else if (t == 1)
            {
                strings[i] = dis.readUTF()
            }
            else if (t == 5 || t == 6)
            {
                dis.readLong()
                i++
            }
            else if (t == 8)
            {
                dis.readShort()
            }
            else
            {
                dis.readInt()
            }
        }
        dis.readShort() // skip access flags
        return strings[classes[(dis.readShort() & 0xffff) - 1i] - 1i].replace('/', '.')
    }

    protected Map getClassLoaderAndSource(Map<String, Object> ctx)
    {
        NCube cube = getNCube(ctx)
        boolean isUrlUsed = StringUtilities.hasContent(url)
        Map output = [:]

        if (cube.name.toLowerCase().startsWith("sys."))
        {   // No URLs allowed, nor code from sys.classpath when executing these cubes
            output.loader = (GroovyClassLoader)NCubeManager.getLocalClassloader(cube.applicationID)
            output.source = cmd
        }
        else if (isUrlUsed)
        {
            if (url.endsWith('.groovy'))
            {
                // If a class exists already with the same name as the groovy file (substituting slashes for dots),
                // then attempt to find and return that class without going through the resource location and parsing
                // code. This can be useful, for example, if a build process pre-builds and load coverage enhanced
                // versions of the classes.
                try
                {
                    String className = url - '.groovy'
                    className = className.replace('/', '.')
                    output.gclass = Class.forName(className)
                }
                catch (Exception ignored)
                { }
            }

            URL groovySourceUrl = getActualUrl(ctx)
            output.loader = (GroovyClassLoader) NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            output.source = StringUtilities.createUtf8String(UrlUtilities.getContentFromUrl(groovySourceUrl, true))
        }
        else
        {   // inline code
            output.loader = (GroovyClassLoader)NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            output.source = cmd
        }
        output.source = expandNCubeShortCuts(buildGroovy(ctx, output.source as String))
        return output
    }

    /**
     * Compute SHA1 hash for this CommandCell.  The tricky bit here is that the command can be either
     * defined inline or via a URL.  If defined inline, then the command hash is SHA1(command text).  If
     * defined through a URL, then the command hash is SHA1(command URL + GroovyClassLoader URLs.toString).
     */
    private void computeCmdHash(Object data, Map<String, Object> ctx)
    {
        String content
        if (url == null)
        {
            content = data != null ? data.toString() : ""
        }
        else
        {   // specified via URL, add classLoader URL strings to URL for SHA-1 source.
            NCube cube = getNCube(ctx)
            GroovyClassLoader gcLoader = (GroovyClassLoader) NCubeManager.getUrlClassLoader(cube.applicationID, getInput(ctx))
            URL[] urls = gcLoader.URLs
            StringBuilder s = new StringBuilder()
            for (URL url : urls)
            {
                s.append(url.toString())
                s.append('.')
            }
            s.append(url)
            content = s.toString()
        }
        cmdHash = EncryptionUtilities.calculateSHA1Hash(StringUtilities.getUTF8Bytes(content))
    }

    protected static String expandNCubeShortCuts(String groovy)
    {
        Matcher m = Regexes.groovyAbsRefCubeCellPattern.matcher(groovy)
        String exp = m.replaceAll('$1go($3, \'$2\')')

        m = Regexes.groovyAbsRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1go($3, \'$2\')')

        m = Regexes.groovyAbsRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1go($2)')

        m = Regexes.groovyAbsRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1go($2)')

        m = Regexes.groovyRelRefCubeCellPattern.matcher(exp)
        exp = m.replaceAll('$1at($3,\'$2\')')

        m = Regexes.groovyRelRefCubeCellPatternA.matcher(exp)
        exp = m.replaceAll('$1at($3, \'$2\')')

        m = Regexes.groovyRelRefCellPattern.matcher(exp)
        exp = m.replaceAll('$1at($2)')

        m = Regexes.groovyRelRefCellPatternA.matcher(exp)
        exp = m.replaceAll('$1at($2)')
        return exp
    }

    void getCubeNamesFromCommandText(final Set<String> cubeNames)
    {
        getCubeNamesFromText(cubeNames, cmd)
    }

    protected static void getCubeNamesFromText(final Set<String> cubeNames, final String text)
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

        m = Regexes.groovyExplicitAtPattern.matcher(text)
        while (m.find())
        {
            cubeNames.add(m.group(2))  // based on Regex pattern - if pattern changes, this could change
        }

        m = Regexes.groovyExplicitGoPattern.matcher(text)
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
    void getScopeKeys(Set<String> scopeKeys)
    {
        Matcher m = Regexes.inputVar.matcher(cmd)
        while (m.find())
        {
            scopeKeys.add(m.group(2))
        }
    }

    static Set<String> extractImportsAndAnnotations(String text, StringBuilder newGroovy)
    {
        // imports
        Matcher m1 = Regexes.importPattern.matcher(text)
        Set<String> extractedLines = new LinkedHashSet<>()
        while (m1.find())
        {
            extractedLines.add(m1.group(0))  // based on Regex pattern - if pattern changes, this could change
        }
        m1.reset()
        String sourceWithoutImports = m1.replaceAll('')

        // @Grapes
        Matcher m2 = Regexes.grapePattern.matcher(sourceWithoutImports)
        while (m2.find())
        {
            extractedLines.add(m2.group(0))  // based on Regex pattern - if pattern changes, this could change
        }

        m2.reset()
        String sourceWithoutGrape = m2.replaceAll('')

        // @Grab, @GrabResolver, @GrabExclude, @GrabConfig
        Matcher m3 = Regexes.grabPattern.matcher(sourceWithoutGrape)
        while (m3.find())
        {
            extractedLines.add(m3.group(0))  // based on Regex pattern - if pattern changes, this could change
        }

        m3.reset()
        String sourceWithoutGrab = m3.replaceAll('')

        newGroovy.append(sourceWithoutGrab)
        return extractedLines
    }
}
