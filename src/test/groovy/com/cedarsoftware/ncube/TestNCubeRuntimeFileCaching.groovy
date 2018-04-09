package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.LocalFileCache
import com.cedarsoftware.util.CallableBean
import groovy.transform.CompileStatic
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.springframework.cache.Cache
import org.springframework.cache.concurrent.ConcurrentMapCache
import org.springframework.cache.support.SimpleCacheManager

import static SnapshotPolicy.*
import static com.cedarsoftware.ncube.ApplicationID.testAppId
import static com.cedarsoftware.ncube.NCubeAppContext.getNcubeRuntime
import static org.junit.Assert.*
import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_INCLUDE_CUBE_DATA
import static com.cedarsoftware.ncube.NCubeConstants.SEARCH_CHECK_SHA1

@CompileStatic
class TestNCubeRuntimeFileCaching extends NCubeBaseTest
{
    NCubeRuntime cacheRuntime
    NCubeRuntimeClient cacheClient
    CallableBean callableBean
    File cacheDir
    final Map cubeFileNameMap = [:]

    ApplicationID snapshotId = testAppId
    ApplicationID releaseId = testAppId.asRelease()

    private static final String MOCK_BEAN_NAME = 'mockCallable'
    private static final String METHOD_LOAD_CUBE_RECORD = 'loadCubeRecord'

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setup()
    {
        cubeFileNameMap.clear()
        cubeFileNameMap.putAll(['TestBranch':'test.branch.1','TestAge':'test.branch.age.1'])

        cacheDir = new File ('target/ncubeFileCacheTests')
        clearDirectory(cacheDir)

        callableBean = Mockito.mock CallableBean.class
        Mockito.when(callableBean.call(Mockito.eq(MOCK_BEAN_NAME),Mockito.anyString(),Mockito.anyListOf(String.class))).then(new Answer<Object>() {
            @Override
            Object answer(InvocationOnMock invocation) throws Throwable {
                String method = invocation.getArgument(1)
                List<Object> methodArgs = (List) invocation.arguments[2]
                if (METHOD_LOAD_CUBE_RECORD==method ) {
                    ApplicationID appId = (ApplicationID) methodArgs[0]
                    String cubeName = (String) methodArgs[1]
                    Map options = (Map) methodArgs[2] ?: [(SEARCH_INCLUDE_CUBE_DATA):true]

                    String fileName = cubeFileNameMap[cubeName]
                    if (!fileName) {
                        return null
                    }
                    NCube resultCube = createRuntimeCubeFromResource(appId,"${fileName}.json")

                    NCubeInfoDto dto = new NCubeInfoDto()
                    dto.tenant = appId.tenant
                    dto.app = appId.app
                    dto.version = appId.version
                    dto.status = appId.status
                    dto.branch = appId.branch
                    dto.name = (String) cubeName
                    dto.sha1 = resultCube.sha1()
                    if (options[SEARCH_INCLUDE_CUBE_DATA]) {
                        dto.bytes = options[SEARCH_CHECK_SHA1]==resultCube.sha1 ? null : resultCube.toFormattedJson().bytes
                    }
                    return dto
                }
                return null
            }
        })

        SimpleCacheManager cacheManager = new SimpleCacheManager()
        cacheManager.setCaches([new ConcurrentMapCache(snapshotId.cacheKey()),new ConcurrentMapCache(releaseId.cacheKey())])
        cacheManager.initializeCaches()

        cacheRuntime = new NCubeRuntime(callableBean, cacheManager, false, MOCK_BEAN_NAME)
        cacheRuntime.localFileCache = new LocalFileCache(cacheDir.path,RELEASE_ONLY)
        cacheClient = cacheRuntime
    }

    @After
    public void tearDown()
    {
    }

    @Test
    void testDefaultSettings()
    {
        assertEquals('',((NCubeRuntime)ncubeRuntime).localFileCache.cacheDir)
        assertEquals(RELEASE_ONLY,((NCubeRuntime)ncubeRuntime).localFileCache.snapshotPolicy)
    }

    @Test
    void testWriteValidSnapshotWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestBranch'

        verifyFileExistence(snapshotId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(snapshotId, cubeName)
        assertEquals(cubeName,cube.name)

        // no cache files should be written with RELEASE_ONLY
        verifySha1Existence(snapshotId, cubeName, null)
        verifyFileExistence(snapshotId, cubeName, '', false)
        verifyFileExistence(snapshotId, cubeName, cube.sha1(), false)
    }

    @Test
    void testWriteMissingSnapshotWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestMissing'

        verifyFileExistence(snapshotId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(snapshotId, cubeName)
        assertNull(cube)

        // no cache files should be written with RELEASE_ONLY
        verifySha1Existence(snapshotId, cubeName, null)
        verifyFileExistence(snapshotId, cubeName, '', false)
    }

    @Test
    void testReadValidSnapshotWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube desiredCube = createRuntimeCubeFromResource(appId,'test.branch.1.json')

        // write out cache files for cube to be ignored
        NCube ignoredCube = createRuntimeCubeFromResource(appId,'test.branch.2.json')
        writeSha1File(appId, cubeName, ignoredCube.sha1())
        writeFile(appId, cubeName, ignoredCube.sha1(), ignoredCube)

        verifySha1Existence(appId, cubeName, ignoredCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, ignoredCube.sha1(), true)
        verifyFileExistence(appId, cubeName, desiredCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(desiredCube.sha1(),cube.sha1())

        // cached cubes should still point to ignored cube
        verifySha1Existence(appId, cubeName, ignoredCube.sha1())
        verifyFileExistence(appId, cubeName, ignoredCube.sha1(), true)
        verifyFileExistence(appId, cubeName, desiredCube.sha1(), false)
    }

    @Test
    void testReadMissingSnapshotWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube desiredCube = createRuntimeCubeFromResource(appId,'test.branch.1.json')
        writeSha1File(appId, cubeName, '')

        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, desiredCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(desiredCube.sha1(),cube.sha1())

        // cached files should remain the same
        verifySha1Existence(appId, cubeName, '')
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, desiredCube.sha1(), false)
    }

    @Test
    void testWriteValidReleaseWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestBranch'

        verifyFileExistence(releaseId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(releaseId, cubeName)
        assertEquals(cubeName,cube.name)

        // release files should still be written even in RELEASE_ONLY mode
        verifyFileExistence(releaseId, cubeName, '', true)
        assertTrue(getFileForCachedCube(releaseId,cubeName,'').length()>0)
        verifyFileExistence(releaseId, cubeName, cube.sha1(), false)
    }

    @Test
    void testWriteMissingReleaseWithReleaseOnly()
    {
        setSnapshotMode(RELEASE_ONLY)
        String cubeName = 'TestMissing'

        verifyFileExistence(releaseId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(releaseId, cubeName)
        assertNull(cube)
        verifyFileExistence(releaseId, cubeName, '', true)
        assertEquals(0,getFileForCachedCube(releaseId,cubeName,'').length())
    }

    @Test
    void testOfflineReadMissingSnapshot()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestMissing'
        ApplicationID appId = snapshotId

        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)

        exception.expectMessage("Failed to find cube: TestMissing in offline cache")
        getCubeFromRuntime(appId, cubeName)
    }

    @Test
    void testOfflineReadEmptySnapshot()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestEmpty'
        ApplicationID appId = snapshotId

        writeSha1File(appId, cubeName, '')
        verifyFileExistence(snapshotId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(snapshotId, cubeName)
        assertNull(cube)
        verifySha1Existence(snapshotId, cubeName, '')
        verifyFileExistence(snapshotId, cubeName, '', false)
        verifyMockLoad(appId, cubeName, 0)
    }

    @Test
    void testOfflineReadValidSnapshot()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")
        writeSha1File(appId, cubeName, loadedCube.sha1())
        writeFile(appId, cubeName, loadedCube.sha1(), loadedCube)
        writeSha1File(appId, SYS_ADVICE, '')

        verifyFileExistence(appId, cubeName, '', false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, loadedCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 0)
    }

    @Test
    void testOfflineReadMissingRelease()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestMissing'
        ApplicationID appId = releaseId

        verifyFileExistence(appId, cubeName, '', false)

        exception.expectMessage("Failed to find cube: TestMissing in offline cache")
        getCubeFromRuntime(appId, cubeName)
    }

    @Test
    void testOfflineReadEmptyRelease()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestEmpty'
        ApplicationID appId = releaseId

        writeFile(appId,cubeName,'',null)
        verifyFileExistence(appId, cubeName, '', true)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertNull(cube)
        verifyFileExistence(appId, cubeName, '', true)
        verifyMockLoad(appId, cubeName, 0)
    }

    @Test
    void testOfflineReadValidRelease()
    {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestBranch'
        ApplicationID appId = releaseId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")
        writeFile(appId, cubeName, '', loadedCube)
        writeFile(appId, SYS_ADVICE, '', null)

        verifyFileExistence(appId, cubeName, '', true)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyFileExistence(appId, cubeName, '', true)
        verifyMockLoad(appId, cubeName, 0)
    }

    @Test
    void testReadWriteNonChangingReleaseWithUpdate()
    {
        setSnapshotMode(UPDATE)
        String cubeName = 'TestBranch'
        ApplicationID appId = releaseId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyFileExistence(appId, cubeName, '', true)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        verifyMockLoad(appId, cubeName, 1)

        // clear cache and ensure cached file is used
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 1)
    }

    @Test
    void testReadWriteNonChangingReleaseWithForce()
    {
        setSnapshotMode(FORCE)
        String cubeName = 'TestBranch'
        ApplicationID appId = releaseId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyFileExistence(appId, cubeName, '', true)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        verifyMockLoad(appId, cubeName, 1)

        // clear cache and ensure cached file is used (sha1 never changed)
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 1)
    }

    @Test
    void testReadWriteNonChangingSnapshotWithUpdate()
    {
        setSnapshotMode(UPDATE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, loadedCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)

        // clear cache and ensure cached file is used (sha1 never changed)
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, loadedCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)
    }

    @Test
    void testReadWriteNonChangingSnapshotWithForce()
    {
        setSnapshotMode(FORCE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube loadedCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, cube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, loadedCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)

        // clear cache and ensure cached file is used (sha1 never changed)
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(loadedCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 1)
        verifyMockLoadWithSha1(appId, cubeName, cube.sha1(), 1)
    }

    @Test
    void testReadWriteChangingSnapshotWithUpdate()
    {
        setSnapshotMode(UPDATE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube origCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(origCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, cube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)

        // simulate cube update
        cubeFileNameMap[cubeName] = 'test.branch.2'
        NCube updatedCube = createRuntimeCubeFromResource(appId,"test.branch.2.json")
        assertTrue(origCube.sha1() != updatedCube.sha1())

        // clear cache and ensure cached original file is returned
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(origCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 1)
        verifySha1Existence(appId, cubeName, origCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), true)
        verifyFileExistence(appId, cubeName, updatedCube.sha1(), false)
    }

    @Test
    void testReadWriteChangingSnapshotWithForce()
    {
        setSnapshotMode(FORCE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube origCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(origCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, origCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)
        verifyMockLoadWithSha1(appId, cubeName, origCube.sha1(), 0)

        cubeFileNameMap[cubeName] = 'test.branch.2'
        NCube updatedCube = createRuntimeCubeFromResource(appId,"test.branch.2.json")
        assertTrue(origCube.sha1() != updatedCube.sha1())

        // clear cache and ensure new file is cached
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(updatedCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, updatedCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, updatedCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)  // hasn't changed
        verifyMockLoadWithSha1(appId, cubeName, origCube.sha1(), 1)
    }

    @Test
    void testReadWriteChangingToMissingSnapshotWithForce()
    {
        setSnapshotMode(FORCE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube origCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // ensure initial load writes the latest file
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(origCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, cube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)

        cubeFileNameMap.remove(cubeName)

        // clear cache and ensure cube no longer returned
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertNull(cube)
        verifySha1Existence(appId, cubeName, '')
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, origCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)
        verifyMockLoadWithSha1(appId, cubeName, origCube.sha1(), 1)
    }

    @Test
    void testReadWriteMissingToValidSnapshotWithForce()
    {
        setSnapshotMode(FORCE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube newCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        // don't allow cube to be found
        cubeFileNameMap.remove(cubeName)

        // ensure initial load returns the missing cube
        verifySha1Existence(appId, cubeName, null)
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, newCube.sha1(), false)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertNull(cube)
        verifySha1Existence(appId,cubeName,'')
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, newCube.sha1(), false)
        verifyMockLoad(appId, cubeName, 1)

        // allow cube to be loaded now
        cubeFileNameMap[cubeName] = 'test.branch.1'

        // clear cache and ensure cube is now found
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(newCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 2)
        verifySha1Existence(appId, cubeName, newCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, newCube.sha1(), true)
    }

    @Test
    void testReadOfInvalidSnapshotWithUpdate() {
        setSnapshotMode(UPDATE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube realCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        writeSha1File(appId,cubeName,realCube.sha1())
        writeFile(appId,cubeName,realCube.sha1(),'bogus'.bytes)

        // the initial load should use a invalid file, but return the good cube
        verifySha1Existence(appId, cubeName, realCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, realCube.sha1(), true)
        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(realCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, cube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, realCube.sha1(), true)
        assertTrue(getFileForCachedCube(appId,cubeName,realCube.sha1()).length()>'bogus'.bytes.length)
        verifyMockLoad(appId, cubeName, 1)

        // clear cache and ensure cube real cube is loaded now from cache
        cacheClient.clearCache(appId,[cubeName])
        cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(realCube.sha1(),cube.sha1())
        verifyMockLoad(appId, cubeName, 1)
        verifySha1Existence(appId, cubeName, realCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, realCube.sha1(), true)
    }

    @Test
    void testOfflineReadOfInvalidSha1() {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube realCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        writeSha1File(appId, cubeName, realCube.sha1())

        // mark sha1 as non-readable to trigger read exception
        getFileForCachedSha1(appId, cubeName).setReadable(false)

        exception.expectMessage("Failed to find cube: TestBranch in offline cache")
        getCubeFromRuntime(appId, cubeName)
    }

    @Test
    void testOfflineReadOfInvalidSnapshot() {
        setSnapshotMode(OFFLINE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube realCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        writeSha1File(appId, cubeName, realCube.sha1())
        writeFile(appId,cubeName,realCube.sha1(),'bogus'.bytes)

        // the initial load should use a invalid file which results in missing file (offline only uses cached files)
        verifySha1Existence(appId, cubeName, realCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, realCube.sha1(), true)

        exception.expectMessage("Failed to load cube: TestBranch from offline cache")
        getCubeFromRuntime(appId, cubeName)
    }

    @Test
    void testWriteWithException()
    {
        setSnapshotMode(UPDATE)
        String cubeName = 'TestBranch'
        ApplicationID appId = snapshotId

        NCube realCube = createRuntimeCubeFromResource(appId,"test.branch.1.json")

        File cachedFile = getFileForCachedCube(appId,cubeName,realCube.sha1())
        File dir = new File(cachedFile.parent)
        if (!dir.isDirectory()) {
            dir.mkdirs()
        }

        File latest = writeFile(appId,cubeName,realCube.sha1(),'bogus'.bytes)
        File versioned = writeFile(appId,cubeName,realCube.sha1(),'bogus'.bytes)
        latest.setReadOnly()
        versioned.setReadOnly()

        NCube cube = getCubeFromRuntime(appId, cubeName)
        assertEquals(cubeName,cube.name)
        assertEquals(realCube.sha1(),cube.sha1())
        verifySha1Existence(appId, cubeName, realCube.sha1())
        verifyFileExistence(appId, cubeName, '', false)
        verifyFileExistence(appId, cubeName, realCube.sha1(), true)
        verifyMockLoad(appId, cubeName, 1)
    }

    private NCube getCubeFromRuntime(ApplicationID appId, String cubeName)
    {
        NCube cube = cacheClient.getCube(appId,cubeName)

        // ensure cube is in the runtime cache
        Cache cache = ((NCubeTestClient)cacheRuntime).getCacheForApp(appId)
        Cache.ValueWrapper wrapper = cache.get(cubeName.toLowerCase())
        if (cube) {
            assertEquals((NCube) wrapper.get(),cube)
        }
        else {
            assertEquals(Boolean.FALSE, wrapper.get())
        }

        return cube
    }

    private void setSnapshotMode(SnapshotPolicy mode)
    {
        cacheRuntime.localFileCache.snapshotPolicy = mode
    }

    private Object verifyMockLoad(ApplicationID appId, String cubeName, int times) {
        return Mockito.verify(callableBean, Mockito.times(times)).call(Mockito.eq(MOCK_BEAN_NAME), Mockito.eq(METHOD_LOAD_CUBE_RECORD), Mockito.eq([appId, cubeName, null]))
    }

    private Object verifyMockLoadWithSha1(ApplicationID appId, String cubeName, String sha1, int times) {
        return Mockito.verify(callableBean, Mockito.times(times)).call(Mockito.eq(MOCK_BEAN_NAME), Mockito.eq(METHOD_LOAD_CUBE_RECORD), Mockito.eq([appId, cubeName, [includeCubeData:true, checkSha1:'D71891F6BD1CE8644F6BF5E2E553E2ECA652E785']]))
    }

    private static void clearDirectory(File dir) {
        if (dir.exists()) {
            assertTrue('directory should be purged', dir.deleteDir())
        }
        assertFalse('directory should not already exist', dir.exists())
    }

    private void writeFile(ApplicationID appId, String cubeName, String sha1, NCube cube) {
        writeFile(appId,cubeName,sha1,cube ? cube.toFormattedJson().bytes : null)
    }

    private File writeSha1File(ApplicationID appId, String cubeName, String sha1) {
        File sha1File = getFileForCachedSha1(appId,cubeName)
        writeBytes(sha1File, sha1 ? sha1.bytes : null)
        return sha1File
    }

    private File writeFile(ApplicationID appId, String cubeName, String sha1, byte [] bytes) {
        File jsonFile = getFileForCachedCube(appId, cubeName, sha1)//new File ("${cacheDir.path}/${appId.cacheKey()}${cubeName}${sha1}.json")
        writeBytes(jsonFile,bytes)
        return jsonFile
    }

    private static void writeBytes(File file, byte [] bytes) {
        File dir = new File(file.parent)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        if (bytes) {
            file.bytes = bytes
        }
        else {
            file.createNewFile()
        }

        assertTrue(file.path, file.isFile())
    }

    private void verifySha1Existence(ApplicationID appId, String cubeName, String contents) {
        boolean exists = contents!=null
        File jsonFile = new File("${cacheDir.path}/${appId.cacheKey()}${cubeName}.sha1")
        assertEquals("file=${jsonFile.path} should ${exists?'':'not '}exist",exists,jsonFile.exists())
        if (exists && contents!=null) {
            assertEquals(contents,jsonFile.text)
        }
    }

    private void verifyFileExistence(ApplicationID appId, String cubeName, String sha1, boolean exists=true) {
        File jsonFile = getFileForCachedCube(appId, cubeName, sha1)
        assertEquals("file=${jsonFile.path} should ${exists?'':'not '}exist",exists,jsonFile.exists())
    }

    private File getFileForCachedCube(ApplicationID appId, String cubeName, String sha1) {
        String suffix = sha1 ? ".${sha1}" : ''
        File jsonFile = new File("${cacheDir.path}/${appId.cacheKey()}${cubeName}${suffix}.json")
        return jsonFile
    }

    private File getFileForCachedSha1(ApplicationID appId, String cubeName) {
        File jsonFile = new File("${cacheDir.path}/${appId.cacheKey()}${cubeName}.sha1")
        return jsonFile
    }
}
