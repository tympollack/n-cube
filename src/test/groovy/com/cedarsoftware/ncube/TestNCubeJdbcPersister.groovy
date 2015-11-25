package com.cedarsoftware.ncube
import com.cedarsoftware.util.IOUtilities
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.sql.Blob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.sql.Timestamp

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNull
import static org.junit.Assert.assertTrue
import static org.junit.Assert.fail
import static org.mockito.Matchers.anyInt
import static org.mockito.Matchers.anyLong
import static org.mockito.Matchers.anyString
import static org.mockito.Mockito.mock
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
class TestNCubeJdbcPersister
{
    static final String APP_ID = "ncube.test";
    static final String USER_ID = "jdirt";

    private ApplicationID defaultSnapshotApp = new ApplicationID(ApplicationID.DEFAULT_TENANT, APP_ID, "1.0.0", ApplicationID.DEFAULT_STATUS, ApplicationID.TEST_BRANCH)

    @Before
    void setUp()
    {
        TestingDatabaseHelper.setupDatabase()
    }

    @After
    void tearDown()
    {
        TestingDatabaseHelper.tearDownDatabase()
    }

    @Test
    void testSelectCubesStatementWithActiveOnlyAndDeletedOnlyBothSetToTrue()
    {
        try
        {
            HashMap options = new HashMap();
            options.put(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY, true);
            options.put(NCubeManager.SEARCH_DELETED_RECORDS_ONLY, true);

            new NCubeJdbcPersister().createSelectCubesStatement(null, null, null, options);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("cannot both be 'true'"));
        }
    }

    @Test
    void testDbApis()
    {
        NCubePersister persister = TestingDatabaseHelper.persister

        NCube ncube1 = NCubeBuilder.testNCube3D_Boolean
        NCube ncube2 = NCubeBuilder.getTestNCube2D(true)

        persister.updateCube(defaultSnapshotApp, ncube1, USER_ID)
        persister.updateCube(defaultSnapshotApp, ncube2, USER_ID)

        Object[] cubeList = persister.search(defaultSnapshotApp, "test.%", null, [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY) : true])

        assertTrue(cubeList != null)
        assertTrue(cubeList.length == 2)

        assertTrue(ncube1.numDimensions == 3)
        assertTrue(ncube2.numDimensions == 2)

        ncube1.deleteAxis("bu")
        ApplicationID next = defaultSnapshotApp.createNewSnapshotId("0.2.0")
        persister.updateCube(defaultSnapshotApp, ncube1, USER_ID)
        int numRelease = persister.releaseCubes(defaultSnapshotApp, "0.2.0")
        assertEquals(0, numRelease)

        cubeList = NCubeManager.search(next, 'test.*', null, [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY):true])
        // Two cubes at the new 1.2.3 SNAPSHOT version.
        assert cubeList.length == 2

        // Verify that you cannot delete a RELEASE ncube
        try
        {
            assertFalse(persister.deleteCube(defaultSnapshotApp, ncube1.name, false, USER_ID))
        }
        catch (Exception e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("delete"))
            assertTrue(e.message.contains("nable"))
            assertTrue(e.message.contains("find"))
        }
        try
        {
            assertFalse(persister.deleteCube(defaultSnapshotApp, ncube2.name, false, USER_ID))
        }
        catch (Exception e)
        {
            assertTrue(e.message.contains("not"))
            assertTrue(e.message.contains("delete"))
            assertTrue(e.message.contains("nable"))
            assertTrue(e.message.contains("find"))
        }

        // Delete new SNAPSHOT cubes
        assertTrue(persister.deleteCube(next, ncube1.name, false, USER_ID))
        assertTrue(persister.deleteCube(next, ncube2.name, false, USER_ID))

        // Ensure that all test ncubes are deleted
        cubeList = persister.search(defaultSnapshotApp, "test.%", null, ['activeRecordsOnly' : true])
        assertTrue(cubeList.length == 0)
    }

    @Test
    void testGetAppNamesWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getAppNames(c, defaultSnapshotApp.DEFAULT_TENANT, null, ApplicationID.TEST_BRANCH)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('cannot be null or empty')
        }
    }

    @Test
    void testGetMinRevisionWithSqlException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getMinRevision(c, defaultSnapshotApp, "foo");
            fail()
        }
        catch (SQLException e)
        {
        }
    }

    @Test
    void testUpdateCubeWithSqlException()
    {
        NCube<Double> ncube = NCubeBuilder.getTestNCube2D(true)
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, ncube, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.contains("nable"))
            assertTrue(e.message.contains("ube"))
        }
    }

    @Test
    void testGetAppVersionsWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getAppVersions(c, "DEFAULT", "FOO", "SNAPSHOT", null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('cannot be null or empty')
        }
    }

    @Test
    void testCommitMergedCubeToBranchThatReturnsEmpty()
    {
        Connection c = mock(Connection.class)
        Statement stmt = mock(Statement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.createStatement(anyInt(), anyInt())).thenReturn(stmt)
        when(stmt.executeQuery(anyString())).thenReturn(rs)
        when(rs.next()).thenReturn(false);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");

        NCube<Double> cube = NCubeBuilder.getTestNCube2D(true)
        assertNull(new NCubeJdbcPersister().commitMergedCubeToBranch(c, defaultSnapshotApp, cube, cube.sha1(), "name"));
    }

    @Test
    void testCommitMergedCubeToBranchThatThrowsRuntimeException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            NCube<Double> cube = NCubeBuilder.getTestNCube2D(true)
            new NCubeJdbcPersister().commitMergedCubeToBranch(c, defaultSnapshotApp, null, null, "name")
            fail()
        }
        catch (NullPointerException e)
        {
            assertNull(e.message)
        }
    }

    @Test
    void testCommitMergedCubeToBranchThatThrowsSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            NCube<Double> cube = NCubeBuilder.getTestNCube2D(true)
            new NCubeJdbcPersister().commitMergedCubeToBranch(c, defaultSnapshotApp, cube, cube.sha1(), "name")
            fail()
        }
        catch (SQLException e)
        {
        }
    }

    @Test
    void testCommitMergedCubeHeadThatThrowsRuntimeException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            NCube<Double> cube = NCubeBuilder.getTestNCube2D(true)
            new NCubeJdbcPersister().commitMergedCubeToHead(c, defaultSnapshotApp, null, "name")
            fail()
        }
        catch (NullPointerException e)
        {
            assertNull(e.message)
        }
    }

    @Test
    void testCommitMergedCubeToHeadThatThrowsSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            NCube<Double> cube = NCubeBuilder.getTestNCube2D(true)
            new NCubeJdbcPersister().commitMergedCubeToHead(c, defaultSnapshotApp, cube, "name")
            fail()
        }
        catch (SQLException e)
        {
        }
    }

    @Test
    void testUpdateBranchCube()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, 0, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.toLowerCase().contains("unable"))
            assertTrue(e.message.contains("update cube"))
        }
    }

    @Test
    void testLoadCubeBySha1ThatReturnsNoCube()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(false);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");

        assertNull(new NCubeJdbcPersister().loadCubeBySha1(c, defaultSnapshotApp, "foo", "sha"));
    }

    @Test
    void testLoadCubeBySha1ThatThrowsException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().loadCubeBySha1(c, defaultSnapshotApp, "foo", "sha")
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.toLowerCase().contains("unable"))
            assertTrue(e.message.contains("load cube"))
        }
    }

    @Test
    void testLoadCubeByRevisionThatThrowsException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().loadCubeById(c, 0L)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.toLowerCase().contains("unable"))
            assertTrue(e.message.contains("load cube"))
        }
    }

    @Test
    void testLoadCubeByRevisionThatReturnsNoCube()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(false);

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");

        try
        {
            new NCubeJdbcPersister().loadCubeById(c, 0L)
        }
        catch (IllegalArgumentException e)
        {
            assert e.message.toLowerCase().contains('unable to find cube')
        }
    }

    @Test
    void testUpdateBranchThatIsNotFound()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps).thenReturn(ps).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs).thenReturn(rs)
        when(rs.next()).thenReturn(false)
        when(rs.getLong(1)).thenReturn(5L)
        when(rs.getDate(anyString())).thenReturn(new java.sql.Date(System.currentTimeMillis()))

        assertNull(new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, 0, USER_ID))
    }

    @Test
    void testReleaseCubesWithRuntimeExceptionWhileCreatingNewSnapshot()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps).thenReturn(ps).thenReturn(ps).thenThrow(NullPointerException.class)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(false)

        try
        {
            new NCubeJdbcPersister().releaseCubes(c, defaultSnapshotApp, "1.2.3")
            fail()
        }
        catch (NullPointerException e)
        {
            assertEquals(e.message, null)
        }
    }

    @Test
    void testLoadCubesWithInvalidCube()
    {
        Connection c = mock(Connection.class)
        ResultSet rs = mock(ResultSet.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true).thenReturn(false)
        when(rs.getString("n_cube_nm")).thenReturn("foo")
        when(rs.getString("branch_id")).thenReturn("HEAD")
        when(rs.getTimestamp("create_dt")).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(rs.getBytes("cube_value_bin")).thenReturn("[                                                     ".getBytes("UTF-8"))
        mockDatabaseMetaData(c);

        Map options = [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY): true]
        Object[] nCubes = new NCubeJdbcPersister().search(c, defaultSnapshotApp, "", null, options)
        assertNotNull(nCubes)
        assertEquals(1, nCubes.length)     // Won't know it fails until getCube() is called.
    }

    @Test
    void testMergeOverwriteBranchCubeWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException();
        try
        {
            new NCubeJdbcPersister().mergeAcceptTheirs(c, defaultSnapshotApp, "TestName", "Foo", USER_ID);
        } catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.getClass());
            assertTrue(e.message.toLowerCase().contains("unable"));
            assertTrue(e.message.toLowerCase().contains("overwrite cube"));
        }
    }

    @Test
    void testMergeAcceptMineWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException();
        try
        {
            new NCubeJdbcPersister().mergeAcceptMine(c, defaultSnapshotApp, "TestName", USER_ID);
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.getClass());
            assertTrue(e.message.toLowerCase().contains("unable"));
            assertTrue(e.message.toLowerCase().contains("overwrite cube"));
        }
    }

    @Test
    void testLoadCubesWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        NCubeInfoDto dto = new NCubeInfoDto()
        dto.id = "0";
        dto.tenant = ApplicationID.DEFAULT_TENANT
        dto.app = ApplicationID.DEFAULT_APP
        dto.version = ApplicationID.DEFAULT_VERSION
        dto.status = 'SNAPSHOT'
        dto.name = 'foo'
        dto.branch = ApplicationID.HEAD

        try
        {
            new NCubeJdbcPersister().loadCubeById(c, 0L)
            fail()
        }
        catch (RuntimeException e)
        {
            assert e.message.toLowerCase().contains("unable to load")
        }
    }

    @Test
    void testGetTestDataWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getTestData(c, defaultSnapshotApp, "foo")
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to fetch"))
            assertTrue(e.message.contains("test data"))
        }
    }

    @Test
    void testGetNotesWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getNotes(c, defaultSnapshotApp, "foo")
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to fetch"))
            assertTrue(e.message.contains("notes"))
        }
    }

    @Test
    void testGetMaxRevisionWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getMaxRevision(c, defaultSnapshotApp, "foo")
            fail()
        }
        catch (SQLException e)
        {
        }
    }

    @Test
    void testDeleteBranchWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().deleteBranch(c, defaultSnapshotApp)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.toLowerCase().startsWith("unable to delete branch"))
        }
    }

    @Test
    void testCommitCubeWithInvalidRevision()
    {
        try
        {
            new NCubeJdbcPersister().commitCube(null, defaultSnapshotApp, null, USER_ID)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains("cannot be empty"))
        }
    }

    @Test
    void testCommitCubeThatDoesntExist()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(c.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(ps.executeUpdate()).thenReturn(1).thenReturn(0);
        when(rs.next()).thenReturn(false)
        when(rs.getBytes("cube_value_bin")).thenReturn("".getBytes("UTF-8"))

        assertNull(new NCubeJdbcPersister().commitCube(c, defaultSnapshotApp, 1L, USER_ID))
    }

    @Test
    void testGetNCubesWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            Map options = [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY) : true]
            new NCubeJdbcPersister().search(c, defaultSnapshotApp, null, null, options)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to fetch"))
        }
    }

    @Test
    void testSearchThatThrowsSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            Map options = [:];
            options[NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY] = true;
            options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA] = true;
            new NCubeJdbcPersister().search(c, defaultSnapshotApp, null, "test", options)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to fetch"))
        }
    }

    @Test
    void testCreateBranchWithNullPointerException()
    {
        Connection c = getConnectionThatThrowsExceptionAfterExistenceCheck(false, NullPointerException.class)

        try
        {
            new NCubeJdbcPersister().createBranch(c, defaultSnapshotApp)
            fail()
        }
        catch (NullPointerException e)
        {
        }
    }

    @Test
    void testGetBranchesWithNullTenant()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().getBranches(c, null)
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('tenant must not be null or empty'))
        }
    }

    @Test
    void testDeleteCubesWithSQLException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().deleteBranch(c, defaultSnapshotApp)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to delete branch"))
        }
    }

    private static getConnectionThatThrowsSQLException = { ->
        Connection c = mock(Connection.class)
        when(c.prepareStatement(anyString())).thenThrow(SQLException.class)
        when(c.createStatement()).thenThrow(SQLException.class)
        when(c.createStatement(anyInt(), anyInt())).thenThrow(SQLException.class)
        when(c.createStatement(anyInt(), anyInt(), anyInt())).thenThrow(SQLException.class)
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");
        return c;
    }

    private static Connection getConnectionThatThrowsExceptionAfterExistenceCheck(boolean exists, Class exceptionClass = SQLException.class) throws SQLException
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps).thenThrow(exceptionClass)
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("HSQL");
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(exists)
        return c;
    }

    @Test
    void testCreateCubeWithSqlException()
    {
        NCube<Double> ncube = NCubeBuilder.getTestNCube2D(true)
        Connection c = getConnectionThatThrowsExceptionAfterExistenceCheck(false)
        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, ncube, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.startsWith("Unable to insert"))
        }
    }

    @Test
    void testCreateCubeThatDoesntCreateCube()
    {
        NCube<Double> ncube = NCubeBuilder.getTestNCube2D(true)

        OutputStream stream = mock(OutputStream.class);
        Blob b = mock(Blob.class);
        when(b.setBinaryStream(anyLong())).thenReturn(stream);
        Connection c = mock(Connection.class)
        when(c.createBlob()).thenReturn(b);
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(false)
        when(ps.executeUpdate()).thenReturn(0)

        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");

        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, ncube, USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("error inserting"))
        }
    }

    @Test
    void testDeleteCubeWithSQLException()
    {

        OutputStream stream = mock(OutputStream.class);
        Blob b = mock(Blob.class);
        when(b.setBinaryStream(anyLong())).thenReturn(stream);
        Connection c = mock(Connection.class)
        when(c.createBlob()).thenReturn(b);

        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenThrow(SQLException.class)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true)
        when(rs.getLong(1)).thenReturn(5L)

        mockDatabaseMetaData(c)

        try
        {
            new NCubeJdbcPersister().deleteCube(c, defaultSnapshotApp, "foo", true, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
        }
    }

    private void mockDatabaseMetaData(Connection c) {
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");
    }


    @Test
    void testRenameCubeThatThrowsSQLEXception()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().renameCube(c, defaultSnapshotApp, "foo", "bar", USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
        }
    }

    @Test
    void testGetRevisionsThatThrowsSQLException()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)

        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenThrow(SQLException.class)

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().getRevisions(c, defaultSnapshotApp, "foo")
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
        }
    }

    @Test
    void testRestoreCubeThatThrowsSQLException()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);

        when(c.prepareStatement(anyString())).thenThrow(SQLException.class)
        when(c.metaData).thenReturn(metaData);
        when(metaData.getDriverName()).thenReturn("Oracle");
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true)
        when(rs.getLong(anyString())).thenReturn(new Long(-9))

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(rs.getBytes(anyString())).thenReturn(out.toByteArray())
        when(rs.getBytes(anyInt())).thenReturn(null)

        try
        {
            new NCubeJdbcPersister().restoreCube(c, defaultSnapshotApp, "foo", USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.toLowerCase().contains("unable to restore cube"))
        }
    }

    @Test
    void testRestoreCubeThatFailsTheUpdate()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)

        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true)
        when(rs.getLong(anyString())).thenReturn(new Long(-9))

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(rs.getBytes(anyString())).thenReturn(out.toByteArray())
        when(rs.getBytes(anyInt())).thenReturn(null)
        when(ps.executeUpdate()).thenReturn(0)

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().restoreCube(c, defaultSnapshotApp, "foo", USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("Could not restore"))
        }
    }

    @Test
    void testDeleteCubeThatThrowsSQLException()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)

        when(c.prepareStatement(anyString())).thenReturn(ps).thenThrow(SQLException.class)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(true)
        when(rs.getLong(anyInt())).thenReturn(new Long(9))

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(rs.getBytes(anyString())).thenReturn(out.toByteArray())
        when(rs.getBytes(anyInt())).thenReturn(null)
        when(rs.getString("status_cd")).thenReturn(ReleaseStatus.SNAPSHOT.name())
        when(rs.getString("n_cube_nm")).thenReturn("foo")
        when(rs.getString("branch_id")).thenReturn(ApplicationID.HEAD)

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().deleteCube(c, defaultSnapshotApp, "foo", false, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
            assertTrue(e.message.contains("Unable to delete cube"))
        }
    }

    @Test
    void testDeleteCubeThatFailsTheUpdate()
    {
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)

        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(true)
        when(rs.getLong(anyInt())).thenReturn(new Long(9))
        when(rs.getBytes(anyString())).thenReturn("foo".getBytes("UTF-8"))

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(ps.executeUpdate()).thenReturn(0)

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().deleteCube(c, defaultSnapshotApp, "foo", false, USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("Cannot delete"))
            assertTrue(e.message.contains("not deleted"))
        }
    }

    @Test
    void testCreateCubeWithWrongUpdateCount()
    {
        NCube<Double> ncube = NCubeBuilder.getTestNCube2D(true)
        Connection c = mock(Connection.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        ResultSet rs = mock(ResultSet.class)
        when(c.prepareStatement(anyString())).thenReturn(ps).thenThrow(SQLException.class)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.next()).thenReturn(false)
        when(rs.getLong(anyInt())).thenReturn(new Long(-9))
        when(ps.executeUpdate()).thenReturn(0)

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, ncube, USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertTrue(e.message.contains("ube"))
            assertTrue(e.message.contains("Unable to insert"))
        }
    }

    @Test
    void testUpdateBranchCubeWithNull()
    {
        try
        {
            new NCubeJdbcPersister().updateCube((Connection)null, (ApplicationID) null,(Long) null, null);
            fail()
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.message.contains('cannot be empty'));
        }
    }

    @Test
    void testUpdateCubeWithWrongUpdateCount()
    {
        NCube<Double> ncube = NCubeBuilder.getTestNCube2D(true)
        OutputStream stream = mock(OutputStream.class);
        Blob b = mock(Blob.class);
        when(b.setBinaryStream(anyLong())).thenReturn(stream);
        Connection c = mock(Connection.class)
        when(c.createBlob()).thenReturn(b);
        mockDatabaseMetaData(c);
        ResultSet rs = mock(ResultSet.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        when(rs.next()).thenReturn(true)
        when(rs.getTimestamp(anyString())).thenReturn(new Timestamp(System.currentTimeMillis()));
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeUpdate()).thenReturn(0)
        when(ps.executeQuery()).thenReturn(rs)

        try
        {
            new NCubeJdbcPersister().updateCube(c, defaultSnapshotApp, ncube, USER_ID)
            fail()
        }
        catch (Exception e)
        {
            assertTrue(e.message.contains("error updating"))
            assertTrue(e.message.contains("not updated"))
        }
    }


    @Test
    void testRenameCubeThatDidntUpdateBecauseOfDatabaseError()
    {
        OutputStream stream = mock(OutputStream.class);
        Blob b = mock(Blob.class);
        when(b.setBinaryStream(anyLong())).thenReturn(stream);
        Connection c = mock(Connection.class)
        when(c.createBlob()).thenReturn(b);
        ResultSet rs = mock(ResultSet.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeUpdate()).thenReturn(0)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.getLong("revision_number")).thenReturn(0L);
        when(rs.next()).thenReturn(true).thenReturn(false);

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(rs.getBytes("cube_value_bin")).thenReturn(out.toByteArray())

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().renameCube(c, defaultSnapshotApp, "foo", "bar", USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("Unable to rename cube"))
        }

    }

    @Test
    void testRenameCubeThatDidntUpdateBecauseOfDatabaseError2()
    {
        OutputStream stream = mock(OutputStream.class);
        Blob b = mock(Blob.class);
        when(b.setBinaryStream(anyLong())).thenReturn(stream);
        Connection c = mock(Connection.class)
        when(c.createBlob()).thenReturn(b);
        ResultSet rs = mock(ResultSet.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeUpdate()).thenReturn(1).thenReturn(0)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.getLong("revision_number")).thenReturn(0L);
        when(rs.next()).thenReturn(true).thenReturn(false);

        ByteArrayOutputStream out = new ByteArrayOutputStream(8192)
        URL url = TestNCubeJdbcPersister.class.getResource("/2DSimpleJson.json")
        IOUtilities.transfer(new File(url.file), out)
        when(rs.getBytes("cube_value_bin")).thenReturn(out.toByteArray())

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().renameCube(c, defaultSnapshotApp, "foo", "bar", USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("Unable to rename cube"))
        }

    }

    @Test
    void testDuplicateCubeThatDidntUpdate()
    {
        ApplicationID head = defaultSnapshotApp.asHead();

        Connection c = mock(Connection.class)
        ResultSet rs = mock(ResultSet.class)
        PreparedStatement ps = mock(PreparedStatement.class)
        when(c.prepareStatement(anyString())).thenReturn(ps)
        when(ps.executeUpdate()).thenReturn(0)
        when(ps.executeQuery()).thenReturn(rs)
        when(rs.getLong("revision_number")).thenReturn(0L);
        when(rs.next()).thenReturn(true).thenReturn(false);

        mockDatabaseMetaData(c);
        try
        {
            new NCubeJdbcPersister().duplicateCube(c, head, defaultSnapshotApp, "name", "name", USER_ID)
            fail()
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.message.contains("Unable to duplicate cube"))
        }

    }

    @Test
    void testDuplicateCubeWithSqlException()
    {
        Connection c = getConnectionThatThrowsSQLException()
        try
        {
            new NCubeJdbcPersister().duplicateCube(c, defaultSnapshotApp, defaultSnapshotApp, "name", "foo", USER_ID)
            fail()
        }
        catch (RuntimeException e)
        {
            assertEquals(SQLException.class, e.cause.class)
        }

    }

//    @Test
//    void testReleaseCubesWithCubeThatExistsAlready()
//    {
//        NCubeBuilder.getTestNCube2D(true)
//        Connection c = getConnectionThatThrowsExceptionAfterExistenceCheck(true)
//
//        try
//        {
//            new NCubeJdbcPersister().releaseCubes(c, defaultSnapshotApp, "1.2.3")
//            fail()
//        }
//        catch (IllegalStateException e)
//        {
//            assertTrue(e.message.contains("already exists"))
//        }
//    }

//    @Test
//    void testChangeVersionWhenCubeAlreadyExists()
//    {
//        NCubeBuilder.getTestNCube2D(true)
//        Connection c = getConnectionThatThrowsExceptionAfterExistenceCheck(true)
//        try
//        {
//            new NCubeJdbcPersister().changeVersionValue(c, defaultSnapshotApp, "1.1.1")
//            fail()
//        }
//        catch (IllegalStateException e)
//        {
//            assertTrue(e.message.contains("already"))
//            assertTrue(e.message.contains("exist"))
//        }
//    }
}
