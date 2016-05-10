package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.formatters.JsonFormatter
import com.cedarsoftware.util.ArrayUtilities
import com.cedarsoftware.util.Converter
import com.cedarsoftware.util.IOUtilities
import com.cedarsoftware.util.SafeSimpleDateFormat
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.UniqueIdGenerator
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.sql.Blob
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.GZIPOutputStream
/**
 * SQL Persister for n-cubes.  Manages all reads and writes of n-cubes to an SQL database.
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
class NCubeJdbcPersister
{
    private static final Logger LOG = LogManager.getLogger(NCubeJdbcPersister.class);
    static final SafeSimpleDateFormat dateTimeFormat = new SafeSimpleDateFormat('yyyy-MM-dd HH:mm:ss')
    static final String CUBE_VALUE_BIN = 'cube_value_bin'
    static final String TEST_DATA_BIN = 'test_data_bin'
    static final String NOTES_BIN = 'notes_bin'
    static final String HEAD_SHA_1 = 'head_sha1'
    private static final long EXECUTE_BATCH_CONSTANT = 35
    private static final int FETCH_SIZE = 1000
    private static final String METHOD_NAME = '~method~'

    List<NCubeInfoDto> search(Connection c, ApplicationID appId, String cubeNamePattern, String searchContent, Map<String, Object> options)
    {
        List<NCubeInfoDto> list = new ArrayList<>()
        Pattern searchPattern = null

        if (StringUtilities.hasContent(searchContent))
        {
            options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA] = true
            searchPattern = Pattern.compile(convertPattern(searchContent), Pattern.CASE_INSENSITIVE)
        }
        options[METHOD_NAME] = 'search'
        runSelectCubesStatement(c, appId, cubeNamePattern, options, { ResultSet row -> getCubeInfoRecords(appId, searchPattern, list, row) })
        return list
    }

    List<AxisRef> getReferenceAxes(Connection c, ApplicationID appId)
    {
        // Step 1: Fetch all NCubeInfoDto's for the passed in ApplicationID
        List<NCubeInfoDto> list = search(c, appId, null, null, [:])
        List<AxisRef> refAxes = new ArrayList<>()

        for (NCubeInfoDto dto : list)
        {
            NCube source = loadCubeById(c, dto.id as long)
            for (Axis axis : source.getAxes())
            {
                if (axis.isReference())
                {
                    AxisRef ref = new AxisRef()
                    ref.srcAppId = appId
                    ref.srcCubeName = source.name
                    ref.srcAxisName = axis.name

                    ApplicationID refAppId = axis.getReferencedApp()
                    ref.destApp = refAppId.app
                    ref.destVersion = refAppId.version
                    ref.destCubeName = axis.getMetaProperty(ReferenceAxisLoader.REF_CUBE_NAME)
                    ref.destAxisName = axis.getMetaProperty(ReferenceAxisLoader.REF_AXIS_NAME)

                    ApplicationID transformAppId = axis.getTransformApp()
                    if (transformAppId)
                    {
                        ref.transformApp = transformAppId.app
                        ref.transformVersion = transformAppId.version
                        ref.transformCubeName = axis.getMetaProperty(ReferenceAxisLoader.TRANSFORM_CUBE_NAME)
                        ref.transformMethodName = axis.getMetaProperty(ReferenceAxisLoader.TRANSFORM_METHOD_NAME)
                    }

                    refAxes.add(ref)
                }
            }
        }
        return refAxes
    }

    boolean updateReferenceAxes(Connection c, List<AxisRef> axisRefs, String username)
    {
        for (AxisRef axisRef : axisRefs)
        {
            axisRef.with {
                NCube ncube = loadCube(c, srcAppId, srcCubeName)
                Axis axis = ncube.getAxis(srcAxisName)

                if (axis.isReference())
                {
                    axis.setMetaProperty(ReferenceAxisLoader.REF_APP, destApp)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_VERSION, destVersion)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_CUBE_NAME, destCubeName)
                    axis.setMetaProperty(ReferenceAxisLoader.REF_AXIS_NAME, destAxisName)
                    ApplicationID appId = new ApplicationID(srcAppId.tenant, destApp, destVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)

                    NCube target = loadCube(c, appId, destCubeName)
                    if (target == null)
                    {
                        throw new IllegalArgumentException('Cannot point reference axis to non-existing cube (' +
                                destCubeName + '). Source: ' + srcAppId + ' ' + srcCubeName + '.' + srcAxisName +
                                ', target: ' + destApp + ' / ' + destVersion + ' / ' + destCubeName + '.' + destAxisName)
                    }

                    if (target.getAxis(destAxisName) == null)
                    {
                        throw new IllegalArgumentException('Cannot point reference axis to non-existing axis (' +
                                destAxisName + '). Source: ' + srcAppId + ' ' + srcCubeName + '.' + srcAxisName +
                                ', target: ' + destApp + ' / ' + destVersion + ' / ' + destCubeName + '.' + destAxisName)
                    }

                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_APP, transformApp)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_VERSION, transformVersion)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_CUBE_NAME, transformCubeName)
                    axis.setMetaProperty(ReferenceAxisLoader.TRANSFORM_METHOD_NAME, transformMethodName)

                    if (transformApp && transformVersion && transformCubeName && transformMethodName)
                    {   // If transformer cube reference supplied, verify that the cube exists
                        ApplicationID txAppId = new ApplicationID(srcAppId.tenant, transformApp, transformVersion, ReleaseStatus.RELEASE.name(), ApplicationID.HEAD)
                        NCube transformCube = loadCube(c, txAppId, transformCubeName)
                        if (transformCube == null)
                        {
                            throw new IllegalArgumentException('Cannot point reference axis transformer to non-existing cube (' +
                                    transformCubeName + '). Source: ' + srcAppId + ' ' + srcCubeName + '.' + srcAxisName +
                                    ', target: ' + transformApp + ' / ' + transformVersion + ' / ' + transformCubeName + '.' + transformMethodName)
                        }

                        if (transformCube.getAxis(transformMethodName) == null)
                        {
                            throw new IllegalArgumentException('Cannot point reference axis transformer to non-existing axis (' +
                                    transformMethodName + '). Source: ' + srcAppId + ' ' + srcCubeName + '.' + srcAxisName +
                                    ', target: ' + transformApp + ' / ' + transformVersion + ' / ' + transformCubeName + '.' + transformMethodName)
                        }
                    }

                    ncube.clearSha1()   // changing meta properties does not clear SHA-1 for recalculation.
                    updateCube(c, axisRef.srcAppId, ncube, username)
                }
            }
        }
    }

    NCube loadCube(Connection c, ApplicationID appId, String cubeName)
    {
        Map<String, Object> options = [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY): true,
                                       (NCubeManager.SEARCH_INCLUDE_CUBE_DATA): true,
                                       (NCubeManager.SEARCH_EXACT_MATCH_NAME): true] as Map

        NCube cube = null
        options[METHOD_NAME] = 'loadCube'
        runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row -> cube = buildCube(appId, row) })
        return cube
    }

    NCube loadCubeById(Connection c, long cubeId)
    {
        Map map = [id: cubeId]
        Sql sql = new Sql(c)
        NCube cube = null
        sql.eachRow(map, "/* loadCubeById */ SELECT n_cube_nm, tenant_cd, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, changed, sha1, head_sha1 FROM n_cube where n_cube_id = :id", 0, 1, { ResultSet row ->
            String tenant = row.getString('tenant_cd')
            String status = row.getString('status_cd')
            String app = row.getString('app_cd')
            String version = row.getString('version_no_cd')
            String branch = row.getString('branch_id')
            ApplicationID appId = new ApplicationID(tenant.trim(), app, version, status, branch)
            cube = buildCube(appId, row)
        })
        if (cube)
        {
            return cube
        }
        throw new IllegalArgumentException("Unable to find cube with id: " + cubeId)
    }

    NCube loadCubeBySha1(Connection c, ApplicationID appId, String cubeName, String sha1)
    {
        Map map = appId as Map
        map.putAll([cube: buildName(c, cubeName), sha1: sha1])
        NCube cube = null

        new Sql(c).eachRow(map, "/* loadCubeBySha1 */ SELECT n_cube_id, n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, changed, sha1, head_sha1, create_dt " +
                "FROM n_cube " +
                "WHERE " + buildNameCondition(c, "n_cube_nm") + " = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch AND sha1 = :sha1 " +
                "ORDER BY abs(revision_number) DESC", 0, 1, { ResultSet row ->
                cube = buildCube(appId, row)
        })
        if (cube)
        {
            return cube
        }
        throw new IllegalArgumentException('Unable to find cube: ' + cubeName + ', app: ' + appId + ' with SHA-1: ' + sha1)
    }

    List<NCubeInfoDto> getRevisions(Connection c, ApplicationID appId, String cubeName)
    {
        List<NCubeInfoDto> records = new ArrayList<>()
        Map map = appId as Map
        map.cube = buildName(c, cubeName)
        Sql sql = new Sql(c)

        sql.eachRow(map, """\
/* getRevisions */ SELECT n_cube_id, n_cube_nm, notes_bin, version_no_cd, status_cd, app_cd, create_dt, create_hid, revision_number, branch_id, cube_value_bin, sha1, head_sha1, changed
 FROM n_cube
 WHERE """ + buildNameCondition(c, "n_cube_nm") + """ = :cube AND app_cd = :app AND version_no_cd = :version AND tenant_cd = RPAD(:tenant, 10, ' ') AND status_cd = :status AND branch_id = :branch
 ORDER BY abs(revision_number) DESC
""", {   ResultSet row -> getCubeInfoRecords(appId, null, records, row) })

        if (records.isEmpty())
        {
            throw new IllegalArgumentException("Cannot fetch revision history for cube: " + cubeName + " as it does not exist in app: " + appId)
        }
        return records
    }

    NCubeInfoDto insertCube(Connection c, ApplicationID appId, String name, Long revision, byte[] cubeData,
                            byte[] testData, String notes, boolean changed, String sha1, String headSha1, long time,
                            String username, String methodName) throws SQLException
    {
        PreparedStatement s = null
        try
        {
            s = c.prepareStatement("""\
/* """ + methodName + """.insertCube */ INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
 sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
            long uniqueId = UniqueIdGenerator.getUniqueId()
            s.setLong(1, uniqueId)
            s.setString(2, appId.tenant)
            s.setString(3, appId.app)
            s.setString(4, appId.version)
            s.setString(5, appId.status)
            s.setString(6, appId.branch)
            s.setString(7, name)
            s.setLong(8, revision)
            s.setString(9, sha1)
            s.setString(10, headSha1)
            Timestamp now = new Timestamp(time)
            s.setTimestamp(11, now)
            s.setString(12, username)
            s.setBytes(13, cubeData)
            s.setBytes(14, testData)
            String note = createNote(username, now, notes)
            s.setBytes(15, StringUtilities.getBytes(note, "UTF-8"))
            s.setInt(16, changed ? 1 : 0)

            NCubeInfoDto dto = new NCubeInfoDto()
            dto.id = Long.toString(uniqueId)
            dto.name = name
            dto.sha1 = sha1
            dto.headSha1 = sha1
            dto.changed = changed
            dto.tenant = appId.tenant
            dto.app = appId.app
            dto.version = appId.version
            dto.status = appId.status
            dto.branch = appId.branch
            dto.createDate = new Date(time)
            dto.createHid = username
            dto.notes = note
            dto.revision = Long.toString(revision)

            int rows = s.executeUpdate()
            if (rows == 1)
            {
                return dto
            }
            throw new IllegalStateException('Unable to insert cube: ' + name + ' into database, app: ' + appId + ', attempted action: ' + notes)
        }
        finally
        {
            s?.close()
        }
    }

    NCubeInfoDto insertCube(Connection c, ApplicationID appId, NCube cube, Long revision, byte[] testData, String notes,
                            boolean changed, String headSha1, long time, String username, String methodName)
    {
        long uniqueId = UniqueIdGenerator.getUniqueId()
        Timestamp now = new Timestamp(time)
        final Blob blob = c.createBlob()
        OutputStream out = blob.setBinaryStream(1L)
        OutputStream stream = new GZIPOutputStream(out, 8192)
        new JsonFormatter(stream).formatCube(cube, null)
        PreparedStatement s = null

        try
        {
            s = c.prepareStatement('''\
/* ''' + methodName + '''.insertCube */ INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
 sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''')
            s.setLong(1, uniqueId)
            s.setString(2, appId.tenant)
            s.setString(3, appId.app)
            s.setString(4, appId.version)
            s.setString(5, appId.status)
            s.setString(6, appId.branch)
            s.setString(7, cube.name)
            s.setLong(8, revision)
            s.setString(9, cube.sha1())
            s.setString(10, headSha1)
            s.setTimestamp(11, now)
            s.setString(12, username)
            s.setBlob(13, blob)
            s.setBytes(14, testData)
            String note = createNote(username, now, notes)
            s.setBytes(15, StringUtilities.getBytes(note, "UTF-8"))
            s.setBoolean(16, changed)

            NCubeInfoDto dto = new NCubeInfoDto()
            dto.id = Long.toString(uniqueId)
            dto.name = cube.name
            dto.sha1 = cube.sha1()
            dto.headSha1 = cube.sha1()
            dto.changed = changed
            dto.tenant = appId.tenant
            dto.app = appId.app
            dto.version = appId.version
            dto.status = appId.status
            dto.branch = appId.branch
            dto.createDate = new Date(time)
            dto.createHid = username
            dto.notes = note
            dto.revision = Long.toString(revision)

            int rows = s.executeUpdate()
            if (rows == 1)
            {
                return dto
            }
            throw new IllegalStateException('Unable to insert cube: ' + cube.name + ' into database, app: ' + appId + ", attempted action: " + notes)
        }
        finally
        {
            s?.close()
        }
    }

    boolean deleteCubes(Connection c, ApplicationID appId, Object[] cubeNames, boolean allowDelete, String username)
    {
        boolean autoCommit = c.getAutoCommit()
        PreparedStatement stmt = null
        try
        {
            c.setAutoCommit(false)
            int count = 0
            if (allowDelete)
            {   // Not the most efficient, but this is only used for testing, never from running app.
                String sqlCmd = "/* deleteCubes */ DELETE FROM n_cube WHERE app_cd = ? AND " + buildNameCondition(c, "n_cube_nm") + " = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?"
                stmt = c.prepareStatement(sqlCmd)
                for (int i = 0; i < cubeNames.length; i++)
                {

                    stmt.setString(1, appId.app)
                    stmt.setString(2, buildName(c, (String) cubeNames[i]))
                    stmt.setString(3, appId.version)
                    stmt.setString(4, appId.tenant)
                    stmt.setString(5, appId.branch)
                    count += stmt.executeUpdate()
                }
                return count > 0
            }

            stmt = c.prepareStatement("""\
/* deleteCubes */ INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
 sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")


            Map<String, Object> options = [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY): true,
                                           (NCubeManager.SEARCH_INCLUDE_CUBE_DATA)  : true,
                                           (NCubeManager.SEARCH_INCLUDE_TEST_DATA)  : true,
                                           (NCubeManager.SEARCH_EXACT_MATCH_NAME)   : true,
                                           (METHOD_NAME) : 'deleteCubes'] as Map
            cubeNames.each { String cubeName ->
                Long revision = null
                runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
                    revision = row.getLong('revision_number')
                    addBatchInsert(stmt, row, appId, cubeName, -(revision + 1), "deleted", username, ++count)
                })
                if (revision == null)
                {
                    throw new IllegalArgumentException("Cannot delete cube: " + cubeName + " as it does not exist in app: " + appId)
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                stmt.executeBatch()
            }
            return count > 0
        }
        finally
        {
            c.setAutoCommit(autoCommit)
            stmt?.close()
        }
    }

    boolean restoreCubes(Connection c, ApplicationID appId, Object[] names, String username)
    {
        boolean autoCommit = c.getAutoCommit()
        PreparedStatement ins = null
        try
        {
            c.setAutoCommit(false)
            ins = c.prepareStatement("""\
/* restoreCubes */ INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

            Map<String, Object> options = [(NCubeManager.SEARCH_DELETED_RECORDS_ONLY): true,
                                           (NCubeManager.SEARCH_INCLUDE_CUBE_DATA)   : true,
                                           (NCubeManager.SEARCH_INCLUDE_TEST_DATA)   : true,
                                           (NCubeManager.SEARCH_EXACT_MATCH_NAME)    : true,
                                           (METHOD_NAME) : 'restoreCubes'] as Map
            int count = 0
            names.each { String cubeName ->
                Long revision = null
                runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
                    revision = row.getLong('revision_number')
                    addBatchInsert(ins, row, appId, cubeName, Math.abs(revision as long) + 1, "restored", username, ++count)
                })

                if (revision == null)
                {
                    throw new IllegalArgumentException("Cannot restore cube: " + cubeName + " as it not deleted in app: " + appId)
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                ins.executeBatch()
            }
            return count > 0
        }
        finally
        {
            c.setAutoCommit(autoCommit)
            ins?.close()
        }
    }

    private void addBatchInsert(PreparedStatement stmt, ResultSet row, ApplicationID appId, String cubeName, long rev, String action, String username, int count)
    {
        byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
        byte[] testData = row.getBytes(TEST_DATA_BIN)
        String sha1 = row.getString('sha1')
        String headSha1 = row.getString('head_sha1')

        long uniqueId = UniqueIdGenerator.getUniqueId()
        stmt.setLong(1, uniqueId)
        stmt.setString(2, appId.tenant)
        stmt.setString(3, appId.app)
        stmt.setString(4, appId.version)
        stmt.setString(5, appId.status)
        stmt.setString(6, appId.branch)
        stmt.setString(7, cubeName)
        stmt.setLong(8, rev)
        stmt.setString(9, sha1)
        stmt.setString(10, headSha1)
        Timestamp now = new Timestamp(System.currentTimeMillis())
        stmt.setTimestamp(11, now)
        stmt.setString(12, username)
        stmt.setBytes(13, jsonBytes)
        stmt.setBytes(14, testData)
        stmt.setBytes(15, StringUtilities.getBytes(createNote(username, now, action), "UTF-8"))
        stmt.setInt(16, 1)
        stmt.addBatch()
        if (count % EXECUTE_BATCH_CONSTANT == 0)
        {
            stmt.executeBatch()
        }
    }

    List<NCubeInfoDto> pullToBranch(Connection c, ApplicationID appId, Object[] cubeIds, String username)
    {
        List<NCubeInfoDto> infoRecs = new ArrayList<>()
        if (ArrayUtilities.isEmpty(cubeIds))
        {
            return infoRecs
        }

        String sql = "/* pullToBranch */ SELECT n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, sha1, head_sha1, create_dt from n_cube WHERE n_cube_id = ?"
        PreparedStatement stmt = null
        try
        {
            stmt = c.prepareStatement(sql)
            for (int i = 0; i < cubeIds.length; i++)
            {
                stmt.setLong(1, (Long) Converter.convert(cubeIds[i], Long.class))
                ResultSet row = stmt.executeQuery()

                if (row.next())
                {
                    byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
                    String sha1 = row.getString('sha1')
                    String cubeName = row.getString('n_cube_nm')
                    Long revision = row.getLong('revision_number')
                    long time = row.getTimestamp('create_dt').getTime()
                    String branch = row.getString('branch_id')
                    byte[] testData = row.getBytes(TEST_DATA_BIN)

                    Long maxRevision = getMaxRevision(c, appId, cubeName, 'pullToBranch')

                    //  create case because max revision was not found.
                    if (maxRevision == null)
                    {
                        maxRevision = revision < 0 ? new Long(-1) : new Long(0)
                    }
                    else if (revision < 0)
                    {
                        // cube deleted in branch
                        maxRevision = -(Math.abs(maxRevision as long) + 1)
                    }
                    else
                    {
                        maxRevision = Math.abs(maxRevision as long) + 1
                    }

                    NCubeInfoDto dto = insertCube(c, appId, cubeName, maxRevision, jsonBytes, testData, 'updated from ' + branch, false, sha1, sha1, time, username, 'pullToBranch')
                    infoRecs.add(dto)
                }
            }
        }
        finally
        {
            stmt?.close()
        }
        return infoRecs
    }

    void updateCube(Connection c, ApplicationID appId, NCube cube, String username)
    {
        Map<String, Object> options = [(NCubeManager.SEARCH_INCLUDE_CUBE_DATA): true,
                                       (NCubeManager.SEARCH_INCLUDE_TEST_DATA): true,
                                       (NCubeManager.SEARCH_EXACT_MATCH_NAME): true,
                                       (METHOD_NAME) : 'updateCube'] as Map
        boolean rowFound = false
        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            rowFound = true
            Long revision = row.getLong("revision_number")
            byte[] testData = row.getBytes(TEST_DATA_BIN)

            if (revision < 0)
            {
                testData = null
            }

            String headSha1 = row.getString('head_sha1')
            String oldSha1 = row.getString('sha1')

            if (StringUtilities.equals(oldSha1, cube.sha1()) && revision >= 0)
            {
                // SHA-1's are equals and both revision values are positive.  No need for new revision of record.
                return
            }

            insertCube(c, appId, cube, Math.abs(revision as long) + 1, testData, "updated", true, headSha1, System.currentTimeMillis(), username, 'updateCube')
        })

        // No existing row found, then create a new cube (updateCube can be used for update or create)
        if (!rowFound)
        {
            insertCube(c, appId, cube, 0L, null, "created", true, null, System.currentTimeMillis(), username, 'updateCube')
        }
    }

    boolean duplicateCube(Connection c, ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    {
        byte[] jsonBytes = null
        Long oldRevision = null
        byte[] oldTestData = null
        String sha1 = null

        Map<String, Object> options = [
                (NCubeManager.SEARCH_INCLUDE_CUBE_DATA):true,
                (NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                (NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'duplicateCube'] as Map

        runSelectCubesStatement(c, oldAppId, oldName, options, 1, { ResultSet row ->
            jsonBytes = row.getBytes(CUBE_VALUE_BIN)
            oldRevision = row.getLong('revision_number')
            oldTestData = row.getBytes(TEST_DATA_BIN)
            sha1 = row.getString('sha1')
        })

        if (oldRevision == null)
        {   // not found
            throw new IllegalArgumentException("Could not duplicate cube because cube does not exist, app:  " + oldAppId + ", name: " + oldName)
        }

        if (oldRevision < 0)
        {
            throw new IllegalArgumentException("Unable to duplicate deleted cube, app:  " + oldAppId + ", name: " + oldName)
        }

        Long newRevision = null
        String headSha1 = null

        // Do not include test, n-cube, or notes blob columns in search - much faster
        Map<String, Object> options1 = [(NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'duplicateCube'] as Map
        runSelectCubesStatement(c, newAppId, newName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            headSha1 = row.getString('head_sha1')
        })

        if (newRevision != null && newRevision >= 0)
        {
            throw new IllegalArgumentException("Unable to duplicate cube, cube already exists with the new name, app:  " + newAppId + ", name: " + newName)
        }

        boolean changed = !StringUtilities.equalsIgnoreCase(oldName, newName)
        boolean sameExceptBranch = oldAppId.equalsNotIncludingBranch(newAppId)

        // If names are different we need to recalculate the sha-1
        if (changed)
        {
            NCube ncube = NCube.createCubeFromBytes(jsonBytes)
            ncube.setName(newName)
            ncube.setApplicationID(newAppId)
            jsonBytes = ncube.getCubeAsGzipJsonBytes()
            sha1 = ncube.sha1()
        }

        String notes = 'Cube duplicated from app: ' + oldAppId + ', name: ' + oldName
        Long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L
        insertCube(c, newAppId, newName, rev, jsonBytes, oldTestData, notes, changed, sha1, sameExceptBranch ? headSha1 : null, System.currentTimeMillis(), username, 'duplicateCube')
        return true
    }

    boolean renameCube(Connection c, ApplicationID appId, String oldName, String newName, String username)
    {
        byte[] oldBytes = null
        Long oldRevision = null
        String oldSha1 = null
        String oldHeadSha1 = null
        byte[] oldTestData = null

        Map<String, Object> options = [
                (NCubeManager.SEARCH_INCLUDE_CUBE_DATA):true,
                (NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                (NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'renameCube'] as Map

        runSelectCubesStatement(c, appId, oldName, options, 1, { ResultSet row ->
            oldBytes = row.getBytes(CUBE_VALUE_BIN)
            oldRevision = row.getLong('revision_number')
            oldTestData = row.getBytes(TEST_DATA_BIN)
            oldSha1 = row.getString('sha1')
            oldHeadSha1 = row.getString('head_sha1')
        })

        if (oldRevision == null)
        {   // not found
            throw new IllegalArgumentException("Could not rename cube because cube does not exist, app:  " + appId + ", name: " + oldName)
        }

        if (oldRevision != null && oldRevision < 0)
        {
            throw new IllegalArgumentException("Deleted cubes cannot be renamed.  AppId:  " + appId + ", " + oldName + " -> " + newName)
        }

        Long newRevision = null
        String newHeadSha1 = null

        // Do not include n-cube, tests, or notes in search
        Map<String, Object> options1 = [(NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'renameCube'] as Map
        runSelectCubesStatement(c, appId, newName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            newHeadSha1 = row.getString(HEAD_SHA_1)
        })

        if (newRevision != null && newRevision >= 0)
        {
            throw new IllegalArgumentException("Unable to rename cube, a cube already exists with that name, app:  " + appId + ", name: " + newName)
        }

        NCube ncube = NCube.createCubeFromBytes(oldBytes)
        ncube.setName(newName)
        String notes = "renamed: " + oldName + " -> " + newName

        Long rev = newRevision == null ? 0L : Math.abs(newRevision as long) + 1L
        insertCube(c, appId, ncube, rev, oldTestData, notes, true, newHeadSha1, System.currentTimeMillis(), username, 'renameCube')
        insertCube(c, appId, oldName, -(oldRevision + 1), oldBytes, oldTestData, notes, true, oldSha1, oldHeadSha1, System.currentTimeMillis(), username, 'renameCube')
        return true
    }

    NCubeInfoDto commitMergedCubeToBranch(Connection c, ApplicationID appId, NCube cube, String headSha1, String username)
    {
        Map options = [(NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                       (NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                       (METHOD_NAME) : 'commitMergedCubeToBranch'] as Map

        NCubeInfoDto result = null

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            Long revision = row.getLong('revision_number')
            byte[] testData = row.getBytes(TEST_DATA_BIN)
            long now = System.currentTimeMillis()
            revision = revision < 0 ? revision - 1 : revision + 1
            result = insertCube(c, appId, cube, revision, testData, "merged", true, headSha1, now, username, 'commitMergedCubeToBranch')
        })
        return result
    }

    NCubeInfoDto commitMergedCubeToHead(Connection c, ApplicationID appId, NCube cube, String username)
    {
        final String methodName = 'commitMergedCubeToHead'
        Map options = [(NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                       (NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                       (METHOD_NAME) : methodName]

        ApplicationID headAppId = appId.asHead()
        NCubeInfoDto result = null

        runSelectCubesStatement(c, appId, cube.name, options, 1, { ResultSet row ->
            Long revision = row.getLong('revision_number')

            // get current max HEAD revision
            Long maxRevision = getMaxRevision(c, headAppId, cube.name, methodName)

            if (maxRevision == null)
            {
                maxRevision = revision < 0 ? -1L : 0L
            }
            else if (revision < 0)
            {
                // cube deleted in branch
                maxRevision = -(Math.abs(maxRevision as long) + 1)
            }
            else
            {
                maxRevision = Math.abs(maxRevision as long) + 1
            }

            byte[] testData = row.getBytes(TEST_DATA_BIN)
            long now = System.currentTimeMillis()
            // ok to use this here, because we're writing out these bytes twice (once to head and once to branch)
            byte[] cubeData = cube.getCubeAsGzipJsonBytes()
            String sha1 = cube.sha1()

            insertCube(c, headAppId, cube.name, maxRevision, cubeData, testData, "merged, committed", false, sha1, null, now, username, methodName)
            result = insertCube(c, appId, cube.name, revision > 0 ? ++revision : --revision, cubeData, testData, "merged", false, sha1, sha1, now, username,  methodName)
        })
        return result
    }

    List<NCubeInfoDto> commitCubes(Connection c, ApplicationID appId, Object[] cubeIds, String username)
    {
        List<NCubeInfoDto> infoRecs = new ArrayList<>()
        if (ArrayUtilities.isEmpty(cubeIds))
        {
            return infoRecs
        }

        ApplicationID headAppId = appId.asHead()
        Sql sql = new Sql(c)
        def map = [:]

        for (int i = 0; i < cubeIds.length; i++)
        {
            map.id = Converter.convert(cubeIds[i], Long.class)
            sql.eachRow("/* commitCubes */ SELECT n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, sha1, head_sha1 from n_cube WHERE n_cube_id = :id",
                    map, 0, 1, { ResultSet row ->
                byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
                String sha1 = row.getString("sha1")
                String cubeName = row.getString("n_cube_nm")
                Long revision = row.getLong("revision_number")
                Long maxRevision = getMaxRevision(c, headAppId, cubeName, 'commitCubes')

                //  create case because max revision was not found.
                String changeType
                boolean skip = false
                if (maxRevision == null)
                {
                    if (revision < 0)
                    {   // User created then deleted cube, but it has no HEAD corresponding cube, don't promote it
                        skip = true
                    }
                    else
                    {
                        changeType = ChangeType.CREATED.code
                        maxRevision = 0L
                    }
                }
                else if (revision < 0)
                {
                    if (maxRevision < 0)
                    {   // Deleted in both, don't promote it
                        skip = true
                    }
                    else
                    {
                        changeType = ChangeType.DELETED.code
                        maxRevision = -(maxRevision + 1)
                    }
                }
                else
                {
                    if (maxRevision < 0)
                    {
                        changeType = ChangeType.RESTORED.code
                    }
                    else
                    {
                        changeType = ChangeType.UPDATED.code
                    }
                    maxRevision = Math.abs(maxRevision as long) + 1
                }

                if (!skip)
                {
                    byte[] testData = row.getBytes(TEST_DATA_BIN)
                    long now = System.currentTimeMillis()

                    NCubeInfoDto dto = insertCube(c, headAppId, cubeName, maxRevision, jsonBytes, testData, 'committed', false, sha1, null, now, username, 'commitCubes')
                    def map1 = [head_sha1: sha1, create_dt: new Timestamp(now), id: cubeIds[i]]
                    Sql sql1 = new Sql(c)

                    sql1.executeUpdate(map1, '/* commitCubes */ UPDATE n_cube set head_sha1 = :head_sha1, changed = 0, create_dt = :create_dt WHERE n_cube_id = :id')
                    dto.changed = false
                    dto.changeType = changeType
                    dto.id = Converter.convert(cubeIds[i], String.class)
                    dto.sha1 = sha1
                    dto.headSha1 = sha1
                    infoRecs.add(dto)
                }
            })
        }
        return infoRecs
    }

    /**
     * Rollback branch cube to the last time it matched the HEAD branch (SHA-1 == HEAD_SHA-1).  This entails
     * going through the revision history from highest revision toward lowest revision, and finding the
     * last time sha1 == headSha1.  When found, INSERT a new record that is a copy of that record, with
     * revision_number == max(revision_number) + 1.
     * If there are no matches, then the revision_number inserted is negative (deleted).  This is the case of
     * creating a new cube and never calling commit with it.
     */
    int rollbackCubes(Connection c, ApplicationID appId, Object[] names, String username)
    {
        int count = 0
        boolean autoCommit = c.getAutoCommit()
        PreparedStatement ins = null
        try
        {
            c.setAutoCommit(false)
            ins = c.prepareStatement("""\
/* rollbackCubes */ INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number,
 sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

            names.each { String cubeName ->
                Long maxRev = getMaxRevision(c, appId, cubeName, 'rollbackCubes')
                if (maxRev == null)
                {
                    LOG.info('Attempt to rollback non-existing cube: ' + cubeName + ', app: ' + appId)
                }
                else
                {
                    Long rollbackRev = findRollbackRevision(c, appId, cubeName)
                    boolean mustDelete = rollbackRev == null
                    Map map = appId as Map
                    map.putAll([cube: buildName(c, cubeName), rev: (mustDelete ? maxRev : rollbackRev)])
                    Sql sql = new Sql(c)

                    sql.eachRow(map, """\
/* rollbackCubes */ SELECT n_cube_id, n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, changed, sha1, head_sha1, create_dt
 FROM n_cube
 WHERE """ + buildNameCondition(c, "n_cube_nm") + """ = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status
 AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch AND revision_number = :rev""", 0, 1, { ResultSet row ->
                        byte[] bytes = row.getBytes(CUBE_VALUE_BIN)
                        byte[] testData = row.getBytes(TEST_DATA_BIN)
                        String sha1 = row.getString('sha1')
                        String headSha1 = row.getString('head_sha1')

                        String notes = "rolled back"
                        Long rev = Math.abs(maxRev as long) + 1L

                        long uniqueId = UniqueIdGenerator.getUniqueId()
                        ins.setLong(1, uniqueId)
                        ins.setString(2, appId.tenant)
                        ins.setString(3, appId.app)
                        ins.setString(4, appId.version)
                        ins.setString(5, appId.status)
                        ins.setString(6, appId.branch)
                        ins.setString(7, cubeName)
                        ins.setLong(8, mustDelete ? -rev : rev)
                        ins.setString(9, sha1)
                        ins.setString(10, headSha1)
                        Timestamp now = new Timestamp(System.currentTimeMillis())
                        ins.setTimestamp(11, now)
                        ins.setString(12, username)
                        ins.setBytes(13, bytes)
                        ins.setBytes(14, testData)
                        String note = createNote(username, now, notes)
                        ins.setBytes(15, StringUtilities.getBytes(note, "UTF-8"))
                        ins.setInt(16, 0)
                        ins.addBatch()
                        count++
                        if (count % EXECUTE_BATCH_CONSTANT == 0)
                        {
                            ins.executeBatch()
                        }
                    })
                }
            }
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                ins.executeBatch()
            }
        }
        finally
        {
            c.setAutoCommit(autoCommit)
            ins?.close()
        }
        return count
    }

    private Long findRollbackRevision(Connection c, ApplicationID appId, String cubeName)
    {
        Sql sql = new Sql(c)
        Map map = appId as Map
        map.putAll([cube: buildName(c, cubeName)])
        Long maxRev = null

        sql.eachRow(map, """\
/* rollbackCubes.findRollbackRev */ SELECT revision_number FROM n_cube
 WHERE """ + buildNameCondition(c, "n_cube_nm") + """ = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status
 AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch AND revision_number >= 0 AND sha1 = head_sha1
 ORDER BY revision_number desc""", 0, 1, { ResultSet row ->
            maxRev = row.getLong("revision_number")
        });
        return maxRev
    }

    /**
     * Fast forward branch cube to HEAD cube, because even though it's HEAD_SHA-1 value is out-of-date,
     * the cubes current SHA-1 is the same as the HEAD cube's SHA-1.  Therefore, we can 'scoot' up the
     * cube record's HEAD-SHA-1 value to the same as the HEAD Cube's SHA-1.
     */
    boolean updateBranchCubeHeadSha1(Connection c, Long cubeId, String headSha1)
    {
        if (cubeId == null)
        {
            throw new IllegalArgumentException("Update branch cube's HEAD SHA-1, cube id cannot be empty")
        }

        if (StringUtilities.isEmpty(headSha1))
        {
            throw new IllegalArgumentException("Update branch cube's HEAD SHA-1, SHA-1 cannot be empty")
        }

        Map map = [sha1:headSha1, id: cubeId]
        Sql sql = new Sql(c)
        int count = sql.executeUpdate(map, '/* updateBranchCubeHeadSha1 */ UPDATE n_cube set head_sha1 = :sha1, changed = 0 WHERE n_cube_id = :id')
        if (count == 0)
        {
            throw new IllegalArgumentException("error updating branch cube: " + cubeId + ", to HEAD SHA-1: " + headSha1 + ", no record found.")
        }
        if (count != 1)
        {
            throw new IllegalStateException("error updating branch cube: " + cubeId + ", to HEAD SHA-1: " + headSha1 + ", more than one record found: " + count)
        }
        return true
    }

    boolean mergeAcceptMine(Connection c, ApplicationID appId, String cubeName, String username)
    {
        ApplicationID headId = appId.asHead()
        Long headRevision = null
        String headSha1 = null

        Map<String, Object> options = [(NCubeManager.SEARCH_EXACT_MATCH_NAME): true,
                                       (METHOD_NAME) : 'mergeAcceptMine'] as Map

        runSelectCubesStatement(c, headId, cubeName, options, 1, { ResultSet row ->
            headRevision = row.getLong('revision_number')
            headSha1 = row.getString('sha1')
        })

        if (headRevision == null)
        {
            throw new IllegalStateException("failed to update branch cube because HEAD cube does not exist: " + cubeName + ", app: " + appId)
        }

        Long newRevision = null
        String tipBranchSha1 = null
        byte[] myTestData = null
        byte[] myBytes = null
        boolean changed = false
        options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA] = true
        options[NCubeManager.SEARCH_INCLUDE_TEST_DATA] = true
        options[METHOD_NAME] = 'mergeAcceptMine'

        runSelectCubesStatement(c, appId, cubeName, options, 1, { ResultSet row ->
            myBytes = row.getBytes(CUBE_VALUE_BIN)
            myTestData = row.getBytes(TEST_DATA_BIN)
            newRevision = row.getLong('revision_number')
            tipBranchSha1 = row.getString('sha1')
            changed = row.getBoolean('changed')
        })

        if (newRevision == null)
        {
            throw new IllegalStateException("failed to update branch cube because branch cube does not exist: " + cubeName + ", app: " + appId)
        }

        String notes = 'merge: branch accepted over head'
        Long rev = Math.abs(newRevision as long) + 1L
        insertCube(c, appId, cubeName, newRevision < 0 ? -rev : rev, myBytes, myTestData, notes, changed, tipBranchSha1, headSha1, System.currentTimeMillis(), username, 'mergeAcceptMine')
        return true
    }

    boolean mergeAcceptTheirs(Connection c, ApplicationID appId, String cubeName, String branchSha1, String username)
    {
        ApplicationID headId = appId.asHead()
        byte[] headBytes = null
        Long headRevision = null
        byte[] headTestData = null
        String headSha1 = null

        Map<String, Object> options = [
                (NCubeManager.SEARCH_INCLUDE_CUBE_DATA):true,
                (NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                (NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                (METHOD_NAME) : 'mergeAcceptTheirs'] as Map

        runSelectCubesStatement(c, headId, cubeName, options, 1, { ResultSet row ->
            headBytes = row.getBytes(CUBE_VALUE_BIN)
            headTestData = row.getBytes(TEST_DATA_BIN)
            headRevision = row.getLong('revision_number')
            headSha1 = row.getString('sha1')
        })

        if (headRevision == null)
        {
            throw new IllegalStateException("Failed to overwrite cube in your branch, because 'their' cube does not exist: " + cubeName + ", app: " + appId)
        }

        Long newRevision = null
        String oldBranchSha1 = null

        // Do not use cube_value_bin, test data, or notes to speed up search
        Map<String, Object> options1 = [(NCubeManager.SEARCH_EXACT_MATCH_NAME):true,
                                        (METHOD_NAME) : 'mergeAcceptTheirs'] as Map
        runSelectCubesStatement(c, appId, cubeName, options1, 1, { ResultSet row ->
            newRevision = row.getLong('revision_number')
            oldBranchSha1 = row.getString('sha1')
        })

        if (newRevision == null)
        {
            throw new IllegalStateException("failed to overwrite cube in your branch, because branch cube does not exist: " + cubeName + ", app: " + appId)
        }

        if (!StringUtilities.equalsIgnoreCase(branchSha1, oldBranchSha1))
        {
            throw new IllegalStateException("failed to overwrite cube in your branch, because branch cube has changed: " + cubeName + ", app: " + appId)
        }

        String notes = 'merge: head accepted over branch'
        Long rev = Math.abs(newRevision as long) + 1L
        insertCube(c, appId, cubeName, headRevision < 0 ? -rev : rev, headBytes, headTestData, notes, false, headSha1, headSha1, System.currentTimeMillis(), username, 'mergeAcceptTheirs')
        return true
    }

    protected void runSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map options, Closure closure)
    {
        runSelectCubesStatement(c, appId, namePattern, options, 0, closure)
    }

    /**
     * @param c Connection (JDBC) from ConnectionProvider
     * @param appId ApplicationID
     * @param namePattern String name pattern (using wildcards * and ?)
     * @param options map with possible keys:
     *                changedRecordsOnly - default false
     *                activeRecordsOnly - default false
     *                deletedRecordsOnly - default false
     *                includeCubeData - default false
     *                includeTestData - default false
     *                exactMatchName - default false
     * @param closure Closure to run for each record selected.
     */
    protected void runSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map options, int max, Closure closure)
    {
        boolean includeCubeData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA])
        boolean includeTestData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_TEST_DATA])
        boolean includeNotes = toBoolean(options[NCubeManager.SEARCH_INCLUDE_NOTES])
        boolean changedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_CHANGED_RECORDS_ONLY])
        boolean activeRecordsOnly = toBoolean(options[NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY])
        boolean deletedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_DELETED_RECORDS_ONLY])
        boolean exactMatchName = toBoolean(options[NCubeManager.SEARCH_EXACT_MATCH_NAME])
        String methodName = options[METHOD_NAME]
        if (StringUtilities.isEmpty(methodName))
        {
            methodName = 'methodNameNotSet'
        }

        if (activeRecordsOnly && deletedRecordsOnly)
        {
            throw new IllegalArgumentException("activeRecordsOnly and deletedRecordsOnly are mutually exclusive options and cannot both be 'true'.")
        }

        namePattern = convertPattern(buildName(c, namePattern))
        boolean hasNamePattern = StringUtilities.hasContent(namePattern)
        String nameCondition1 = ''
        String nameCondition2 = ''
        Map map = appId as Map
        map.putAll([name:namePattern, changed:changedRecordsOnly])

        if (hasNamePattern)
        {
            nameCondition1 = ' AND ' + buildNameCondition(c, 'n_cube_nm') + (exactMatchName ? ' = :name' : ' LIKE :name')
            nameCondition2 = ' AND ' + buildNameCondition(c, 'm.n_cube_nm') + (exactMatchName ? ' = :name' : ' LIKE :name')
        }

        String revisionCondition = activeRecordsOnly ? ' AND n.revision_number >= 0' : deletedRecordsOnly ? ' AND n.revision_number < 0' : ''
        String changedCondition = changedRecordsOnly ? ' AND n.changed = :changed' : ''
        String testCondition = includeTestData ? ', n.test_data_bin' : ''
        String cubeCondition = includeCubeData ? ', n.cube_value_bin' : ''
        String notesCondition = includeNotes ? ', n.notes_bin' : ''

        Sql sql = new Sql(c)

        String select = '''\
/* ''' + methodName + ''' */ SELECT n.n_cube_id, n.n_cube_nm, n.app_cd, n.notes_bin, n.version_no_cd, n.status_cd, n.create_dt, n.create_hid, n.revision_number, n.branch_id, n.changed, n.sha1, n.head_sha1 ''' +
testCondition +
cubeCondition +
notesCondition + '''\
 FROM n_cube n,
 ( SELECT n_cube_nm, max(abs(revision_number)) AS max_rev
  FROM n_cube
  WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch ''' +
 nameCondition1 + '''\
 GROUP BY n_cube_nm ) m
 WHERE m.n_cube_nm = n.n_cube_nm AND m.max_rev = abs(n.revision_number) AND n.app_cd = :app AND n.version_no_cd = :version AND n.status_cd = :status AND n.tenant_cd = RPAD(:tenant, 10, ' ') AND n.branch_id = :branch''' +
revisionCondition +
changedCondition +
nameCondition2

        if (max >= 1)
        {   // Use pre-closure to fiddle with batch fetchSize and to monitor row count
            long count = 0
            sql.eachRow(map, select, 0, max + 1, { ResultSet row ->
                if (row.getFetchSize() < FETCH_SIZE)
                {
                    row.setFetchSize(FETCH_SIZE)
                }
                if (count > max)
                {
                    throw new IllegalStateException('More than one result returned, expecting only 1')
                }
                count++
                closure(row)
            })
        }
        else
        {   // Use pre-closure to fiddle with batch fetchSizes
            sql.eachRow(map, select, { ResultSet row ->
                if (row.getFetchSize() < FETCH_SIZE)
                {
                    row.setFetchSize(FETCH_SIZE)
                }
                closure(row)
            })
        }
    }

    int createBranch(Connection c, ApplicationID appId)
    {
        if (doCubesExist(c, appId, true, 'createBranch'))
        {
            throw new IllegalStateException("Branch '" + appId.branch + "' already exists, app: " + appId)
        }

        ApplicationID headId = appId.asHead()
        Map<String, Object> options = [(NCubeManager.SEARCH_INCLUDE_CUBE_DATA): true,
                                       (NCubeManager.SEARCH_INCLUDE_TEST_DATA): true,
                                       (METHOD_NAME) : 'createBranch'] as Map
        int count = 0
        boolean autoCommit = c.getAutoCommit()
        PreparedStatement insert = null
        try
        {
            c.setAutoCommit(false)
            insert = c.prepareStatement(
                    "/* createBranch */ INSERT INTO n_cube (n_cube_id, n_cube_nm, cube_value_bin, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number, changed, sha1, head_sha1) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            runSelectCubesStatement(c, headId, null, options, { ResultSet row ->
                String sha1 = row['sha1'] as String
                insert.setLong(1, UniqueIdGenerator.getUniqueId())
                insert.setString(2, row.getString('n_cube_nm'))
                insert.setBytes(3, row.getBytes(CUBE_VALUE_BIN))
                insert.setTimestamp(4, row.getTimestamp('create_dt'))
                insert.setString(5, row.getString('create_hid'))
                insert.setString(6, appId.version)
                insert.setString(7, ReleaseStatus.SNAPSHOT.name())
                insert.setString(8, appId.app)
                insert.setBytes(9, row.getBytes(TEST_DATA_BIN))
                insert.setBytes(10, ('branch ' + appId.version + ' created').getBytes('UTF-8'))
                insert.setString(11, appId.tenant)
                insert.setString(12, appId.branch)
                insert.setLong(13, (row.getLong('revision_number') >= 0) ? 0 : -1)
                insert.setBoolean(14, false)
                insert.setString(15, sha1)
                insert.setString(16, sha1)
                insert.addBatch()
                count++
                if (count % EXECUTE_BATCH_CONSTANT == 0)
                {
                    insert.executeBatch()
                }
            })
            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                insert.executeBatch()
            }
            return count
        }
        finally
        {
            c.setAutoCommit(autoCommit)
            insert?.close()
        }
    }

    boolean deleteBranch(Connection c, ApplicationID appId)
    {
        Map map = appId as Map
        new Sql(c).execute(map, "/* deleteBranch */ DELETE FROM n_cube WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch")
        return true
    }

    int releaseCubes(Connection c, ApplicationID appId, String newSnapVer)
    {
        // Step 1: Move everyone's SNAPSHOT version cubes to new version.
        // (Update version number to new version where branch != HEAD (and rest of appId matches) ignore revision)
        Map map = appId as Map
        map.putAll([newVer: newSnapVer])
        Sql sql = new Sql(c)
        sql.executeUpdate(map, "/* releaseCubes */ UPDATE n_cube SET version_no_cd = :newVer WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id != 'HEAD'")

        // Step 2: Release cubes where branch == HEAD (change their status from SNAPSHOT to RELEASE)
        map.create_dt = new Timestamp(System.currentTimeMillis())
        int releaseCount = sql.executeUpdate(map, "/* releaseCubes */ UPDATE n_cube SET create_dt = :create_dt, status_cd = 'RELEASE' WHERE app_cd = :app AND version_no_cd = :version AND status_cd = 'SNAPSHOT' AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = 'HEAD'")

        // Step 3: Create new SNAPSHOT cubes from the HEAD RELEASE cubes (next version higher, started for development)
        ApplicationID releaseId = appId.asRelease()

        Map<String, Object> options = [(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY): true,
                                       (NCubeManager.SEARCH_INCLUDE_TEST_DATA): true,
                                       (NCubeManager.SEARCH_INCLUDE_CUBE_DATA): true,
                                       (METHOD_NAME) : 'releaseCubes'] as Map

        boolean autoCommit = c.getAutoCommit()
        PreparedStatement insert = null
        try
        {
            c.setAutoCommit(false)
            int count = 0
            insert = c.prepareStatement(
                    "/* releaseCubes */ INSERT INTO n_cube (n_cube_id, n_cube_nm, cube_value_bin, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            runSelectCubesStatement(c, releaseId, null, options, { ResultSet row ->
                insert.setLong(1, UniqueIdGenerator.getUniqueId())
                insert.setString(2, row.getString('n_cube_nm'))
                insert.setBytes(3, row.getBytes(CUBE_VALUE_BIN))
                insert.setTimestamp(4, new Timestamp(System.currentTimeMillis()))
                insert.setString(5, row.getString('create_hid'))
                insert.setString(6, newSnapVer)
                insert.setString(7, ReleaseStatus.SNAPSHOT.name())
                insert.setString(8, appId.app)
                insert.setBytes(9, row.getBytes(TEST_DATA_BIN))
                insert.setBytes(10, ('SNAPSHOT ' + newSnapVer + ' created').getBytes("UTF-8"))
                insert.setString(11, appId.tenant)
                insert.setString(12, ApplicationID.HEAD)
                insert.setLong(13, 0) // New SNAPSHOT revision numbers start at 0, we don't move forward deleted records.
                insert.addBatch()
                count++
                if (count % EXECUTE_BATCH_CONSTANT == 0)
                {
                    insert.executeBatch()
                }
            })

            if (count % EXECUTE_BATCH_CONSTANT != 0)
            {
                insert.executeBatch()
                c.commit()
            }
            return releaseCount
        }
        finally
        {
            c.setAutoCommit(autoCommit)
            insert?.close()
        }
    }

    int changeVersionValue(Connection c, ApplicationID appId, String newVersion)
    {
        ApplicationID newSnapshot = appId.createNewSnapshotId(newVersion)
        if (doCubesExist(c, newSnapshot, true, 'changeVersionValue'))
        {
            throw new IllegalStateException("Cannot change version value to " + newVersion + " because cubes with this version already exists.  Choose a different version number, app: " + appId)
        }

        Map map = appId as Map
        map.putAll([newVer: newVersion, status: 'SNAPSHOT'])
        Sql sql = new Sql(c)
        int count = sql.executeUpdate(map, "/* changeVersionValue */ UPDATE n_cube SET version_no_cd = :newVer WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch")
        if (count < 1)
        {
            throw new IllegalArgumentException("No SNAPSHOT n-cubes found with version " + appId.version + ", therefore no versions updated, app: " + appId)
        }
        return count
    }

    boolean updateTestData(Connection c, ApplicationID appId, String cubeName, String testData)
    {
        Long maxRev = getMaxRevision(c, appId, cubeName, 'updateTestData')

        if (maxRev == null)
        {
            throw new IllegalArgumentException("Cannot update test data, cube: " + cubeName + " does not exist in app: " + appId)
        }

        Map map = [testData: testData == null ? null : testData.getBytes("UTF-8"), tenant: appId.getTenant(),
                   app: appId.getApp(), ver: appId.getVersion(), status: ReleaseStatus.SNAPSHOT.name(),
                   branch: appId.getBranch(), rev: maxRev, cube: buildName(c, cubeName)]
        Sql sql = new Sql(c)

        int rows = sql.executeUpdate(map, """\
/* updateTestData */ UPDATE n_cube SET test_data_bin=:testData
WHERE app_cd = :app AND """ + buildNameCondition(c, "n_cube_nm") + """ = :cube AND version_no_cd = :ver
 AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch AND revision_number = :rev""")
        return rows == 1
    }

    String getTestData(Connection c, ApplicationID appId, String cubeName)
    {
        Map map = appId as Map
        map.cube = buildName(c, cubeName)
        Sql sql = new Sql(c)
        byte[] testBytes = null
        boolean found = false

        String select = '''\
/* getTestData */ SELECT test_data_bin FROM n_cube
 WHERE ''' + buildNameCondition(c, 'n_cube_nm') + ''' = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch
 ORDER BY abs(revision_number) DESC'''

        sql.eachRow(select, map, 0, 1, { ResultSet row ->
            testBytes = row.getBytes(TEST_DATA_BIN)
            found = true
        })

        if (!found)
        {
            throw new IllegalArgumentException('Could not fetch test data, cube: ' + cubeName + ' does not exist in app: ' + appId)
        }
        return testBytes == null ? '' : new String(testBytes, "UTF-8")
    }

    boolean updateNotes(Connection c, ApplicationID appId, String cubeName, String notes)
    {
        Long maxRev = getMaxRevision(c, appId, cubeName, 'updateNotes')
        if (maxRev == null)
        {
            throw new IllegalArgumentException("Cannot update notes, cube: " + cubeName + " does not exist in app: " + appId)
        }

        Map map = appId as Map
        map.putAll([notes: notes == null ? null : notes.getBytes("UTF-8"), status: ReleaseStatus.SNAPSHOT.name(),
                    rev: maxRev, cube: buildName(c, cubeName)])
        Sql sql = new Sql(c)

        int rows = sql.executeUpdate(map, """\
/* updateNotes */ UPDATE n_cube SET notes_bin = :notes
WHERE app_cd = :app AND """ + buildNameCondition(c, "n_cube_nm") + """ = :cube AND version_no_cd = :version
AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch AND revision_number = :rev""")
        return rows == 1
    }

    List<String> getAppNames(Connection c, String tenant)
    {
        if (StringUtilities.isEmpty(tenant))
        {
            throw new IllegalArgumentException('error calling getAppVersions(), tenant (' + tenant + ') cannot be null or empty')
        }
        Map map = [tenant:tenant]
        Sql sql = new Sql(c)
        List<String> apps = []

        sql.eachRow("/* getAppNames */ SELECT DISTINCT app_cd FROM n_cube WHERE tenant_cd = RPAD(:tenant, 10, ' ')", map, { ResultSet row ->
            if (row.getFetchSize() < FETCH_SIZE)
            {
                row.setFetchSize(FETCH_SIZE)
            }
            apps.add(row.getString('app_cd'))
        })
        return apps
    }

    Map<String, List<String>> getVersions(Connection c, String tenant, String app)
    {
        if (StringUtilities.isEmpty(tenant) ||
            StringUtilities.isEmpty(app))
        {
            throw new IllegalArgumentException('error calling getAppVersions() tenant (' + tenant + ') or app (' + app +') cannot be null or empty')
        }
        Sql sql = new Sql(c)
        Map map = [tenant:tenant, app:app]
        List<String> releaseVersions = []
        List<String> snapshotVersions = []
        Map<String, List<String>> versions = [:]

        sql.eachRow("/* getVersions */ SELECT DISTINCT version_no_cd, status_cd FROM n_cube WHERE app_cd = :app AND tenant_cd = RPAD(:tenant, 10, ' ')", map, { ResultSet row ->
            if (row.getFetchSize() < FETCH_SIZE)
            {
                row.setFetchSize(FETCH_SIZE)
            }

            String version = row.getString('version_no_cd')
            if (ReleaseStatus.RELEASE.name() == row.getString('status_cd'))
            {
                releaseVersions.add(version)
            }
            else
            {
                snapshotVersions.add(version)
            }
        })
        versions[ReleaseStatus.SNAPSHOT.name()] = snapshotVersions
        versions[ReleaseStatus.RELEASE.name()] = releaseVersions
        return versions
    }

    Set<String> getBranches(Connection c, String tenant)
    {
        if (StringUtilities.isEmpty(tenant))
        {
            throw new IllegalArgumentException('error calling getBranches(), tenant must not be null or empty')
        }
        Sql sql = new Sql(c)
        Set<String> branches = new HashSet<>()

        sql.eachRow("/* getBranches.all */ SELECT DISTINCT branch_id FROM n_cube WHERE tenant_cd = RPAD(:tenant, 10, ' ')", [tenant:tenant], { ResultSet row ->
            if (row.getFetchSize() < FETCH_SIZE)
            {
                row.setFetchSize(FETCH_SIZE)
            }
            branches.add(row.getString('branch_id'))
        })
        return branches
    }

    Set<String> getBranches(Connection c, ApplicationID appId)
    {
        Map map = appId as Map
        Sql sql = new Sql(c)
        Set<String> branches = new HashSet<>()

        sql.eachRow("/* getBranches.appVerStat */ SELECT DISTINCT branch_id FROM n_cube WHERE app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ')", map, { ResultSet row ->
            if (row.getFetchSize() < FETCH_SIZE)
            {
                row.setFetchSize(FETCH_SIZE)
            }
            branches.add(row.getString('branch_id'))
        })
        return branches
    }

    /**
     * Check for existence of a cube with this appId.  You can ignoreStatus if you want to check for existence of
     * a SNAPSHOT or RELEASE cube.
     * @param ignoreStatus - If you want to ignore status (check for both SNAPSHOT and RELEASE cubes in existence) pass
     *                     in true.
     * @return true if any cubes exist for the given AppId, false otherwise.
     */
    boolean doCubesExist(Connection c, ApplicationID appId, boolean ignoreStatus, String methodName)
    {
        Map map = appId as Map
        Sql sql = new Sql(c)
        String statement = "/* " + methodName + ".doCubesExist */ SELECT DISTINCT n_cube_id FROM n_cube WHERE app_cd = :app AND version_no_cd = :version AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch"

        if (!ignoreStatus)
        {
            statement += ' AND status_cd = :status'
        }

        boolean result = false
        sql.eachRow(statement, map, 0, 1, { ResultSet row -> result = true })
        return result
    }


    Long getMaxRevision(Connection c, ApplicationID appId, String cubeName, String methodName)
    {
        Map map = appId as Map
        map.cube = buildName(c, cubeName)
        Sql sql = new Sql(c)
        Long rev = null

        String select = '''\
/* ''' + methodName + '''.maxRev */ SELECT revision_number FROM n_cube
 WHERE ''' + buildNameCondition(c, "n_cube_nm") + ''' = :cube AND app_cd = :app AND version_no_cd = :version AND status_cd = :status AND tenant_cd = RPAD(:tenant, 10, ' ') AND branch_id = :branch
 ORDER BY abs(revision_number) DESC'''

        sql.eachRow(select, map, 0, 1, { ResultSet row ->
            rev = row.getLong('revision_number')
        })
        return rev
    }

    protected static void getCubeInfoRecords(ApplicationID appId, Pattern searchPattern, List<NCubeInfoDto> list, ResultSet row)
    {
        if (row.getFetchSize() < FETCH_SIZE)
        {
            row.setFetchSize(FETCH_SIZE)
        }
        boolean contentMatched = false

        if (searchPattern != null)
        {
            byte[] bytes = IOUtilities.uncompressBytes(row.getBytes(CUBE_VALUE_BIN))
            String cubeData = StringUtilities.createUtf8String(bytes)
            Matcher matcher = searchPattern.matcher(cubeData)
            contentMatched = matcher.find()
        }

        if (searchPattern == null || contentMatched)
        {
            NCubeInfoDto dto = new NCubeInfoDto()
            dto.id = row.getString('n_cube_id')
            dto.name = row.getString('n_cube_nm')
            dto.branch = appId.branch
            dto.tenant = appId.tenant
            byte[] notes = null
            try
            {
                notes = row.getBytes(NOTES_BIN)
            }
            catch (Exception ignored) { }
            dto.notes = new String(notes == null ? "".getBytes() : notes, 'UTF-8')
            dto.version = appId.version
            dto.status = row.getString('status_cd')
            dto.app = appId.app
            dto.createDate = new Date(row.getTimestamp('create_dt').getTime())
            dto.createHid = row.getString('create_hid')
            dto.revision = row.getString('revision_number')
            dto.changed = row.getBoolean('changed')
            dto.sha1 = row.getString('sha1')
            dto.headSha1 = row.getString('head_sha1')
            list.add(dto)
        }
    }

    protected static NCube buildCube(ApplicationID appId, ResultSet row)
    {
        NCube ncube = NCube.createCubeFromStream(row.getBinaryStream(CUBE_VALUE_BIN))
        ncube.setSha1(row.getString('sha1'))
        ncube.setApplicationID(appId)
        return ncube
    }


    // ------------------------------------------ local non-JDBC helper methods ----------------------------------------

    protected static String createNote(String user, Date date, String notes)
    {
        return dateTimeFormat.format(date) + ' [' + user + '] ' + notes
    }

    protected static boolean toBoolean(Object value)
    {
        if (value == null)
        {
            return false
        }
        return ((Boolean)value).booleanValue()
    }

    private static String convertPattern(String pattern)
    {
        if (StringUtilities.isEmpty(pattern) || '*'.equals(pattern))
        {
            return null
        }
        else
        {
            pattern = pattern.replace('*', '%')
            pattern = pattern.replace('?', '_')
        }
        return pattern
    }

    static String buildNameCondition(Connection c, String name)
    {
        if (isOracle(c))
        {
            return ('LOWER(' + name + ')')
        }
        return name
    }

    static String buildName(Connection c, String name)
    {
        if (isOracle(c))
        {
            return name == null ? null : name.toLowerCase()
        }
        return name
    }

    static boolean isOracle(Connection c)
    {
        if (c == null)
        {
            return false
        }
        return Regexes.isOraclePattern.matcher(c.getMetaData().getDriverName()).matches()
    }
}
