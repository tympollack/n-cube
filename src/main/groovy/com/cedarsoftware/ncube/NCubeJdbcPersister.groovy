package com.cedarsoftware.ncube
import com.cedarsoftware.util.StringUtilities
import groovy.sql.GroovyResultSet
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
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
class NCubeJdbcPersister extends NCubeJdbcPersisterJava
{
    private static final Logger LOG = LogManager.getLogger(NCubeJdbcPersisterJava.class)
    private static final long EXECUTE_BATCH_CONSTANT = 35
    static final String CUBE_VALUE_BIN = "cube_value_bin"
    static final String TEST_DATA_BIN = "test_data_bin"
    static final String NOTES_BIN = "notes_bin"
    static final String HEAD_SHA_1 = "head_sha1"

    NCubeInfoDto commitMergedCubeToBranch(Connection c, ApplicationID appId, NCube cube, String headSha1, String username)
    {
        Map options = [(NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                       (NCubeManager.SEARCH_EXACT_MATCH_NAME):true]

        NCubeInfoDto result = null

        runSelectCubesStatement(c, appId, cube.name, options, { GroovyResultSet row ->
            if (result != null)
            {
                throw new IllegalStateException('Should only match one record, (commit merged cube to branch), app: ' + appId + ", cube: " + cube.name)
            }
            Long revision = row['revision_number'] as Long
            byte[] testData = row['test_data_bin'] as byte[]
            long now = System.currentTimeMillis()
            revision = revision < 0 ? revision-1 : revision+1;
            result = insertCube(c, appId, cube, revision, testData, "Cube committed", true, headSha1, now, username)
        })
        return result
    }

    NCubeInfoDto commitMergedCubeToHead(Connection c, ApplicationID appId, NCube cube, String username)
    {
        Map options = [(NCubeManager.SEARCH_INCLUDE_TEST_DATA):true,
                       (NCubeManager.SEARCH_EXACT_MATCH_NAME):true]

        ApplicationID headAppId = appId.asHead()
        NCubeInfoDto result = null

        runSelectCubesStatement(c, appId, cube.name, options, { GroovyResultSet row ->
            if (result != null)
            {
                throw new IllegalStateException('Should only match one record, (commit merged cube to HEAD), app: ' + appId + ", cube: " + cube.name)
            }
            Long revision = row['revision_number'] as Long

            // get current max HEAD revision
            Long maxRevision = getMaxRevision(c, headAppId, cube.getName())

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
                maxRevision = Math.abs(maxRevision as long) + 1;
            }

            byte[] testData = row['test_data_bin'] as byte[]
            long now = System.currentTimeMillis()
            // ok to use this here, because we're writing out these bytes twice (once to head and once to branch)
            byte[] cubeData = cube.getCubeAsGzipJsonBytes()
            String sha1 = cube.sha1()

            NCubeInfoDto head = insertCube(c, headAppId, cube.getName(), maxRevision, cubeData, testData, "Cube committed", false, sha1, null, now, username)

            if (head == null)
            {
                String s = "Unable to commit cube: " + cube.getName() + " to app:  " + appId;
                throw new IllegalStateException(s)
            }

            result = insertCube(c, appId, cube.getName(), revision > 0 ? ++revision : --revision, cubeData, testData, "Cube committed", false, sha1, sha1, now, username)
        })
        return result
    }

    NCubeInfoDto commitCube(Connection c, ApplicationID appId, Long cubeId, String username)
    {
        if (cubeId == null)
        {
            throw new IllegalArgumentException("Commit cube, cube id cannot be empty, app: " + appId)
        }

        ApplicationID headAppId = appId.asHead()
        def map = [id:cubeId]
        Sql sql = new Sql(c)
        NCubeInfoDto result = null

        sql.eachRow("SELECT n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, sha1, head_sha1 from n_cube WHERE n_cube_id = $map.id",
        { GroovyResultSet row ->
            if (result != null)
            {
                throw new IllegalStateException('Should only match one record, (commit), app: ' + appId + ', cube id: ' + cubeId)
            }
            byte[] jsonBytes = row.getBytes(CUBE_VALUE_BIN)
            String sha1 = row.getString("sha1")
            String cubeName = row.getString("n_cube_nm")
            Long revision = row.getLong("revision_number")
            Long maxRevision = getMaxRevision(c, headAppId, cubeName)

            //  create case because maxrevision was not found.
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
                maxRevision = Math.abs(maxRevision as long) + 1;
            }

            byte[] testData = row.getBytes(TEST_DATA_BIN)

            long now = System.currentTimeMillis()

            NCubeInfoDto dto = insertCube(c, headAppId, cubeName, maxRevision, jsonBytes, testData, "Cube committed", false, sha1, null, now, username)

            if (dto == null)
            {
                String s = "Unable to commit cube: " + cubeName + " to app:  " + headAppId;
                throw new IllegalStateException(s)
            }

            PreparedStatement update = null
            try
            {
                update = updateBranchToHead(c, cubeId, sha1, now)
                if (update.executeUpdate() != 1)
                {
                    throw new IllegalStateException("error updating n-cube: " + cubeName + "', app: " + headAppId + ", row was not updated")
                }
            }
            finally
            {
                if (update)
                {
                    update.close()
                }
            }

            dto.changed = false;
            dto.id = Long.toString(cubeId)
            dto.sha1 = sha1;
            dto.headSha1 = sha1;
            result = dto;
        })

        return result
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
    protected void runSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map options, Closure closure)
    {
        boolean changedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_CHANGED_RECORDS_ONLY], false)
        boolean activeRecordsOnly = toBoolean(options[NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY], false)
        boolean deletedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_DELETED_RECORDS_ONLY], false)
        boolean includeCubeData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA], false)
        boolean includeTestData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_TEST_DATA], false)
        boolean exactMatchName = toBoolean(options[NCubeManager.SEARCH_EXACT_MATCH_NAME], false)

        if (activeRecordsOnly && deletedRecordsOnly)
        {
            throw new IllegalArgumentException("activeRecordsOnly and deletedRecordsOnly are mutually exclusive options and cannot both be 'true'.")
        }

        //  convert pattern will return null even if full pattern = '*', saying we don't need to do the like statement.
        //  That is why it is before the hasContentCheck.

        namePattern = convertPattern(buildName(c, namePattern))
        boolean hasNamePattern = StringUtilities.hasContent(namePattern)

        String nameCondition1 = ''
        String nameCondition2 = ''

        def map = [app:appId.app, ver:appId.version, status:appId.status, tenant:appId.tenant, branch:appId.branch, name:namePattern, changed:changedRecordsOnly]
        if (hasNamePattern)
        {
            nameCondition1 = ' AND ' + buildNameCondition(c, 'n_cube_nm') + (exactMatchName ? " = '$map.name'" : " LIKE '$map.name")
            nameCondition2 = ' AND ' + buildNameCondition(c, 'm.n_cube_nm') + (exactMatchName ? " = '$map.name'" : " LIKE '$map.name")
        }

        String revisionCondition = activeRecordsOnly ? ' AND n.revision_number >= 0' : deletedRecordsOnly ? ' AND n.revision_number < 0' : ''
        String changedCondition = changedRecordsOnly ? ' AND n.changed = $map.changed' : ''
        String testCondition = includeTestData ? ', n.test_data_bin' : ''
        String cubeCondition = includeCubeData ? ', n.cube_value_bin' : ''

        Sql sql = new Sql(c)
        sql.eachRow("SELECT n_cube_id, n.n_cube_nm, app_cd, n.notes_bin, version_no_cd, status_cd, n.create_dt, n.create_hid, n.revision_number, n.branch_id, n.changed, n.sha1, n.head_sha1" +
                testCondition +
                cubeCondition +
                " FROM n_cube n, " +
                "( " +
                "  SELECT n_cube_nm, max(abs(revision_number)) AS max_rev " +
                "  FROM n_cube " +
                "  WHERE app_cd = '$map.app' AND version_no_cd = '$map.ver' AND status_cd = '$map.status' AND tenant_cd = RPAD('$map.tenant', 10, ' ') AND branch_id = '$map.branch'" +
                nameCondition1 +
                " GROUP BY n_cube_nm " +
                ") m " +
                "WHERE m.n_cube_nm = n.n_cube_nm AND m.max_rev = abs(n.revision_number) AND n.app_cd = '$map.app' AND n.version_no_cd = '$map.ver' AND n.status_cd = '$map.status' AND n.tenant_cd = RPAD('$map.tenant', 10, ' ') AND n.branch_id = '$map.branch'" +
                revisionCondition +
                changedCondition +
                nameCondition2, closure)
        sql.close()
    }

    /**
     * @param c
     * @param appId
     * @param namePattern
     * @param options map with possible keys:
     *                changedRecordsOnly - default false
     *                activeRecordsOnly - default false
     *                deletedRecordsOnly - default false
     *                includeCubeData - default false
     *                includeTestData - default false
     *                exactMatchName - default false
     * @return
     * @throws java.sql.SQLException
     */
    PreparedStatement createSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map<String, Object> options) throws SQLException
    {
        boolean changedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_CHANGED_RECORDS_ONLY], false)
        boolean activeRecordsOnly = toBoolean(options[NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY], false)
        boolean deletedRecordsOnly = toBoolean(options[NCubeManager.SEARCH_DELETED_RECORDS_ONLY], false)
        boolean includeCubeData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_CUBE_DATA], false)
        boolean includeTestData = toBoolean(options[NCubeManager.SEARCH_INCLUDE_TEST_DATA], false)
        boolean exactMatchName = toBoolean(options[NCubeManager.SEARCH_EXACT_MATCH_NAME], false)

        if (activeRecordsOnly && deletedRecordsOnly)
        {
            throw new IllegalArgumentException("activeRecordsOnly and deletedRecordsOnly are mutually exclusive options and cannot both be 'true'.")
        }

        //  convert pattern will return null even if full pattern = '*', saying we don't need to do the like statement.
        //  That is why it is before the hasContentCheck.

        namePattern = convertPattern(buildName(c, namePattern))
        boolean hasNamePattern = StringUtilities.hasContent(namePattern)

        String nameCondition1 = ''
        String nameCondition2 = ''

        if (hasNamePattern)
        {
            nameCondition1 = " AND " + buildNameCondition(c, "n_cube_nm") + (exactMatchName ? " = ?" : " LIKE ?")
            nameCondition2 = " AND " + buildNameCondition(c, "m.n_cube_nm") + (exactMatchName ? " = ?" : " LIKE ?")
        }

        String revisionCondition = activeRecordsOnly ? " AND n.revision_number >= 0" : deletedRecordsOnly ? " AND n.revision_number < 0" : ""
        String changedCondition = changedRecordsOnly ? " AND n.changed = ?" : ""
        String testCondition = includeTestData ? ", n.test_data_bin" : ""
        String cubeCondition = includeCubeData ? ", n.cube_value_bin" : ""

        String sql = "SELECT n_cube_id, n.n_cube_nm, app_cd, n.notes_bin, version_no_cd, status_cd, n.create_dt, n.create_hid, n.revision_number, n.branch_id, n.changed, n.sha1, n.head_sha1" +
                testCondition +
                cubeCondition +
                " FROM n_cube n, " +
                "( " +
                "  SELECT n_cube_nm, max(abs(revision_number)) AS max_rev " +
                "  FROM n_cube " +
                "  WHERE app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?" +
                nameCondition1 +
                " GROUP BY n_cube_nm " +
                ") m " +
                "WHERE m.n_cube_nm = n.n_cube_nm AND m.max_rev = abs(n.revision_number) AND n.app_cd = ? AND n.version_no_cd = ? AND n.status_cd = ? AND n.tenant_cd = RPAD(?, 10, ' ') AND n.branch_id = ?" +
                revisionCondition +
                changedCondition +
                nameCondition2;

        PreparedStatement stmt = c.prepareStatement(sql)
        stmt.setString(1, appId.getApp())
        stmt.setString(2, appId.getVersion())
        stmt.setString(3, appId.getStatus())
        stmt.setString(4, appId.getTenant())
        stmt.setString(5, appId.getBranch())

        int i=6;
        if (hasNamePattern)
        {
            stmt.setString(i++, namePattern)
        }

        stmt.setString(i++, appId.getApp())
        stmt.setString(i++, appId.getVersion())
        stmt.setString(i++, appId.getStatus())
        stmt.setString(i++, appId.getTenant())
        stmt.setString(i++, appId.getBranch())

        if (changedRecordsOnly)
        {
            stmt.setBoolean(i++, changedRecordsOnly)
        }

        if (hasNamePattern)
        {
            stmt.setString(i++, namePattern)
        }

        return stmt;
    }

    Long getMaxRevision(Connection c, ApplicationID appId, String cubeName)
    {
        return getMinMaxRevision(c, appId, cubeName, "DESC")
    }

    Long getMinRevision(Connection c, ApplicationID appId, String cubeName)
    {
        return getMinMaxRevision(c, appId, cubeName, "ASC")
    }

    Long getMinMaxRevision(Connection c, ApplicationID appId, String cubeName, String order)
    {
        PreparedStatement stmt = null
        try 
        {
            stmt = c.prepareStatement(
                    "SELECT revision_number FROM n_cube " +
                            "WHERE " + buildNameCondition(c, "n_cube_nm") + " = ? AND app_cd = ? AND status_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ? " +
                            "ORDER BY abs(revision_number) " + order)

            stmt.setString(1, buildName(c, cubeName))
            stmt.setString(2, appId.getApp())
            stmt.setString(3, appId.getStatus())
            stmt.setString(4, appId.getVersion())
            stmt.setString(5, appId.getTenant())
            stmt.setString(6, appId.getBranch())

            ResultSet rs = stmt.executeQuery()
            return rs.next() ? rs.getLong(1) : null;
        }
        catch (Exception e)
        {
            String revType = "ASC".equalsIgnoreCase(order) ? "minimum" : "maximum";
            String s = "Unable to get " + revType + " revision number for cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e)
            throw new RuntimeException(s, e)
        }
        finally
        {
            if (stmt != null)
            {
                stmt.close()
            }
        }
    }

    protected boolean toBoolean(Object value, boolean defVal)
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
            return null;
        }
        else
        {
            pattern = pattern.replace('*', '%')
            pattern = pattern.replace('?', '_')
        }
        return pattern;
    }

}
