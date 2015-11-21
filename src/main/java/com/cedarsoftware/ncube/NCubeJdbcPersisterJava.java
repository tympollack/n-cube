package com.cedarsoftware.ncube;

import com.cedarsoftware.ncube.formatters.JsonFormatter;
import com.cedarsoftware.util.IOUtilities;
import com.cedarsoftware.util.StringUtilities;
import com.cedarsoftware.util.UniqueIdGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static java.lang.StrictMath.abs;

/**
 * SQL Persister for n-cubes.  Manages all reads and writes of n-cubes to an SQL database.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br>
 *         Copyright (c) Cedar Software LLC
 *         <br><br>
 *         Licensed under the Apache License, Version 2.0 (the "License");
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
public abstract class NCubeJdbcPersisterJava
{
    private static final Logger LOG = LogManager.getLogger(NCubeJdbcPersisterJava.class);
    private static final long EXECUTE_BATCH_CONSTANT = 35;
    public static final String CUBE_VALUE_BIN = "cube_value_bin";
    public static final String TEST_DATA_BIN = "test_data_bin";
    public static final String NOTES_BIN = "notes_bin";
    public static final String HEAD_SHA_1 = "head_sha1";

    protected PreparedStatement updateBranchToHead(Connection c, Long cubeId, String sha1, long now) throws SQLException
    {
        PreparedStatement update = c.prepareStatement("UPDATE n_cube set head_sha1 = ?, changed = ?, create_dt = ? WHERE n_cube_id = ?");
        update.setString(1, sha1);
        update.setInt(2, 0);
        update.setTimestamp(3, new Timestamp(now));
        update.setLong(4, cubeId);
        return update;
    }

    public boolean updateBranchCubeHeadSha1(Connection c, Long cubeId, String headSha1)
    {
        if (cubeId == null)
        {
            throw new IllegalArgumentException("Update cube's HEAD SHA-1, cube id cannot be empty");
        }

        if (StringUtilities.isEmpty(headSha1))
        {
            throw new IllegalArgumentException("Update cube's HEAD SHA-1, SHA-1 cannot be empty");
        }

        // select head cube in question
        String sql = "UPDATE n_cube set head_sha1 = ?, changed = ? WHERE n_cube_id = ?";

        try (PreparedStatement stmt = c.prepareStatement(sql))
        {
            stmt.setString(1, headSha1);
            stmt.setBoolean(2, false);
            stmt.setLong(3, cubeId);
            long count = stmt.executeUpdate();
            if (count == 0)
            {
                throw new IllegalStateException("error updating cube: " + cubeId + ", to HEAD SHA-1: " + headSha1 + ", no record found.");
            }
            else if (count != 1)
            {
                throw new IllegalStateException("error updating cube: " + cubeId + ", to HEAD SHA-1: " + headSha1 + ", more than one record found: " + count);
            }
            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to update cube: " + cubeId + " to have HEAD SHA-1: " + headSha1;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }
    }

    NCubeInfoDto updateCube(Connection c, ApplicationID appId, Long cubeId, String username)
    {
        if (cubeId == null)
        {
            throw new IllegalArgumentException("Update cube, Cube id cannot be empty, app: " + appId);
        }

        // select head cube in question
        String sql = "SELECT n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, sha1, head_sha1, create_dt from n_cube WHERE n_cube_id = ?";

        try (PreparedStatement stmt = c.prepareStatement(sql))
        {
            stmt.setLong(1, cubeId);

            try (ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    byte[] jsonBytes = rs.getBytes(CUBE_VALUE_BIN);
                    String sha1 = rs.getString("sha1");
                    String cubeName = rs.getString("n_cube_nm");
                    Long revision = rs.getLong("revision_number");
                    long time = rs.getTimestamp("create_dt").getTime();
                    byte[] testData = rs.getBytes(TEST_DATA_BIN);

                    Long maxRevision = getMaxRevision(c, appId, cubeName);

                    //  create case because max revision was not found.
                    if (maxRevision == null)
                    {
                        maxRevision = revision < 0 ? new Long(-1) : new Long(0);
                    }
                    else if (revision < 0)
                    {
                        // cube deleted in branch
                        maxRevision = -(Math.abs(maxRevision)+1);
                    }
                    else
                    {
                        maxRevision = Math.abs(maxRevision)+1;
                    }

                    NCubeInfoDto dto = insertCube(c, appId, cubeName, maxRevision, jsonBytes, testData, "Cube updated from HEAD", false, sha1, sha1, time, username);

                    if (dto == null)
                    {
                        String s = "Unable to update cube: " + cubeName + " to app:  " + appId;
                        throw new IllegalStateException(s);
                    }

                    return dto;
                }

                return null;
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to update cube: " + cubeId + " to app:  " + appId;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }
    }

    public void updateCube(Connection connection, ApplicationID appId, NCube cube, String username)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
        options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
        options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

        try (PreparedStatement stmt = createSelectCubesStatement(connection, appId, cube.getName(), options);
             ResultSet rs = stmt.executeQuery())
        {
            if (rs.next())
            {
                Long revision = rs.getLong("revision_number");

                byte[] testData = rs.getBytes(TEST_DATA_BIN);

                if (revision < 0)
                {
                    testData = null;
                }

                String headSha1 = rs.getString("head_sha1");
                String oldSha1 = rs.getString("sha1");


                if (StringUtilities.equals(oldSha1, cube.sha1()) && revision >= 0)
                {
                    //  shas are equals and both revision values are positive.  no need for new revision of record.
                    return;
                }

                NCubeInfoDto dto = insertCube(connection, appId, cube, abs(revision) + 1, testData, "Cube updated", true, headSha1, System.currentTimeMillis(), username);

                if (dto == null)
                {
                    throw new IllegalStateException("error updating n-cube: " + cube.getName() + "', app: " + appId + ", row was not updated");
                }
            }
            else if (insertCube(connection, appId, cube, 0L, null, "Cube created", true, null, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("error inserting new n-cube: " + cube.getName() + "', app: " + appId);
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to insert cube: " + cube.getName() + ", app: " + appId;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }
    }

    abstract Long getMaxRevision(Connection c, ApplicationID appId, String cubeName);

    abstract Long getMinRevision(Connection c, ApplicationID appId, String cubeName);

    List<NCubeInfoDto> search(Connection c, ApplicationID appId, String cubeNamePattern, String searchPattern, Map<String, Object> options)
    {
        boolean hasSearchPattern = StringUtilities.hasContent(searchPattern);

        if (hasSearchPattern)
        {
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
        }

        try (PreparedStatement statement = createSelectCubesStatement(c, appId, cubeNamePattern, options))
        {
            return getCubeInfoRecords(appId, statement, searchPattern);
        }
        catch (Exception e)
        {
            String s = "Unable to fetch cubes matching name '" + cubeNamePattern + "' with content '" + searchPattern + "' for app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    PreparedStatement createSelectSingleCubeStatement(Connection c, long id) throws SQLException
    {
        String sql = "SELECT n_cube_nm, tenant_cd, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, changed, sha1, head_sha1 FROM n_cube where n_cube_id = ?";
        PreparedStatement s = c.prepareStatement(sql);
        s.setLong(1, id);
        return s;
    }

    public PreparedStatement createSelectCubeWithMatchingSha1Statement(Connection c, ApplicationID appId, String cubeName, String sha1) throws SQLException
    {
        String sql = "SELECT n_cube_id, n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, changed, sha1, head_sha1, create_dt " +
                "FROM n_cube " +
                "WHERE " + buildNameCondition(c, "n_cube_nm") + " = ? AND app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ? AND sha1 = ? " +
                "ORDER BY abs(revision_number) desc";

        PreparedStatement s = c.prepareStatement(sql);
        s.setString(1, buildName(c, cubeName));
        s.setString(2, appId.getApp());
        s.setString(3, appId.getVersion());
        s.setString(4, appId.getStatus());
        s.setString(5, appId.getTenant());
        s.setString(6, appId.getBranch());
        s.setString(7, sha1);
        s.setMaxRows(1);
        return s;
    }

    public PreparedStatement createSelectCubeWithMatchingRevisionStatement(Connection c, ApplicationID appId, String cubeName, long revision) throws SQLException
    {
        String sql = "SELECT n_cube_id, n_cube_nm, app_cd, version_no_cd, status_cd, revision_number, branch_id, cube_value_bin, test_data_bin, notes_bin, changed, sha1, head_sha1, create_dt " +
                "FROM n_cube " +
                "WHERE " + buildNameCondition(c, "n_cube_nm") + " = ? AND app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ? AND revision_number = ?";

        PreparedStatement s = c.prepareStatement(sql);
        s.setString(1, buildName(c, cubeName));
        s.setString(2, appId.getApp());
        s.setString(3, appId.getVersion());
        s.setString(4, appId.getStatus());
        s.setString(5, appId.getTenant());
        s.setString(6, appId.getBranch());
        s.setLong(7, revision);
        s.setMaxRows(1);
        return s;
    }

    abstract boolean toBoolean(Object value, boolean def);

    /**
     *
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
     * @throws SQLException
     */
    abstract PreparedStatement createSelectCubesStatement(Connection c, ApplicationID appId, String namePattern, Map<String, Object> options) throws SQLException;

    public List<NCubeInfoDto> getChangedRecords(Connection c, ApplicationID appId)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(NCubeManager.SEARCH_CHANGED_RECORDS_ONLY, true);
        return search(c, appId, null, null, options);
    }

    public List<NCubeInfoDto> getDeletedCubeRecords(Connection c, ApplicationID appId, String pattern)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(NCubeManager.SEARCH_DELETED_RECORDS_ONLY, true);
        return search(c, appId, pattern, null, options);
    }

    public List<NCubeInfoDto> getRevisions(Connection c, ApplicationID appId, String cubeName)
    {
        try (PreparedStatement stmt = c.prepareStatement(
                "SELECT n_cube_id, n_cube_nm, notes_bin, version_no_cd, status_cd, app_cd, create_dt, create_hid, revision_number, branch_id, cube_value_bin, sha1, head_sha1, changed " +
                        "FROM n_cube " +
                        "WHERE " + buildNameCondition(c, "n_cube_nm") + " = ? AND app_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND status_cd = ? AND branch_id = ?" +
                        "ORDER BY abs(revision_number) DESC"))
        {
            stmt.setString(1, buildName(c, cubeName));
            stmt.setString(2, appId.getApp());
            stmt.setString(3, appId.getVersion());
            stmt.setString(4, appId.getTenant());
            stmt.setString(5, appId.getStatus());
            stmt.setString(6, appId.getBranch());

            List<NCubeInfoDto> records = getCubeInfoRecords(appId, stmt, null);
            if (records.isEmpty())
            {
                throw new IllegalArgumentException("Cannot fetch revision history for cube:  " + cubeName + " as it does not exist in app:  " + appId);
            }
            return records;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to get revision history for cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    private List<NCubeInfoDto> getCubeInfoRecords(ApplicationID appId, PreparedStatement stmt, String subSearchPattern) throws Exception
    {
        List<NCubeInfoDto> list = new ArrayList<>();

        Pattern searchPattern = null;

        if (StringUtilities.hasContent(subSearchPattern)) {
            searchPattern = Pattern.compile(convertPattern(subSearchPattern), Pattern.CASE_INSENSITIVE);
        }

        try (ResultSet rs = stmt.executeQuery())
        {
            while (rs.next())
            {
                boolean contentMatched = false;

                if (searchPattern != null)
                {
                    byte[] bytes = IOUtilities.uncompressBytes(rs.getBytes("cube_value_bin"));
                    String cubeData = StringUtilities.createUtf8String(bytes);
                    Matcher matcher = searchPattern.matcher(cubeData);
                    contentMatched = matcher.find();
                }

                if (searchPattern == null || contentMatched)
                {
                    NCubeInfoDto dto = new NCubeInfoDto();
                    dto.id = Long.toString(rs.getLong("n_cube_id"));
                    dto.name = rs.getString("n_cube_nm");
                    dto.branch = appId.getBranch();
                    dto.tenant = appId.getTenant();
                    byte[] notes = rs.getBytes(NOTES_BIN);
                    dto.notes = new String(notes == null ? "".getBytes() : notes, "UTF-8");
                    dto.version = appId.getVersion();
                    dto.status = rs.getString("status_cd");
                    dto.app = appId.getApp();
                    dto.createDate = new Date(rs.getTimestamp("create_dt").getTime());
                    dto.createHid = rs.getString("create_hid");
                    dto.revision = Long.toString(rs.getLong("revision_number"));
                    dto.changed = rs.getBoolean("changed");
                    dto.sha1 = rs.getString("sha1");
                    dto.headSha1 = rs.getString("head_sha1");

                    list.add(dto);
                }
            }
        }
        return list;
    }

    public NCube loadCubeById(Connection c, long cubeId)
    {
        try (PreparedStatement stmt = createSelectSingleCubeStatement(c, cubeId);
             ResultSet rs = stmt.executeQuery())
        {
            if (rs.next())
            {
                String tenant = rs.getString("tenant_cd");
                String status = rs.getString("status_cd");
                String app = rs.getString("app_cd");
                String version = rs.getString("version_no_cd");
                String branch = rs.getString("branch_id");
                ApplicationID appId = new ApplicationID(tenant.trim(), app, version, status, branch);
                return buildCube(appId, rs);
            }
        }
        catch (Exception e)
        {
            String s = "Unable to load cube with id: " + cubeId;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }

        throw new IllegalArgumentException("Unable to find cube with id: " + cubeId);
    }

    public NCube loadCube(Connection c, ApplicationID appId, String cubeName)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY, true);
        options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
        options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
        options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

        try (PreparedStatement stmt = createSelectCubesStatement(c, appId, cubeName, options);
             ResultSet rs = stmt.executeQuery())
        {
            if (rs.next())
            {
                return buildCube(appId, rs);
            }
        }
        catch (Exception e)
        {
            String s = "Unable to load cube: " + appId + ", " + cubeName;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }
        return null;
    }

    public NCube loadCubeBySha1(Connection c, ApplicationID appId, String cubeName, String sha1)
    {
        try (PreparedStatement stmt = createSelectCubeWithMatchingSha1Statement(c, appId, cubeName, sha1);
             ResultSet rs = stmt.executeQuery())
        {
            if (rs.next())
            {
                return buildCube(appId, rs);
            }
        }
        catch (Exception e)
        {
            String s = "Unable to load cube: " + appId + ", " + cubeName + " with sha1: " + sha1;
            LOG.error(s, e);
            throw new IllegalStateException(s, e);
        }
        return null;
    }

    private NCube buildCube(ApplicationID appId, ResultSet rs) throws SQLException, UnsupportedEncodingException
    {
        NCube ncube = NCube.createCubeFromStream(rs.getBinaryStream(CUBE_VALUE_BIN));
        ncube.setSha1(rs.getString("sha1"));
        ncube.setApplicationID(appId);
        return ncube;
    }

    public void restoreCube(Connection c, ApplicationID appId, String cubeName, String username)
    {
        Map<String, Object> options = new HashMap<>();
        options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
        options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
        options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

        try (PreparedStatement stmt = createSelectCubesStatement(c, appId, cubeName, options);
             ResultSet rs = stmt.executeQuery())
        {
            if (rs.next())
            {
                Long revision = rs.getLong("revision_number");

                if (revision >= 0)
                {
                    throw new IllegalArgumentException("Cube: " + cubeName + " is already restored in app: " + appId);
                }

                byte[] jsonBytes = rs.getBytes(CUBE_VALUE_BIN);
                byte[] testData = rs.getBytes(TEST_DATA_BIN);
                String notes = "Cube restored";
                String sha1 = rs.getString("sha1");
                String headSha1 = rs.getString("head_sha1");

                if (insertCube(c, appId, cubeName, Math.abs(revision) + 1, jsonBytes, testData, notes, true, sha1, headSha1, System.currentTimeMillis(), username) == null)
                {
                    throw new IllegalStateException("Could not restore n-cube: " + cubeName + "', app: " + appId);
                }
            }
            else
            {
                throw new IllegalArgumentException("Cannot restore cube: " + cubeName + " as it does not exist in app: " + appId);
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (SQLException e)
        {
            String s = "Unable to restore cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean deleteBranch(Connection c, ApplicationID appId)
    {
        String sql = "DELETE FROM n_cube WHERE app_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?";

        try (PreparedStatement ps = c.prepareStatement(sql))
        {
            ps.setString(1, appId.getApp());
            ps.setString(2, appId.getVersion());
            ps.setString(3, appId.getTenant());
            ps.setString(4, appId.getBranch());
            return ps.executeUpdate() > 0;
        }
        catch (Exception e)
        {
            String s = "Unable to delete branch: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    /**
     * Rollback branch cube to initial state when it was created from the HEAD.  This is different
     * than what the HEAD cube is currently.  This is also different than deleting the branch cube's
     * history.  We are essentially copying the initial revision to the end (max revision).  This
     * approach maintains revision history in the branch (and adheres to the no SQL DELETE rule on
     * n_cube table).
     */
    public boolean rollbackCube(Connection c, ApplicationID appId, String cubeName, String username)
    {
        Long revision = getMinRevision(c, appId, cubeName);

        if (revision == null)
        {
            throw new IllegalArgumentException("Could not rollback cube.  Cube was not found.  App:  " + appId + ", cube: " + cubeName);
        }

        try
        {
            byte[] bytes = null;
            byte[] testData = null;
            String sha1 = null;
            String headSha1 = null;

            try (PreparedStatement stmt = createSelectCubeWithMatchingRevisionStatement(c, appId, cubeName, revision);
                 ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    bytes = rs.getBytes("cube_value_bin");
                    testData = rs.getBytes("test_data_bin");
                    sha1 = rs.getString("sha1");
                    headSha1 = rs.getString("head_sha1");
                }
            }

            Long newRevision = getMaxRevision(c, appId, cubeName);
            if (newRevision == null)
            {
                throw new IllegalStateException("failed to rollback because branch cube does not exist: " + cubeName + "', app: " + appId);
            }

            String notes = "Rollback of cube: " + cubeName + ", app: " + appId + ", was successful";
            long rev = Math.abs(newRevision) + 1;

            if (insertCube(c, appId, cubeName, (revision < 0 || headSha1 == null) ?  -rev : rev, bytes, testData, notes, false, sha1, headSha1, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to rollback branch cube: '" + cubeName + "', app: " + appId);
            }

            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to rollback cube: " + cubeName + ", app: " + appId + " due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean deleteCube(Connection c, ApplicationID appId, String cubeName, boolean allowDelete, String username)
    {
        if (allowDelete)
        {
            String sql = "DELETE FROM n_cube WHERE app_cd = ? AND " + buildNameCondition(c, "n_cube_nm") + " = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?";

            try (PreparedStatement ps = c.prepareStatement(sql))
            {
                ps.setString(1, appId.getApp());
                ps.setString(2, buildName(c, cubeName));
                ps.setString(3, appId.getVersion());
                ps.setString(4, appId.getTenant());
                ps.setString(5, appId.getBranch());
                return ps.executeUpdate() > 0;
            }
            catch (Exception e)
            {
                String s = "Unable to delete cube: " + cubeName + ", app: " + appId;
                LOG.error(s, e);
                throw new RuntimeException(s, e);
            }
        }
        else
        {
            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY, true);
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
            options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, appId, cubeName, options);
                 ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    Long revision = rs.getLong("revision_number");
                    byte[] jsonBytes = rs.getBytes(CUBE_VALUE_BIN);
                    byte[] testData = rs.getBytes(TEST_DATA_BIN);
                    String sha1 = rs.getString("sha1");
                    String headSha1 = rs.getString("head_sha1");

                    if (insertCube(c, appId, cubeName, -(revision + 1), jsonBytes, testData, "Cube deleted", true, sha1, headSha1, System.currentTimeMillis(), username) == null)
                    {
                        throw new IllegalStateException("Cannot delete n-cube: " + cubeName + "', app: " + appId + ", row was not deleted");
                    }
                    return true;
                }
                return false;
            }
            catch (RuntimeException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                String s = "Unable to delete cube: " + cubeName + ", app: " + appId;
                LOG.error(s, e);
                throw new RuntimeException(s, e);
            }
        }
    }

    public boolean updateNotes(Connection c, ApplicationID appId, String cubeName, String notes)
    {
        Long maxRev = getMaxRevision(c, appId, cubeName);
        if (maxRev == null)
        {
            throw new IllegalArgumentException("Cannot update notes, cube: " + cubeName + " does not exist in app: " + appId);
        }
        if (maxRev < 0)
        {
            throw new IllegalArgumentException("Cannot update notes, cube: " + cubeName + " is deleted in app: " + appId);
        }

        String statement = "UPDATE n_cube SET notes_bin = ? WHERE app_cd = ? AND " + buildNameCondition(c, "n_cube_nm") + " = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ? AND revision_number = ?";

        try (PreparedStatement stmt = c.prepareStatement(statement))
        {
            stmt.setBytes(1, notes == null ? null : notes.getBytes("UTF-8"));
            stmt.setString(2, appId.getApp());
            stmt.setString(3, buildName(c, cubeName));
            stmt.setString(4, appId.getVersion());
            stmt.setString(5, appId.getTenant());
            stmt.setString(6, appId.getBranch());
            stmt.setLong(7, maxRev);
            return stmt.executeUpdate() == 1;
        }
        catch (Exception e)
        {
            String s = "Unable to update notes for cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public String getNotes(Connection c, ApplicationID appId, String cubeName)
    {
        try (PreparedStatement stmt = c.prepareStatement(
                "SELECT notes_bin FROM n_cube " +
                        "WHERE app_cd = ? AND " + buildNameCondition(c, "n_cube_nm") + " = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?" +
                        "ORDER BY abs(revision_number) DESC"
        ))
        {
            stmt.setString(1, appId.getApp());
            stmt.setString(2, buildName(c, cubeName));
            stmt.setString(3, appId.getVersion());
            stmt.setString(4, appId.getTenant());
            stmt.setString(5, appId.getBranch());
            try (ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    byte[] notes = rs.getBytes("notes_bin");
                    return new String(notes == null ? "".getBytes() : notes, "UTF-8");
                }
            }
            throw new IllegalArgumentException("Could not fetch notes, no cube: " + cubeName + " in app: " + appId);
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to fetch notes for cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public int createBranch(Connection c, ApplicationID appId)
    {
        if (doCubesExist(c, appId, true))
        {
            throw new IllegalStateException("Branch already exists, app: " + appId);
        }

        try
        {
            ApplicationID headId = appId.asHead();

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, headId, null, options);
                 ResultSet rs = stmt.executeQuery();
                 PreparedStatement insert = c.prepareStatement(
                         "INSERT INTO n_cube (n_cube_id, n_cube_nm, cube_value_bin, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number, changed, sha1, head_sha1) " +
                                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
            {
                int count = 0;
                while (rs.next())
                {
                    byte[] jsonBytes = rs.getBytes("cube_value_bin");
                    String sha1 = rs.getString("sha1");
                    //  Old cube types with no sha1 saved with them.
                    if (sha1 == null)
                    {

                        Blob blob = c.createBlob();
                        try (OutputStream out = blob.setBinaryStream(1L)) {
                            try (PreparedStatement update = c.prepareStatement(
                                    "UPDATE n_cube set sha1 = ?, cube_value_bin = ? where n_cube_id = ?")) {

                                NCube cube = NCube.createCubeFromStream(rs.getBinaryStream("cube_value_bin"));
                                sha1 = cube.sha1();

                                try (OutputStream stream = new GZIPOutputStream(out, 8192)) {
                                    new JsonFormatter(stream).formatCube(cube);
                                }

                                update.setString(1, sha1);
                                update.setBlob(2, blob);
                                update.setLong(3, rs.getLong("n_cube_id"));
                                update.executeUpdate();
                            }
                        }
                    }

                    insert.setLong(1, UniqueIdGenerator.getUniqueId());
                    insert.setString(2, rs.getString("n_cube_nm"));
                    insert.setBytes(3, jsonBytes);
                    insert.setTimestamp(4, rs.getTimestamp("create_dt"));
                    insert.setString(5, rs.getString("create_hid"));
                    insert.setString(6, appId.getVersion());
                    insert.setString(7, ReleaseStatus.SNAPSHOT.name());
                    insert.setString(8, appId.getApp());
                    insert.setBytes(9, rs.getBytes(TEST_DATA_BIN));
                    insert.setBytes(10, rs.getBytes(NOTES_BIN));
                    insert.setString(11, appId.getTenant());
                    insert.setString(12, appId.getBranch());
                    insert.setLong(13, (rs.getLong("revision_number") >= 0) ? 0 : -1);
                    insert.setBoolean(14, false);
                    insert.setString(15, sha1);
                    insert.setString(16, sha1);
                    insert.addBatch();
                    count++;
                    if (count % EXECUTE_BATCH_CONSTANT == 0)
                    {
                        insert.executeBatch();
                    }
                }
                if (count % EXECUTE_BATCH_CONSTANT != 0)
                {
                    insert.executeBatch();
                }
                return count;
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to create new BRANCH for app: " + appId + ", due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public int releaseCubes(Connection c, ApplicationID appId, String newSnapVer)
    {
        if (doReleaseCubesExist(c, appId))
        {
            throw new IllegalStateException("A RELEASE version " + appId.getVersion() + " already exists, app: " + appId);
        }

        // Step 1: Update version number to new version where branch != HEAD (and rest of appId matches) ignore revision
        try
        {
            try (PreparedStatement statement = c.prepareStatement(
                    "UPDATE n_cube SET version_no_cd = ? " +
                            "WHERE app_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id != 'HEAD'"))
            {
                statement.setString(1, newSnapVer);
                statement.setString(2, appId.getApp());
                statement.setString(3, appId.getVersion());
                statement.setString(4, appId.getTenant());
                statement.executeUpdate();
            }
        }
        catch (Exception e)
        {
            String s = "Unable to move branched snapshot cubes for app: " + appId + ", due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }

        // Step 2: Release cubes where branch == HEAD (change their status from SNAPSHOT to RELEASE)
        try
        {
            try (PreparedStatement statement = c.prepareStatement(
                    "UPDATE n_cube SET create_dt = ?, status_cd = ? " +
                            "WHERE app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = 'HEAD'"))
            {
                statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                statement.setString(2, ReleaseStatus.RELEASE.name());
                statement.setString(3, appId.getApp());
                statement.setString(4, appId.getVersion());
                statement.setString(5, ReleaseStatus.SNAPSHOT.name());
                statement.setString(6, appId.getTenant());
                statement.executeUpdate();
            }
        }
        catch (Exception e)
        {
            String s = "Unable to release head cubes for app: " + appId + ", due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }

        // Step 3: Create new SNAPSHOT cubes from the HEAD RELEASE cubes (next version higher, started for development)
        try
        {
            ApplicationID releaseId = appId.asRelease();

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_ACTIVE_RECORDS_ONLY, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, releaseId, null, options);
                 ResultSet rs = stmt.executeQuery())
            {
                int count = 0;
                try (PreparedStatement insert = c.prepareStatement(
                        "INSERT INTO n_cube (n_cube_id, n_cube_nm, cube_value_bin, create_dt, create_hid, version_no_cd, status_cd, app_cd, test_data_bin, notes_bin, tenant_cd, branch_id, revision_number) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
                {
                    while (rs.next())
                    {
                        insert.setLong(1, UniqueIdGenerator.getUniqueId());
                        insert.setString(2, rs.getString("n_cube_nm"));
                        insert.setBytes(3, rs.getBytes("cube_value_bin"));
                        insert.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                        insert.setString(5, rs.getString("create_hid"));
                        insert.setString(6, newSnapVer);
                        insert.setString(7, ReleaseStatus.SNAPSHOT.name());
                        insert.setString(8, appId.getApp());
                        insert.setBytes(9, rs.getBytes(TEST_DATA_BIN));
                        insert.setBytes(10, rs.getBytes(NOTES_BIN));
                        insert.setString(11, appId.getTenant());
                        insert.setString(12, ApplicationID.HEAD);
                        insert.setLong(13, 0); // New SNAPSHOT revision numbers start at 0, we don't move forward deleted records.
                        insert.addBatch();
                        count++;
                        if (count % EXECUTE_BATCH_CONSTANT == 0)
                        {
                            insert.executeBatch();
                        }
                    }

                    if (count % EXECUTE_BATCH_CONSTANT != 0)
                    {
                        insert.executeBatch();
                    }
                }
                return count;
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to create SNAPSHOT cubes for app: " + appId + ", new version: " + newSnapVer + ", due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public int changeVersionValue(Connection c, ApplicationID appId, String newVersion)
    {
        ApplicationID newSnapshot = appId.createNewSnapshotId(newVersion);
        if (doCubesExist(c, newSnapshot, true))
        {
            throw new IllegalStateException("Cannot change version value to " + newVersion + " because this version already exists.  Choose a different version number, app: " + appId);
        }

        try
        {
            try (PreparedStatement ps = c.prepareStatement("UPDATE n_cube SET version_no_cd = ? WHERE app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?"))
            {
                ps.setString(1, newVersion);
                ps.setString(2, appId.getApp());
                ps.setString(3, appId.getVersion());
                ps.setString(4, ReleaseStatus.SNAPSHOT.name());
                ps.setString(5, appId.getTenant());
                ps.setString(6, appId.getBranch());

                int count = ps.executeUpdate();
                if (count < 1)
                {
                    throw new IllegalStateException("No SNAPSHOT n-cubes found with version " + appId.getVersion() + ", therefore no versions updated, app: " + appId);
                }
                return count;
            }
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to change SNAPSHOT version from " + appId.getVersion() + " to " + newVersion + " for app: " + appId + ", due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public String getTestData(Connection c, ApplicationID appId, String cubeName)
    {
        try (PreparedStatement stmt = c.prepareStatement(
                "SELECT test_data_bin FROM n_cube " +
                        "WHERE " + buildNameCondition(c, "n_cube_nm") + " = ? AND app_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?" +
                        "ORDER BY abs(revision_number) DESC"
        ))
        {
            stmt.setString(1, buildName(c, cubeName));
            stmt.setString(2, appId.getApp());
            stmt.setString(3, appId.getVersion());
            stmt.setString(4, appId.getTenant());
            stmt.setString(5, appId.getBranch());

            try (ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    byte[] testData = rs.getBytes("test_data_bin");
                    return testData == null ? "" : new String(testData, "UTF-8");
                }
            }
            throw new IllegalArgumentException("Unable to fetch test data, cube: " + cubeName + ", app: " + appId + " does not exist.");
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to fetch test data for cube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean updateTestData(Connection c, ApplicationID appId, String cubeName, String testData)
    {
        Long maxRev = getMaxRevision(c, appId, cubeName);

        if (maxRev == null)
        {
            throw new IllegalArgumentException("Cannot update test data, cube: " + cubeName + " does not exist in app: " + appId);
        }
        if (maxRev < 0)
        {
            throw new IllegalArgumentException("Cannot update test data, cube: " + cubeName + " is deleted in app: " + appId);
        }

        try (PreparedStatement stmt = c.prepareStatement(
                "UPDATE n_cube SET test_data_bin=? " +
                        "WHERE app_cd = ? AND " + buildNameCondition(c, "n_cube_nm") + " = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ? AND revision_number = ?"))
        {
            stmt.setBytes(1, testData == null ? null : testData.getBytes("UTF-8"));
            stmt.setString(2, appId.getApp());
            stmt.setString(3, buildName(c, cubeName));
            stmt.setString(4, appId.getVersion());
            stmt.setString(5, ReleaseStatus.SNAPSHOT.name());
            stmt.setString(6, appId.getTenant());
            stmt.setString(7, appId.getBranch());
            stmt.setLong(8, maxRev);
            return stmt.executeUpdate() == 1;
        }
        catch (Exception e)
        {
            String s = "Unable to update test data for NCube: " + cubeName + ", app: " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean mergeAcceptTheirs(Connection c, ApplicationID appId, String cubeName, String branchSha1, String username)
    {
        try
        {
            ApplicationID headId = appId.asHead();
            byte[] headBytes = null;
            Long headRevision = null;
            byte[] headTestData = null;
            String headSha1 = null;

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
            options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, headId, cubeName, options);
                 ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    headBytes = rs.getBytes("cube_value_bin");
                    headRevision = rs.getLong("revision_number");
                    headTestData = rs.getBytes("test_data_bin");
                    headSha1 = rs.getString("sha1");
                }
            }

            if (headRevision == null)
            {
                throw new IllegalStateException("failed to overwrite because HEAD cube does not exist: " + cubeName + "', app: " + appId);
            }

            Long newRevision = null;
            String oldBranchSha1 = null;

            try (PreparedStatement ps = createSelectCubesStatement(c, appId, cubeName, options);
                 ResultSet rs = ps.executeQuery())
            {
                if (rs.next())
                {
                    newRevision = rs.getLong("revision_number");
                    oldBranchSha1 = rs.getString("sha1");
                }
            }

            if (newRevision == null)
            {
                throw new IllegalStateException("failed to overwrite because branch cube does not exist: " + cubeName + "', app: " + appId);
            }

            if (!StringUtilities.equalsIgnoreCase(branchSha1, oldBranchSha1))
            {
                throw new IllegalStateException("failed to overwrite because branch cube has changed: " + cubeName + "', app: " + appId);
            }

            String notes = "Branch cube overwritten: " + appId + ", name: " + cubeName;

            long rev = Math.abs(newRevision) + 1;

            if (insertCube(c, appId, cubeName, headRevision < 0 ?  -rev : rev, headBytes, headTestData, notes, false, headSha1, headSha1, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to overwrite branch cube: '" + cubeName + "', app: " + appId);
            }

            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to overwrite cube: " + cubeName + ", app: " + appId + " due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean mergeAcceptMine(Connection c, ApplicationID appId, String cubeName, String username)
    {
        try
        {
            ApplicationID headId = appId.asHead();
            Long headRevision = null;
            String headSha1 = null;

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, headId, cubeName, options);
                 ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    headRevision = rs.getLong("revision_number");
                    headSha1 = rs.getString("sha1");
                }
            }

            if (headRevision == null)
            {
                throw new IllegalStateException("failed to update branch cube because HEAD cube does not exist: " + cubeName + "', app: " + appId);
            }

            Long newRevision = null;
            String tipBranchSha1 = null;
            byte[] myTestData = null;
            byte[] myBytes = null;
            boolean changed = false;
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);

            try (PreparedStatement ps = createSelectCubesStatement(c, appId, cubeName, options);
                 ResultSet rs = ps.executeQuery())
            {
                if (rs.next())
                {
                    myBytes = rs.getBytes("cube_value_bin");
                    myTestData = rs.getBytes("test_data_bin");
                    newRevision = rs.getLong("revision_number");
                    tipBranchSha1 = rs.getString("sha1");
                    changed = rs.getBoolean("changed");
                }
            }

            if (newRevision == null)
            {
                throw new IllegalStateException("failed to update branch cube because branch cube does not exist: " + cubeName + "', app: " + appId);
            }

            String notes = "Merge: branch cube accepted over head cube: " + appId + ", name: " + cubeName;

            long rev = Math.abs(newRevision) + 1;

            if (insertCube(c, appId, cubeName, newRevision < 0 ?  -rev : rev, myBytes, myTestData, notes, changed, tipBranchSha1, headSha1, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to overwrite branch cube: '" + cubeName + "', app: " + appId);
            }

            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to overwrite cube: " + cubeName + ", app: " + appId + " due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean duplicateCube(Connection c, ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    {
        try
        {
            byte[] jsonBytes = null;
            Long oldRevision = null;
            byte[] oldTestData = null;
            String sha1 = null;

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
            options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

            try (PreparedStatement originalStatement = createSelectCubesStatement(c, oldAppId, oldName, options);
                 ResultSet rs = originalStatement.executeQuery())
            {
                if (rs.next())
                {
                    jsonBytes = rs.getBytes("cube_value_bin");
                    oldRevision = rs.getLong("revision_number");
                    oldTestData = rs.getBytes("test_data_bin");
                    sha1 = rs.getString("sha1");
                }
                else
                {
                    throw new IllegalArgumentException("Unable to duplicate cube because source cube does not exist.  AppId:  " + oldAppId + ", " + oldName);
                }
            }

            if (oldRevision < 0)
            {
                throw new IllegalArgumentException("Unable to duplicate deleted cube.  AppId:  " + oldAppId + ", " + oldName);
            }

            Long newRevision = null;
            String headSha1 = null;

            try (PreparedStatement newStatement = createSelectCubesStatement(c, newAppId, newName, options);
                 ResultSet rs = newStatement.executeQuery())
            {
                if (rs.next())
                {
                    newRevision = rs.getLong("revision_number");
                    headSha1 = rs.getString("head_sha1");
                }
            }

            if (newRevision != null && newRevision >= 0)
            {
                throw new IllegalArgumentException("Unable to duplicate cube, a cube already exists with the new name.  appId:  " + newAppId + ", name: " + newName);
            }

            boolean changed = !StringUtilities.equalsIgnoreCase(oldName, newName);
            boolean sameExceptBranch = oldAppId.equalsNotIncludingBranch(newAppId);

            // If names are different we need to recalculate the sha-1
            if (changed)
            {
                NCube ncube = NCube.createCubeFromGzipBytes(jsonBytes);
                ncube.setName(newName);
                ncube.setApplicationID(newAppId);
                jsonBytes = ncube.getCubeAsGzipJsonBytes();
                sha1 = ncube.sha1();
            }

            String notes = "Cube duplicated from app: " + oldAppId + ", name: " + oldName;

            if (insertCube(c, newAppId, newName, newRevision == null ? 0 : Math.abs(newRevision) + 1, jsonBytes, oldTestData, notes, changed, sha1, sameExceptBranch ? headSha1 : null, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to duplicate cube: " + oldName + " -> " + newName + "', app: " + oldAppId);
            }

            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to duplicate cube: " + oldName + ", app: " + oldAppId + ", new name: " + newName + ", app: " + newAppId + " due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    //    public int cleanUp(Connection c, ApplicationID appId)
//    {
//        DELETE FROM posts WHERE id IN (
//            SELECT * FROM (
//                    SELECT id FROM posts GROUP BY id HAVING ( COUNT(id) > 1 )
//            ) AS p
//        )
//
//        int count = 0;
//
//        String sql = "SELECT *" +
//                " FROM n_cube n, " +
//                "( " +
//                "  SELECT n_cube_nm, max(abs(revision_number)) AS max_rev " +
//                "  FROM n_cube " +
//                "  WHERE app_cd = ? AND version_no_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?" +
//                " GROUP BY n_cube_nm " +
//                ") m " +
//                "WHERE m.n_cube_nm = n.n_cube_nm AND (m.max_rev <> abs(n.revision_number) OR n.revision_number <> -1 OR n.revision_number <> 0) AND n.app_cd = ? AND n.version_no_cd = ? AND n.status_cd = ? AND n.tenant_cd = RPAD(?, 10, ' ') AND n.branch_id = ?";
//
//        try (PreparedStatement stmt = c.prepareStatement(sql))
//        {
//            try (ResultSet rs = stmt.executeQuery())
//            {
//                while(rs.next()) {
//                    count++;
//                }
//            }
//            return count;
//        }
//        catch(Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    public boolean renameCube(Connection c, ApplicationID appId, String oldName, String newName, String username)
    {
        try
        {
            byte[] oldBytes = null;
            Long oldRevision = null;
            String oldSha1 = null;
            String oldHeadSha1 = null;
            byte[] oldTestData = null;

            Map<String, Object> options = new HashMap<>();
            options.put(NCubeManager.SEARCH_INCLUDE_CUBE_DATA, true);
            options.put(NCubeManager.SEARCH_INCLUDE_TEST_DATA, true);
            options.put(NCubeManager.SEARCH_EXACT_MATCH_NAME, true);

            try (PreparedStatement stmt = createSelectCubesStatement(c, appId, oldName, options);
                 ResultSet rs = stmt.executeQuery())
            {
                if (rs.next())
                {
                    oldBytes = rs.getBytes("cube_value_bin");
                    oldRevision = rs.getLong("revision_number");
                    oldTestData = rs.getBytes("test_data_bin");
                    oldSha1 = rs.getString("sha1");
                    oldHeadSha1 = rs.getString("head_sha1");
                }
                else
                {
                    throw new IllegalArgumentException("Could not rename cube because cube does not exist.  AppId:  " + appId + ", " + oldName);
                }
            }

            if (oldRevision != null && oldRevision < 0)
            {
                throw new IllegalArgumentException("Deleted cubes cannot be renamed.  AppId:  " + appId + ", " + oldName + " -> " + newName);
            }

            Long newRevision = null;
            String newHeadSha1 = null;

            try (PreparedStatement ps = createSelectCubesStatement(c, appId, newName, options);
                 ResultSet rs = ps.executeQuery())
            {
                if (rs.next())
                {
                    newRevision = rs.getLong("revision_number");
                    newHeadSha1 = rs.getString(HEAD_SHA_1);
                }
            }

            if (newRevision != null && newRevision >= 0)
            {
                throw new IllegalArgumentException("Unable to rename cube, a cube already exists with that name.  appId:  " + appId + ", name: " + newName);
            }

            NCube ncube = NCube.createCubeFromGzipBytes(oldBytes);
            ncube.setName(newName);

            String notes = "Cube renamed:  " + oldName + " -> " + newName;
            if (insertCube(c, appId, ncube, newRevision == null ? 0 : Math.abs(newRevision) + 1, oldTestData, notes, true, newHeadSha1, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to rename cube: " + oldName + " -> " + newName + "', app: " + appId);
            }

            if (insertCube(c, appId, oldName, -(oldRevision + 1), oldBytes, oldTestData, notes, true, oldSha1, oldHeadSha1, System.currentTimeMillis(), username) == null)
            {
                throw new IllegalStateException("Unable to rename cube: " + oldName + " -> " + newName + ", app: " + appId);
            }

            return true;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            String s = "Unable to rename cube: " + oldName + ", app: " + appId + ", new name: " + newName + " due to: " + e.getMessage();
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    private String createNote(String user, Date date, String notes)
    {
        return date + " [" + user + "] " + notes;
    }

    public PreparedStatement createInsertStatement(Connection c) throws SQLException
    {
        String sql = "INSERT INTO n_cube (n_cube_id, tenant_cd, app_cd, version_no_cd, status_cd, branch_id, n_cube_nm, revision_number, sha1, head_sha1, create_dt, create_hid, cube_value_bin, test_data_bin, notes_bin, changed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        return c.prepareStatement(sql);
    }

    NCubeInfoDto insertCube(Connection c, ApplicationID appId, String name, Long revision, byte[] cubeData, byte[] testData, String notes, boolean changed, String sha1, String headSha1, long time, String username) throws SQLException
    {
        try (PreparedStatement s = createInsertStatement(c))
        {
            long uniqueId = UniqueIdGenerator.getUniqueId();
            s.setLong(1, uniqueId);
            s.setString(2, appId.getTenant());
            s.setString(3, appId.getApp());
            s.setString(4, appId.getVersion());
            s.setString(5, appId.getStatus());
            s.setString(6, appId.getBranch());
            s.setString(7, name);
            s.setLong(8, revision);
            s.setString(9, sha1);
            s.setString(10, headSha1);
            Timestamp now = new Timestamp(time);
            s.setTimestamp(11, now);
            s.setString(12, username);
            s.setBytes(13, cubeData);
            s.setBytes(14, testData);
            String note = createNote(username, now, notes);
            s.setBytes(15, StringUtilities.getBytes(note, "UTF-8"));
            s.setInt(16, changed ? 1 : 0);

            NCubeInfoDto dto = new NCubeInfoDto();
            dto.id = Long.toString(uniqueId);
            dto.name = name;
            dto.sha1 = sha1;
            dto.headSha1 = sha1;
            dto.changed = changed;
            dto.app = appId.getApp();
            dto.branch = appId.getBranch();
            dto.tenant = appId.getTenant();
            dto.version = appId.getVersion();
            dto.status = appId.getStatus();
            dto.createDate = new Date(time);
            dto.createHid = username;
            dto.notes = note;
            dto.revision = Long.toString(revision);

            return s.executeUpdate() == 1 ? dto : null;
        }
    }

    NCubeInfoDto insertCube(Connection c, ApplicationID appId, NCube cube, Long revision, byte[] testData, String notes, boolean changed, String headSha1, long time, String username) throws SQLException, IOException
    {
        try (PreparedStatement s = createInsertStatement(c))
        {
            final Blob blob = c.createBlob();
            OutputStream out = blob.setBinaryStream(1L);

            try (OutputStream stream = new GZIPOutputStream(out, 8192)) {
                new JsonFormatter(stream).formatCube(cube);
            }

            long uniqueId = UniqueIdGenerator.getUniqueId();
            s.setLong(1, uniqueId);
            s.setString(2, appId.getTenant());
            s.setString(3, appId.getApp());
            s.setString(4, appId.getVersion());
            s.setString(5, appId.getStatus());
            s.setString(6, appId.getBranch());
            s.setString(7, cube.getName());
            s.setLong(8, revision);
            s.setString(9, cube.sha1());
            s.setString(10, headSha1);
            Timestamp now = new Timestamp(time);
            s.setTimestamp(11, now);
            s.setString(12, username);
            s.setBlob(13, blob);
            s.setBytes(14, testData);
            String note = createNote(username, now, notes);
            s.setBytes(15, StringUtilities.getBytes(note, "UTF-8"));
            s.setBoolean(16, changed);

            NCubeInfoDto dto = new NCubeInfoDto();
            dto.id = Long.toString(uniqueId);
            dto.name = cube.getName();
            dto.sha1 = cube.sha1();
            dto.headSha1 = cube.sha1();
            dto.changed = changed;
            dto.app = appId.getApp();
            dto.branch = appId.getBranch();
            dto.tenant = appId.getTenant();
            dto.version = appId.getVersion();
            dto.status = appId.getStatus();
            dto.createDate = new Date(time);
            dto.createHid = username;
            dto.notes = note;
            dto.revision = Long.toString(revision);

            return s.executeUpdate() == 1 ? dto : null;
        }
    }

    /**
     * Check for existence of a cube with this appId.  You can ignoreStatus if you want to check for existence of
     * a SNAPSHOT or RELEASE cube.
     * @param ignoreStatus - If you want to ignore status (check for both SNAPSHOT and RELEASE cubes in existence) pass
     *                     in true.
     * @return true if any cubes exist for the given AppId, false otherwise.
     */
    public boolean doCubesExist(Connection c, ApplicationID appId, boolean ignoreStatus)
    {
        String statement = "SELECT n_cube_id FROM n_cube WHERE app_cd = ? AND version_no_cd = ? AND tenant_cd = RPAD(?, 10, ' ') AND branch_id = ?";

        if (!ignoreStatus)
        {
            statement += " AND status_cd = ?";
        }

        try (PreparedStatement ps = c.prepareStatement(statement))
        {
            ps.setString(1, appId.getApp());
            ps.setString(2, appId.getVersion());
            ps.setString(3, appId.getTenant());
            ps.setString(4, appId.getBranch());

            if (!ignoreStatus)
            {
                ps.setString(5, appId.getStatus());
            }

            ps.setMaxRows(1);

            try (ResultSet rs = ps.executeQuery())
            {
                return rs.next();
            }
        }
        catch (Exception e)
        {
            String s = "Error checking for existing cubes:  " + appId;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public boolean doReleaseCubesExist(Connection c, ApplicationID appId)
    {
        return doCubesExist(c, appId.asRelease(), false);
    }

    /**
     * Return an array [] of Strings containing all unique App names.
     */
    public List<String> getAppNames(Connection connection, String tenant, String status, String branch)
    {
        String sql = "SELECT DISTINCT app_cd FROM n_cube WHERE tenant_cd = RPAD(?, 10, ' ') and status_cd = ? and branch_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
            List<String> records = new ArrayList<>();

            stmt.setString(1, tenant);
            stmt.setString(2, status);
            stmt.setString(3, branch);

            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    records.add(rs.getString(1));
                }
            }
            return records;
        }
        catch (Exception e)
        {
            String s = "Unable to fetch all app names for tenant: " + tenant + ", branch: " + branch;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public List<String> getAppVersions(Connection connection, String tenant, String app, String status, String branch)
    {
        final String sql = "SELECT DISTINCT version_no_cd FROM n_cube WHERE app_cd = ? AND status_cd = ? AND tenant_cd = RPAD(?, 10, ' ') and branch_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt.setString(1, app);
            stmt.setString(2, status);
            stmt.setString(3, tenant);
            stmt.setString(4, branch);

            List<String> records = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    records.add(rs.getString(1));
                }
            }

            return records;
        }
        catch (Exception e)
        {
            String s = "Unable to fetch all versions for app: " + app;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public Set<String> getBranches(Connection connection, String tenant)
    {
        final String sql = "SELECT DISTINCT branch_id FROM n_cube WHERE tenant_cd = RPAD(?, 10, ' ')";
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt.setString(1, tenant);

            Set<String> branches = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery())
            {
                while (rs.next())
                {
                    branches.add(rs.getString(1));
                }
            }

            return branches;
        }
        catch (Exception e)
        {
            String s = "Unable to fetch all branches for tenant: " + tenant;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    private static String convertPattern(String pattern)
    {
        if (StringUtilities.isEmpty(pattern) || "*".equals(pattern))
        {
            return null;
        }
        else
        {
            pattern = pattern.replace('*', '%');
            pattern = pattern.replace('?', '_');
        }
        return pattern;
    }

    public String buildNameCondition(Connection c, String name)
    {
        if (isOracle(c))
        {
            return ("LOWER(" + name + ")");
        }

        return name;
    }

    public String buildName(Connection c, String name)
    {
        if (isOracle(c))
        {
            return name == null ? null : name.toLowerCase();
        }

        return name;
    }

    public boolean isOracle(Connection c)
    {
        if (c == null)
        {
            return false;
        }
        try
        {
            return Regexes.isOraclePattern.matcher(c.getMetaData().getDriverName()).matches();
        }
        catch (SQLException e)
        {
            LOG.warn("Unable to fetch JDBC connection metadata", e);
            return false;
        }
    }
}
