package com.cedarsoftware.ncube

import java.sql.Connection
/**
 * This adapter could be replaced by an adapting proxy.  Then you could
 * implement the interface and the class and not need this class.
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
public class NCubeJdbcPersisterAdapter implements NCubePersister
{
    private final NCubeJdbcPersister persister = new NCubeJdbcPersister()
    private final JdbcConnectionProvider connectionProvider;

    public NCubeJdbcPersisterAdapter(JdbcConnectionProvider provider)
    {
        connectionProvider = provider;
    }

    public Object jdbcOperation(Closure closure)
    {
        Connection c = connectionProvider.getConnection()
        try
        {
//            long start = System.nanoTime()
            Object ret = closure(c)
//            long end = System.nanoTime()
//            long delta = end - start
//            if (delta / 1000000 > 250)
//            {   // Greater than 250ms, log as slow db query
//                println 'slow DB operation: ' + (delta / 1000000)
//            }
            return ret
        }
        finally
        {
            connectionProvider.releaseConnection(c)
        }
    }

    public void updateCube(ApplicationID appId, NCube cube, final String username)
    {
        jdbcOperation { Connection c -> persister.updateCube(c, appId, cube, username) }
    }

    public NCube loadCubeById(long cubeId)
    {
        return (NCube) jdbcOperation { Connection c -> persister.loadCubeById(c, cubeId) }
    }

    public NCube loadCube(ApplicationID appId, String name)
    {
        return (NCube) jdbcOperation { Connection c -> persister.loadCube(c, appId, name) }
    }

    public NCube loadCubeBySha1(ApplicationID appId, String name, String sha1)
    {
        return (NCube) jdbcOperation { Connection c -> persister.loadCubeBySha1(c, appId, name, sha1) }
    }

    public boolean restoreCubes(ApplicationID appId, Object[] names, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.restoreCubes(c, appId, names, username) }
    }

    public List<NCubeInfoDto> getRevisions(ApplicationID appId, String cubeName)
    {
        return (List<NCubeInfoDto>) jdbcOperation { Connection c -> persister.getRevisions(c, appId, cubeName) }
    }

    public Set<String> getBranches(String tenant)
    {
        return (Set<String>) jdbcOperation { Connection c -> persister.getBranches(c, tenant) }
    }

    public boolean deleteBranch(ApplicationID branchId)
    {
        return (boolean) jdbcOperation { Connection c -> persister.deleteBranch(c, branchId) }
    }

    public boolean deleteCubes(ApplicationID appId, Object[] cubeNames, boolean allowDelete, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.deleteCubes(c, appId, cubeNames, allowDelete, username) }
    }

    public List<String> getAppNames(String tenant, String status, String branch)
    {
        return (List<String>) jdbcOperation { Connection c -> persister.getAppNames(c, tenant, status, branch) }
    }

    public List<String> getAppVersions(String tenant, String app, String status, String branch)
    {
        return (List<String>) jdbcOperation { Connection c -> persister.getAppVersions(c, tenant, app, status, branch) }
    }

    public int changeVersionValue(ApplicationID appId, String newVersion)
    {
        return (int) jdbcOperation { Connection c -> persister.changeVersionValue(c, appId, newVersion) }
    }

    public int releaseCubes(ApplicationID appId, String newSnapVer)
    {
        return (int) jdbcOperation { Connection c -> persister.releaseCubes(c, appId, newSnapVer) }
    }

    public int createBranch(ApplicationID appId)
    {
        return (int) jdbcOperation { Connection c -> persister.createBranch(c, appId) }
    }

    public boolean renameCube(ApplicationID appId, String oldName, String newName, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.renameCube(c, appId, oldName, newName, username) }
    }

    public boolean mergeAcceptTheirs(ApplicationID appId, String cubeName, String branchSha1, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.mergeAcceptTheirs(c, appId, cubeName, branchSha1, username) }
    }

    public boolean mergeAcceptMine(ApplicationID appId, String cubeName, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.mergeAcceptMine(c, appId, cubeName, username) }
    }

    public boolean duplicateCube(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName, String username)
    {
        return (boolean) jdbcOperation { Connection c -> persister.duplicateCube(c, oldAppId, newAppId, oldName, newName, username) }
    }

    public boolean updateNotes(ApplicationID appId, String cubeName, String notes)
    {
        return (boolean) jdbcOperation { Connection c -> persister.updateNotes(c, appId, cubeName, notes) }
    }

    public boolean updateTestData(ApplicationID appId, String cubeName, String testData)
    {
        return (boolean) jdbcOperation { Connection c -> persister.updateTestData(c, appId, cubeName, testData) }
    }

    public String getTestData(ApplicationID appId, String cubeName)
    {
        return (String) jdbcOperation { Connection c -> persister.getTestData(c, appId, cubeName) }
    }

    public NCubeInfoDto commitMergedCubeToHead(ApplicationID appId, NCube cube, String username)
    {
        return (NCubeInfoDto) jdbcOperation { Connection c -> persister.commitMergedCubeToHead(c, appId, cube, username) }
    }

    public NCubeInfoDto commitMergedCubeToBranch(ApplicationID appId, NCube cube, String headSha1, String username)
    {
        return (NCubeInfoDto) jdbcOperation { Connection c -> persister.commitMergedCubeToBranch(c, appId, cube, headSha1, username) }
    }

    public List<NCubeInfoDto> commitCubes(ApplicationID appId, Object[] cubeIds, String username)
    {
        return (List<NCubeInfoDto>) jdbcOperation { Connection c -> persister.commitCubes(c, appId, cubeIds, username) }
    }

    public int rollbackCubes(ApplicationID appId, Object[] names, String username)
    {
        return (int) jdbcOperation { Connection c -> persister.rollbackCubes(c, appId, names, username) }
    }

    public List<NCubeInfoDto> pullToBranch(ApplicationID appId, Object[] cubeIds, String username)
    {
        return (List<NCubeInfoDto>) jdbcOperation { Connection c -> persister.pullToBranch(c, appId, cubeIds, username) }
    }

    public boolean updateBranchCubeHeadSha1(Long cubeId, String headSha1)
    {
        return (boolean) jdbcOperation { Connection c -> persister.updateBranchCubeHeadSha1(c, cubeId, headSha1) }
    }

    public List<NCubeInfoDto> search(ApplicationID appId, String cubeNamePattern, String searchValue, Map options)
    {
        return (List<NCubeInfoDto>) jdbcOperation { Connection c -> persister.search(c, appId, cubeNamePattern, searchValue, options) }
    }
}
