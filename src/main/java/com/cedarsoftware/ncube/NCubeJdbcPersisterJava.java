package com.cedarsoftware.ncube;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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


}
