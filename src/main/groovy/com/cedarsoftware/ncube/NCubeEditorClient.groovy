package com.cedarsoftware.ncube

import groovy.transform.CompileStatic

/**
 * @author John DeRegnaucourt (jdereg@gmail.com), Josh Snyder (joshsnyder@gmail.com)
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
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either eƒfetxpress or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */

@CompileStatic
interface NCubeEditorClient extends NCubeReleaseClient
{
    boolean updateCube(NCube ncube)

    NCube loadCubeById(long id)

    void createCube(NCube ncube)

    void duplicate(ApplicationID oldAppId, ApplicationID newAppId, String oldName, String newName)

    boolean checkPermissions(ApplicationID appId, String resource, Action action)

    boolean isAdmin(ApplicationID appId)

    String getAppLockedBy(ApplicationID appId)

    void lockApp(ApplicationID appId)

    void unlockApp(ApplicationID appId)
}