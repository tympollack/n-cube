package com.cedarsoftware.ncube.exception

import groovy.transform.CompileStatic

/**
 * This exception supports the Rule Engine jump() feature which allows
 * restarting rules on the 1st rule or named rule.
 *
 * This exception only kills off one 'stack frame'.  So if an n-cube calls another
 * n-cube, and the called n-cube throws a rule stop, only the inner n-cubes
 * execution is ended.  The execution is returned to the outer (calling) n-cube
 * and picks up as it normally does on a call to another n-cube.
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
@CompileStatic
class RuleJump extends RuntimeException
{
    private final Map coord

    RuleJump(Map coordinate)
    {
        coord = coordinate
    }

    Map<String, Object> getCoord()
    {
        return coord
    }
}
