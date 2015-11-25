package com.cedarsoftware.ncube.formatters;

import com.cedarsoftware.ncube.CellInfo;
import com.cedarsoftware.ncube.NCubeTest;
import com.cedarsoftware.ncube.StringValuePair;

import java.io.IOException;

/**
 * @author Ken Partlow (kpartlow@gmail.com)
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
public class NCubeTestWriter extends BaseJsonFormatter
{
    public String format(Object[] tests) throws IOException {
        startArray();
        if (tests != null && tests.length > 0)
        {
            boolean firstPass = true;
            for (Object test : tests)
            {
                if (!firstPass) {
                    comma();
                }
                writeTest((NCubeTest) test);
                firstPass = false;
            }
        }
        endArray();

        return builder.toString();
    }

    private void writeTest(NCubeTest test) throws IOException {
        startObject();
        writeObjectKeyValue("name", test.getName(), true);
        writeObjectKey("coord");
        writeCoord(test.getCoord());
        comma();
        writeObjectKey("assertions");
        writeAssertions(test.getAssertions());
        endObject();
    }

    private void writeCoord(StringValuePair<CellInfo>[] coord) throws IOException {
        startArray();
        if (coord != null && coord.length > 0)
        {
            boolean firstPass = true;
            for (StringValuePair<CellInfo> parameter : coord)
            {
                if (!firstPass) {
                    comma();
                }
                startObject();
                writeObjectKey(parameter.getKey());
                writeCellInfo(parameter.getValue());
                endObject();
                firstPass = false;
            }
        }
        endArray();
    }

    private void writeAssertions(CellInfo[] assertions) throws IOException {
        startArray();
        if (assertions != null && assertions.length > 0)
        {
            boolean firstPass = true;
            for (CellInfo item : assertions)
            {
                if (!firstPass) {
                    comma();
                }
                writeCellInfo(item);
                firstPass = false;
            }
        }
        endArray();
    }

    public void writeCellInfo(CellInfo info) throws IOException
    {
        startObject();
        if (info != null)
        {
            writeObjectKeyValue("type", info.dataType, true);
            writeObjectKeyValue("isUrl", info.isUrl, true);
            writeObjectKeyValue("value", info.value, false);
        }
        endObject();
    }
}
