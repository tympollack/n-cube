package com.cedarsoftware.ncube.formatters;

import com.cedarsoftware.ncube.CellInfo;
import com.cedarsoftware.ncube.NCubeTest;
import com.cedarsoftware.ncube.StringValuePair;
import com.cedarsoftware.util.ArrayUtilities;
import com.cedarsoftware.util.StringUtilities;
import com.cedarsoftware.util.io.JsonObject;
import com.cedarsoftware.util.io.JsonReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ken Partlow (kpartlow@gmail.com)
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
public class NCubeTestReader
{
    public static List<NCubeTest> convert(String s) throws IOException
    {
        List<NCubeTest> list = new ArrayList<>();
        if (StringUtilities.isEmpty(s)) {
            return list;
        }

        Map args = new HashMap();
        args.put(JsonReader.USE_MAPS, true);
        Object[] items = (Object[]) JsonReader.jsonToJava(s, args);

        for (Object o : items)
        {
            JsonObject item = (JsonObject)o;
            String name = (String)item.get("name");
            List<StringValuePair<CellInfo>> coord = createCoord((Object[])item.get("coord"));
            List<CellInfo> assertions = createAssertions((Object[]) item.get("assertions"));
            NCubeTest test = new NCubeTest(name, coord.toArray(new StringValuePair[coord.size()]), assertions.toArray(new CellInfo[assertions.size()]));
            list.add(test);
        }
        return list;
    }

    public static List<StringValuePair<CellInfo>> createCoord(Object[] inputs)
    {
        List<StringValuePair<CellInfo>> list = new ArrayList<>();
        if (ArrayUtilities.isEmpty(inputs))
        {
            return list;
        }

        for (Object item : inputs)
        {
            JsonObject<String, Object> input = (JsonObject<String, Object>)item;
            for (Map.Entry<String, Object> entry : input.entrySet())
            {
                list.add(new StringValuePair(entry.getKey(), createCellInfo((JsonObject)entry.getValue())));
            }
        }

        return list;
    }

    public static CellInfo createCellInfo(JsonObject o)
    {
        String type = (String)o.get("type");
        String value = (String)o.get("value");
        return new CellInfo(type, value, o.get("isUrl"), false);
    }

    public static List<CellInfo> createAssertions(Object[] cellInfoMaps)
    {
        List<CellInfo> list = new ArrayList<>();
        if (ArrayUtilities.isEmpty(cellInfoMaps))
        {
            return list;
        }
        for (Object cellInfoMap : cellInfoMaps)
        {
            list.add(createCellInfo((JsonObject)cellInfoMap));
        }
        return list;
    }
}
