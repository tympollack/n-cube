import com.cedarsoftware.ncube.*
import com.cedarsoftware.ncube.exception.*
import com.cedarsoftware.ncube.formatters.*
import com.cedarsoftware.ncube.proximity.*
import com.cedarsoftware.ncube.util.*
import com.cedarsoftware.util.*
import com.cedarsoftware.util.io.*


NCube getCube(cubeName = ncube.name)
{
    if (cubeName == ncube.name)
    {
        return ncube
    }
    NCube cube = NCubeManager.getCube(ncube.applicationID, cubeName)
    if (cube == null)
    {
        throw new IllegalArgumentException('n-cube: ' + cubeName + ', does not exist in app: ' + ncube.applicationID)
    }
    return cube
}

Axis getAxis(String axisName, String cubeName = ncube.name)
{
    Axis axis = getCube(cubeName).getAxis(axisName)
    if (axis == null)
    {
        throw new IllegalArgumentException('Axis: ' + axisName + ', does not exist on n-cube: ' + cubeName + ', app: ' + ncube.applicationID)
    }
    return axis
}

Column getColumn(Comparable value, String axisName, String cubeName = ncube.name)
{
    return getAxis(axisName, cubeName).findColumn(value)
}

def at(Map coord, String cubeName = ncube.name, def defaultValue = null)
{
    input.putAll(coord)
    return getCube(cubeName).getCell(input, output, defaultValue)
}

def go(Map coord, String cubeName = ncube.name, def defaultValue = null)
{
    return getCube(cubeName).getCell(coord, output, defaultValue)
}

String getStringFromUrl(String url)
{
    byte[] bytes = getBytesFromUrl(url)
    if (bytes == null)
    {
        return null
    }
    return StringUtilities.createUtf8String(bytes)
}

byte[] getBytesFromUrl(String url)
{
    InputStream inStream = getClass().getResourceAsStream(url)
    byte[] bytes = IOUtilities.inputStreamToBytes(inStream)
    IOUtilities.close(inStream as Closeable)
    return bytes
}

def ruleStop()
{
    throw new RuleStop()
}

def jump(Map coord)
{
    input.putAll(coord);
    throw new RuleJump(input)
}
