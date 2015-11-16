package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point2D
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.util.Converter
import com.cedarsoftware.util.StringUtilities

import java.text.MessageFormat
import java.util.regex.Matcher

/**
 * Allowed cell types for n-cube.
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
public enum CellTypes
{
    String('string'),
    Date('date'),
    Boolean('boolean'),
    Byte('byte'),
    Short('short'),
    Integer('int'),
    Long('long'),
    Float('float'),
    Double('double'),
    BigDecimal('bigdec'),
    BigInteger('bigint'),
    Binary('binary'),
    Exp('exp'),
    Method('method'),
    Template('template'),
    LatLon('latlon'),
    Point2D('point2d'),
    Point3D('point3d'),
    Null('null')

    private final String desc

    private CellTypes(String desc)
    {
        this.desc = desc
    }

    public String desc()
    {
        return desc
    }

    public static CellTypes getTypeFromString(String type)
    {
        if (type == null)
        {
            return String
        }

        switch (type)
        {
            case String.desc():
                return String

            case Date.desc():
                return Date

            case Boolean.desc():
                return Boolean

            case Byte.desc():
                return Byte

            case Short.desc():
                return Short

            case Integer.desc():
                return Integer

            case Long.desc():
                return Long

            case Float.desc():
                return Float

            case Double.desc():
                return Double

            case BigDecimal.desc():
                return BigDecimal

            case BigInteger.desc():
                return BigInteger

            case Binary.desc():
                return Binary

            case Exp.desc():
                return Exp

            case Method.desc():
                return Method

            case Template.desc():
                return Template

            case LatLon.desc():
                return LatLon

            case Point2D.desc():
                return Point2D

            case Point3D.desc():
                return Point3D

            case Null.desc():
                return Null

            default:
                throw new IllegalArgumentException("Invalid Type:  " + type)
        }
    }

    public static Object recreate(String type, String value, boolean isUrl, boolean isCached)
    {
        switch (type)
        {
            case String.desc():
                return isUrl ? new StringUrlCmd(value, isCached) : value

            case Date.desc():
                return Converter.convert(value, java.util.Date.class)

            case Boolean.desc():
                return Converter.convert(value, boolean.class)

            case Byte.desc():
                return Converter.convert(value, byte.class)

            case Short.desc():
                return Converter.convert(value, short.class)

            case Integer.desc():
                return Converter.convert(value, int.class)

            case Long.desc():
                return Converter.convert(value, long.class)

            case Float.desc():
                return Converter.convert(value, float.class)

            case Double.desc():
                return Converter.convert(value, double.class)

            case BigDecimal.desc():
                return Converter.convert(value, java.math.BigDecimal.class)

            case BigInteger.desc():
                return Converter.convert(value, java.math.BigInteger.class)

            case Binary.desc():
                return isUrl ? new BinaryUrlCmd(value, isCached) : StringUtilities.decode(value)

            case Exp.desc():
                return new GroovyExpression(isUrl ? null : value, isUrl ? value : null, isCached)

            case Method.desc():
                return new GroovyMethod(isUrl ? null : value, isUrl ? value : null, isCached)

            case Template.desc():
                return new GroovyTemplate(isUrl ? null : value, isUrl ? value : null, isCached)

            case LatLon.desc():
                Matcher m = Regexes.valid2Doubles.matcher(value)
                if (!m.matches())
                {
                    throw new IllegalArgumentException(java.lang.String.format("Invalid Lat/Long value (%s)", value))
                }
                return new LatLon((double) Converter.convert(m.group(1), double.class), (double) Converter.convert(m.group(2), double.class))

            case Point2D.desc():
                Matcher m = Regexes.valid2Doubles.matcher(value)
                if (!m.matches())
                {
                    throw new IllegalArgumentException(java.lang.String.format("Invalid Point2D value (%s)", value))
                }
                return new Point2D((double) Converter.convert(m.group(1), double.class), (double) Converter.convert(m.group(2), double.class))

            case Point3D.desc():
                Matcher m = Regexes.valid3Doubles.matcher(value)
                if (!m.matches())
                {
                    throw new IllegalArgumentException(java.lang.String.format("Invalid Point3D value (%s)", value))
                }
                return new Point3D((double) Converter.convert(m.group(1), double.class),
                        (double) Converter.convert(m.group(2), double.class),
                        (double) Converter.convert(m.group(3), double.class))

            case Null.desc():
                return null

            case null:
                return null

            default:
                throw new IllegalArgumentException("Invalid Type:  " + type)
        }
    }

    public static String getType(Object cell, String section)
    {
        if (cell == null) {
            return null
        }

        if (cell instanceof String) {
            return String.desc()
        }

        if (cell instanceof java.lang.Double) {
            return Double.desc()
        }

        if (cell instanceof Long) {
            return Long.desc()
        }

        if (cell instanceof Boolean) {
            return Boolean.desc()
        }

        if (cell instanceof java.math.BigDecimal) {
            return BigDecimal.desc()
        }

        if (cell instanceof Float) {
            return Float.desc()
        }

        if (cell instanceof Integer) {
            return Integer.desc()
        }

        if (cell instanceof java.math.BigInteger) {
            return BigInteger.desc()
        }

        if (cell instanceof Byte) {
            return Byte.desc()
        }

        if (cell instanceof Short) {
            return Short.desc()
        }

        if (cell instanceof java.util.Date) {
            return Date.desc()
        }

        if (cell instanceof BinaryUrlCmd || cell instanceof byte[]) {
            return Binary.desc()
        }

        if (cell instanceof GroovyExpression || cell instanceof Collection || cell.getClass().isArray()) {
            return Exp.desc()
        }

        if (cell instanceof GroovyMethod) {
            return Method.desc()
        }

        if (cell instanceof GroovyTemplate) {
            return Template.desc()
        }

        if (cell instanceof StringUrlCmd) {
            return String.desc()
        }

        if (cell instanceof Point2D)
        {
            return Point2D.desc()
        }

        if (cell instanceof Point3D)
        {
            return Point3D.desc()
        }

        if (cell instanceof LatLon)
        {
            return LatLon.desc()
        }

        throw new IllegalArgumentException(MessageFormat.format("Unsupported type {0} found in {1}", cell.getClass().getName(), section))
    }
}
