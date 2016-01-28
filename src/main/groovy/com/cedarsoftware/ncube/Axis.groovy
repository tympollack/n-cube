package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.exception.AxisOverlapException
import com.cedarsoftware.ncube.exception.CoordinateNotFoundException
import com.cedarsoftware.ncube.proximity.LatLon
import com.cedarsoftware.ncube.proximity.Point3D
import com.cedarsoftware.ncube.util.LongHashSet
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.Converter
import com.cedarsoftware.util.MapUtilities
import com.cedarsoftware.util.StringUtilities
import com.cedarsoftware.util.io.JsonReader
import groovy.transform.CompileStatic

import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Matcher
/**
 * Implements an Axis of an NCube. When modeling, think of an axis as a 'condition'
 * or decision point.  An input variable (like 'X:1' in a cartesian coordinate system)
 * is passed in, and the Axis's job is to locate the column that best matches the input,
 * as quickly as possible.
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
class Axis
{
    public static final int SORTED = 0
    public static final int DISPLAY = 1
    private static final AtomicLong baseAxisIdForTesting = new AtomicLong(1)
    private static final long BASE_AXIS_ID = 1000000000000L

    private String name
    private AxisType type
    private AxisValueType valueType
    final List<Column> columns = new ArrayList<>()
    protected Map<String, Object> metaProps = null
    private Column defaultCol
    protected final long id
    private long colIdBase = 0
    private int preferredOrder = SORTED
    private boolean fireAll = true
    private final boolean isRef
    private boolean needsIndexed = true

    // used to get O(1) on SET axis for the discrete elements in the Set
    final transient Map<Comparable, Column> discreteToCol = new TreeMap<>()

    // used to get O(1) on Ranges for SET access
    final transient List<RangeToColumn> rangeToCol = new ArrayList<>()

    // used to get O(1) access to columns by ID
    protected final transient Map<Long, Column> idToCol = new HashMap<>()

    // used to get O(1) access to columns by rule-name
    private final transient Map<String, Column> colNameToCol = new CaseInsensitiveMap<>()

    /**
     * Implement to provide data for this Axis
     */
    interface AxisRefProvider
    {
        void load(Axis axis)
    }

    // for testing
    protected Axis(String name, AxisType type, AxisValueType valueType, boolean hasDefault)
    {
        this(name, type, valueType, hasDefault, SORTED)
    }

    // for testing
    protected Axis(String name, AxisType type, AxisValueType valueType, boolean hasDefault, int order)
    {
        this(name, type, valueType, hasDefault, order, baseAxisIdForTesting.getAndIncrement())
    }

    /**
     * Use this constructor for non-rule Axes.
     * @param name String Axis name
     * @param type AxisType (DISCRETE, RANGE, SET, NEAREST, RULE)
     * @param valueType AxisValueType (STRING, LONG, BIG_DECIMAL, DOUBLE, DATE, EXPRESSION, COMPARABLE)
     * @param hasDefault boolean set to true to have a Default column that will match when no other columns match
     * @param order SORTED or DISPLAY (insertion order)
     * @param id long id of Axis.  Ask n-cube for max ID, then add 1 to it, and use that.
     */
    Axis(String name, AxisType type, AxisValueType valueType, boolean hasDefault, int order, long id)
    {
        this(name, type, valueType, hasDefault, order, id, true)
    }

    /**
     * Use this constructor for non-rule Axes.
     * @param name String Axis name
     * @param type AxisType (DISCRETE, RANGE, SET, NEAREST, RULE)
     * @param valueType AxisValueType (STRING, LONG, BIG_DECIMAL, DOUBLE, DATE, EXPRESSION, COMPARABLE)
     * @param hasDefault boolean set to true to have a Default column that will match when no other columns match
     * @param order SORTED or DISPLAY (insertion order)
     * @param id long id of Axis.  Ask n-cube for max ID, then add 1 to it, and use that.
     * @param fireAll boolean if set to true, all conditions that evaluate to true will have their associated
     * statements executed.  If set to false, the first condition that evaluates to true will be executed, but
     * then no conditions on the RULE axis will be evaluated.
     */
    Axis(String name, AxisType type, AxisValueType valueType, boolean hasDefault, int order, long id, boolean fireAll)
    {
        isRef = false
        this.id = id
        this.name = name
        this.type = type
        preferredOrder = order
        this.fireAll = fireAll
        if (type == AxisType.RULE)
        {
            preferredOrder = DISPLAY
            this.valueType = AxisValueType.EXPRESSION
        }
        else if (type == AxisType.NEAREST)
        {
            preferredOrder = SORTED
            this.valueType = valueType
            defaultCol = null
        }
        else
        {
            this.valueType = valueType
        }

        if (hasDefault && type != AxisType.NEAREST)
        {
            defaultCol = new Column(null, getDefaultColId())
            defaultCol.setDisplayOrder(Integer.MAX_VALUE)  // Always at the end
            columns.add(defaultCol)
            idToCol[defaultCol.id] = defaultCol
        }
    }

    /**
     * Use this constructor to create a 'reference' axis.  This allows a single MASTER DATA axis to be referenced
     * by many other axes without repeating the columnar data.
     * @param name String Axis name
     * @param id long id of Axis.  Ask n-cube for max ID, then add 1 to it, and use that.
     * @param axisRefProvider implementer is expected to load(this), e.g. load this axis completely, setting
     * all fields, etc.
     */
    Axis(String name, long id, AxisRefProvider axisRefProvider)
    {
        this.name = name
        this.id = id
        isRef = true

        // TODO: May need to determine if Axis has been initialized.
        // TODO: Call load outside of constructor?
        // Finish construction via the provider.
        axisRefProvider.load(this)

        // Verify that the axis is indeed valid
        if (!AxisType.values().contains(type))
        {
            throw new IllegalStateException('AxisType not set, axis: ' + name)
        }

        if (!AxisValueType.values().contains(valueType))
        {
            throw new IllegalStateException('AxisValueType not set, axis: ' + name)
        }

        if (preferredOrder != DISPLAY && preferredOrder != SORTED)
        {
            throw new IllegalStateException('preferred order not set, axis: ' + name)
        }
    }

    /**
     * @return boolean true if this Axis is a reference to another axis, not a 'real' axis.  A reference axis
     * cannot be modified.
     */
    boolean isReference()
    {
        return isRef
    }

    protected long getNextColId()
    {
        long baseAxisId = id * BASE_AXIS_ID
        while (idToCol.containsKey(++colIdBase + baseAxisId))
        {
        }
        return baseAxisId + colIdBase
    }

    protected long getDefaultColId()
    {
        return id * BASE_AXIS_ID + Integer.MAX_VALUE
    }

    /**
     * @return Map (case insensitive keys) containing meta (additional) properties for the n-cube.
     */
    Map<String, Object> getMetaProperties()
    {
        Map ret = metaProps == null ? new CaseInsensitiveMap() : metaProps
        return Collections.unmodifiableMap(ret)
    }

    /**
     * Set (add / overwrite) a Meta Property associated to this axis.
     * @param key String key name of meta property
     * @param value Object value to associate to key
     * @return prior value associated to key or null if none was associated prior
     */
    Object setMetaProperty(String key, Object value)
    {
        if (metaProps == null)
        {
            metaProps = new CaseInsensitiveMap<>()
        }
        return metaProps[key] = value
    }

    /**
     * Fetch the value associated to the passed in Key from the MetaProperties (if any exist).  If
     * none exist, null is returned.
     */
    Object getMetaProperty(String key)
    {
        if (metaProps == null)
        {
            return null
        }
        return metaProps[key]
    }

    /**
     * Remove a meta-property entry
     */
    Object removeMetaProperty(String key)
    {
        if (metaProps == null)
        {
            return null
        }
        return metaProps.remove(key)
    }

    /**
     * Add a Map of meta properties all at once.
     * @param allAtOnce Map of meta properties to add
     */
    void addMetaProperties(Map<String, Object> allAtOnce)
    {
        if (metaProps == null)
        {
            metaProps = new CaseInsensitiveMap<>()
        }
        metaProps.putAll(allAtOnce)
    }

    /**
     * Remove all meta properties associated to this Axis.
     */
    void clearMetaProperties()
    {
        if (metaProps != null)
        {
            metaProps.clear()
            metaProps = null
        }
    }

    /**
     * @return boolean true if this RULE axis is set to fire all conditions that evaluate to true, false otherwise.
     */
    boolean isFireAll()
    {
        return fireAll
    }

    /**
     * Set the 'fire-all' mode - true to have all conditions that evaluate to true have the associated cell execute,
     * false for only the first condition that is true to have it's associated cell execute.
     */
    void setFireAll(boolean fireAll)
    {
        this.fireAll = fireAll
    }

    /**
     * Use Column id to retrieve column (hash map lookup), O(1)
     */
    Column getColumnById(long colId)
    {
        return idToCol[colId]
    }

    protected void reIndex()
    {
        // Need to do per-index
        rangeToCol.clear()
        discreteToCol.clear()
        idToCol.clear()
        colNameToCol.clear()
        for (Column column : columns)
        {
            indexColumn(column)
        }
    }

    private void indexColumn(Column column)
    {
        // 1: Index columns by ID
        idToCol[column.id] = column

        // 2: Index columns by name (if they have one) - held in CaseInsensitiveMap
        String colName = column.getColumnName()
        if (StringUtilities.hasContent(colName))
        {
            if (colNameToCol.containsKey(colName))
            {
                throw new IllegalArgumentException("Column with name '" + colName + "' alread exists on axis: " + name)
            }
            colNameToCol[colName] = column
        }

        if (type == AxisType.DISCRETE)
        {
            if (column.value != null)
            {
                discreteToCol[standardizeColumnValue(column.value)] = column
            }
        }
        else if (type == AxisType.SET)
        {
            RangeSet set = (RangeSet)column.getValue()
            if (set == null)
            {   // Default column being processed
                return
            }

            final int len = set.size()
            for (int i=0; i < len; i++)
            {
                Comparable elem = set.get(i)
                if (elem instanceof Range)
                {
                    Range range = (Range) elem
                    RangeToColumn rc = new RangeToColumn(range, column)
                    int where = Collections.binarySearch(rangeToCol, rc)
                    if (where < 0)
                    {
                        where = Math.abs(where + 1)
                    }
                    rangeToCol.add(where, rc)
                }
                else
                {
                    discreteToCol[elem] = column
                }
            }
        }
    }

    /**
     * @return long id of this Axis
     */
    long getId()
    {
        return id
    }

    /**
     * @return String version of axis properties.  Useful for SHA-1 computations.
     */
    String getAxisPropString()
    {
        StringBuilder s = new StringBuilder()
        s.append("Axis: ")
        s.append(name)
        s.append(" [")
        s.append(type)
        s.append(", ")
        s.append(valueType)
        s.append(hasDefaultColumn() ? ", default-column" : ", no-default-column")
        s.append(Axis.SORTED == preferredOrder ? ", sorted" : ", unsorted")
        s.append(']')
        return s.toString()
    }

    String toString()
    {
        StringBuilder s = new StringBuilder(getAxisPropString())
        if (!MapUtilities.isEmpty(metaProps))
        {
            s.append("\n")
            s.append("  metaProps: " + metaProps)
        }

        return s.toString()
    }

    /**
     * @return String Axis name.  The name is the value that String keys match (bind to) on the input.  Although
     * the case of the name is maintained, it is compared case-insensitively.
     */
    String getName()
    {
        return name
    }

    protected void setName(String name)
    {
        this.name = name
    }

    /**
     * @return AxisType of this Axis, which is one of: DISCRETE, RANGE, SET, NEAREST, RULE
     */
    AxisType getType()
    {
        return type
    }

    /**
     * @return AxisValueType of this Axis, which is one of: STRING, LONG, BIG_DECIMAL, DOUBLE, DATE, EXPRESSION, COMPARABLE
     */
    AxisValueType getValueType()
    {
        return valueType
    }

    /**
     * @return List<Column> representing all of the Columns on this list.  This is a copy, so operations
     * on the List will not affect the Axis columns.  However, the Column instances inside the List are
     * not 'deep copied' so no modifications to them should be made, as it would violate the internal
     * Map structures maintaining the column indexing. The Columms are in SORTED or DISPLAY order
     * depending on the 'preferredOrder' setting. If you want to obtain the columns more quickly,
     * you can use getColumnsWithoutDefault() - they will always be in sorted order and will not
     * contain the default.
     */
    List<Column> getColumns()
    {
        if (type == AxisType.DISCRETE)
        {   // return 'view' of Columns that matches the desired order (sorted or display)
            List<Column> cols = getColumnsWithoutDefault()
            if (preferredOrder == DISPLAY)
            {
                sortColumnsByDisplayOrder(cols)
            }
            if (defaultCol != null)
            {
                cols.add(defaultCol)
            }
            return cols
        }
        else
        {
            List<Column> cols = new ArrayList<>(columns)
            if (type != AxisType.RULE)
            {
                if (preferredOrder == SORTED)
                {
                    return cols    // Return a copy of the columns, not our internal values list.
                }
                sortColumnsByDisplayOrder(cols)
            }
            return cols
        }
    }

    protected void clear()
    {
        rangeToCol.clear()
        discreteToCol.clear()
        idToCol.clear()
        colNameToCol.clear()
        columns.clear()
    }

    protected List<Column> getColumnsInternal()
    {
        if (type == AxisType.DISCRETE)
        {
            List<Column> cols = new ArrayList<>(size())
            cols.addAll(discreteToCol.values())
            if (defaultCol != null)
            {
                cols.add(defaultCol)
            }
            return cols
        }

        return columns
    }

    /**
     * Given the passed in 'raw' value, get a Column from the passed in value, which entails
     * converting the 'raw' value to the correct type, promoting the value to the appropriate
     * internal value for comparison, and so on.
     * @param value Comparable typically a primitive, but can also be an n-cube Range, RangeSet, CommandCell,
     * or 2D, 3D, or LatLon.
     * @param suggestedId Long suggested column ID.  Can be null, in which case an ID will be generated. If not null,
     * then if the ID < BASE_AXIS_ID, the ID will be used (and shifted by the axis ID * BASE_AXIS_ID), otherwise the
     * ID will be used as-is.
     * @return a Column with the up-promoted value as the column's value, and a unique ID on the column.  If
     * the original value is a Range or RangeSet, the components in the Range or RangeSet are also up-promoted.
     */
    protected Column createColumnFromValue(Comparable value, Long suggestedId)
    {
        Comparable v
        if (value == null)
        {  // Attempting to add Default column to axis
            v = null
        }
        else
        {
            if (type == AxisType.DISCRETE)
            {
                v = standardizeColumnValue(value)
            }
            else if (type == AxisType.RANGE || type == AxisType.SET)
            {
                v = value instanceof String ? convertStringToColumnValue((String) value) : standardizeColumnValue(value)
            }
            else if (type == AxisType.RULE)
            {
                v = value instanceof String ? convertStringToColumnValue((String) value) : value
            }
            else if (type == AxisType.NEAREST)
            {
                v = standardizeColumnValue(value)
            }
            else
            {
                throw new IllegalStateException("New axis type added without complete support.")
            }
        }

        if (suggestedId != null)
        {
            long impliedAxisId = (long) (suggestedId / BASE_AXIS_ID)

            if (impliedAxisId != id ||                  // suggestedID must include matching Axis ID
                idToCol.containsKey(suggestedId))       // suggestedID must not already exist on axis
            {
                return new Column(v, getNextColId())
            }

            return new Column(v, suggestedId)
        }
        else
        {
            return new Column(v, v == null ? getDefaultColId() : getNextColId())
        }
    }

    /**
     * Will throw IllegalArgumentException if passed in value duplicates a value on this axis.
     */
    protected void ensureUnique(Comparable value)
    {
        if (value == null)
        {  // Attempting to add Default column to axis
            if (hasDefaultColumn())
            {
                throw new IllegalArgumentException("Cannot add default column to axis '" + name + "' because it already has a default column.")
            }
            if (type == AxisType.NEAREST)
            {
                throw new IllegalArgumentException("Cannot add default column to NEAREST axis '" + name + "' as it would never be chosen.")
            }
        }
        else
        {
            if (type == AxisType.DISCRETE)
            {
                doesMatchExistingValue(value)
            }
            else if (type == AxisType.RANGE)
            {
                Range range = (Range)value
                if (doesOverlap(range))
                {
                    throw new AxisOverlapException("Passed in Range overlaps existing Range on axis: " + name + ", value: " + value)
                }
            }
            else if (type == AxisType.SET)
            {
                RangeSet set = (RangeSet)value
                if (doesOverlap(set))
                {
                    throw new AxisOverlapException("Passed in RangeSet overlaps existing RangeSet on axis: " + name + ", value: " + value)
                }
            }
            else if (type == AxisType.RULE)
            {
                if (!(value instanceof CommandCell))
                {
                    throw new IllegalArgumentException("Columns for RULE axis must be a CommandCell, axis: " + name + ", value: " + value)
                }
            }
            else if (type == AxisType.NEAREST)
            {
                doesMatchNearestValue(value)
            }
            else
            {
                throw new IllegalStateException("New axis type added without complete support.")
            }
        }
    }

    Column addColumn(Comparable value)
    {
        return addColumn(value, null)
    }

    Column addColumn(Comparable value, String colName)
    {
        return addColumn(value, colName, null)
    }

    Column addColumn(Comparable value, String colName, Long suggestedId)
    {
        final Column column = createColumnFromValue(value, suggestedId)
        if (StringUtilities.hasContent(colName))
        {
            column.setColumnName(colName)
        }
        addColumnInternal(column)
        return column
    }

    protected Column addColumnInternal(Column column)
    {
        if (isRef)
        {
            throw new IllegalStateException('You cannot add columns to a reference Axis, axis: ' + name)
        }
        ensureUnique(column.getValue())

        if (column.getValue() == null)
        {
            column.setId(getDefaultColId())    // Safety check - should never happen
            defaultCol = column
        }

        // TODO: Discrete column - do not add to columns
        // New columns are always added at the end in terms of displayOrder, but internally they are added
        // in the correct sort order location.  The sort order of the list is required because binary searches
        // are done against it.
        int dispOrder = hasDefaultColumn() ? size() - 1 : size()
        column.setDisplayOrder(column.getValue() == null ? Integer.MAX_VALUE : dispOrder)
        if (type == AxisType.RULE)
        {   // Rule columns are added in 'occurrence' order
            if (column != defaultCol && hasDefaultColumn())
            {   // Insert right before default column at the end
                columns.add(Math.max(columns.size() - 1, 0), column)
            }
            else
            {
                columns.add(column)
            }
        }
        else //if (type != AxisType.DISCRETE)
        {
            int where = Collections.binarySearch(columns, column.getValue())
            if (where < 0)
            {
                where = Math.abs(where + 1)
            }
            columns.add(where, column)
        }
        indexColumn(column)
        return column
    }

    /**
     * This method deletes a column from an Axis.  It is intentionally package
     * scoped because there are two parts to deleting a column - this removes
     * the column from the Axis, the other part removes the Cells that reference
     * the column (that is within NCube).
     * @param value Comparable value used to identify the column to delete.
     * @return Column that was deleted, or null if no column would be deleted.
     */
    protected Column deleteColumn(Comparable value)
    {
        Column col = findColumn(value)
        if (col == null)
        {	// Not found.
            return null
        }

        return deleteColumnById(col.id)
    }

    protected Column deleteColumnById(long colId)
    {
        if (isRef)
        {
            throw new IllegalStateException('You cannot delete columns from a reference Axis, axis: ' + name)
        }

        Column col = idToCol[colId]
        if (col == null)
        {
            return null
        }

        // TODO: skip if DISCRETE
        columns.remove(col)
        if (col.isDefault())
        {
            defaultCol = null
        }

        // Remove column from scaffolding
        removeColumnFromIndex(col)
        return col
    }

    private void removeColumnFromIndex(Column col)
    {
        // Remove from col id to column map
        idToCol.remove(col.id)
        colNameToCol.remove(col.getColumnName())

        if (type == AxisType.DISCRETE)
        {
            if (col.value != null)
            {
                discreteToCol.remove(standardizeColumnValue(col.value))
            }
        }
        else if (type == AxisType.RANGE)
        {

        }
        else if (type == AxisType.SET)
        {
            if (discreteToCol != null)
            {
                Iterator<Column> j = discreteToCol.values().iterator()
                while (j.hasNext())
                {
                    Column column = j.next()
                    if (col.equals(column))
                    {   // Multiple discrete values may have pointed to the passed in column, so we must loop through all
                        j.remove()
                    }
                }
            }

            if (rangeToCol != null)
            {
                Iterator<RangeToColumn> i = rangeToCol.iterator()
                while (i.hasNext())
                {
                    Axis.RangeToColumn rangeToColumn = i.next()
                    if (rangeToColumn.column.equals(col))
                    {   // Multiple ranges may have pointed to the passed in column, so we must loop through all
                        i.remove()
                    }
                }
            }
        }
        else if (type == AxisType.NEAREST)
        {

        }
        else if (type == AxisType.RULE)
        {

        }
        else
        {
            throw new IllegalStateException("Unsupported axis type (" + type + ") for axis '" + name + "', trying to remove column from internal index")
        }
    }

    /**
     * Update (change) the value of an existing column.  This entails not only
     * changing the value, but resorting the axis's columns (columns are always in
     * sorted order for quick retrieval).  The display order of the columns is not
     * rebuilt, because the column is changed in-place (e.g., changing Mon to Monday
     * does not change it's display order.)
     * @param colId long Column ID to update
     * @param value 'raw' value to set into the new column (will be up-promoted).
     */
    void updateColumn(long colId, Comparable value)
    {
        if (isRef)
        {
            throw new IllegalStateException('You cannot update columns on a reference Axis, axis: ' + name)
        }

        Column col = idToCol[colId]
        deleteColumnById(colId)
        Column newCol = createColumnFromValue(value, null)
        ensureUnique(newCol.getValue())
        newCol.setId(colId)
        newCol.setDisplayOrder(col.getDisplayOrder())
        String colName = col.getColumnName()
        if (StringUtilities.hasContent(colName))
        {
            newCol.setColumnName(col.getColumnName())
        }

        // Updated column is added in the same 'displayOrder' location.  For example, the months are a
        // displayOrder Axis type.  Updating 'Jun' to 'June' will use the same displayOrder value.
        // However, the columns are stored internally in sorted order (for fast lookup), so we need to
        // find where it should go (updating Fune to June, for example (fixing a misspelling), will
        // result in the column being sorted to a different location (while maintaining its display
        // order, because displayOrder is stored on the column).
        int where = Collections.binarySearch(columns, newCol.getValue())
        if (where < 0)
        {
            where = Math.abs(where + 1)
        }
        // TODO: skip if discrete
        columns.add(where, newCol)
        indexColumn(newCol)
    }

    /**
     * Update columns on this Axis, from the passed in Axis.  Columns that exist on both axes,
     * will have their values updated.  Columns that exist on this axis, but not exist in the
     * 'newCols' will be deleted (and returned as a Set of deleted Columns).  Columns that
     * exist in newCols but not on this are new columns.
     *
     * NOTE: The columns field within the newCols axis are NOT in sorted order as they normally are
     * within the Axis class.  Instead, they are in display order (this order is typically set forth by a UI).
     * Axis is used as a Data-Transfer-Object (DTO) in this case, not the normal way it is typically used
     * where the columns would always be sorted for quick access.
     */
    Set<Long> updateColumns(Collection<Column> newCols)
    {
        if (isRef)
        {
            throw new IllegalStateException('You cannot update columns on a reference Axis, axis: ' + name)
        }

        Set<Long> colsToDelete = new LongHashSet()
        Map<Long, Column> newColumnMap = new LinkedHashMap<>()

        // Step 1. Map all columns coming in from "DTO" Axis by ID
        for (Column col : newCols)
        {
            Column newColumn = createColumnFromValue(col.getValue(), null)
            Map<String, Object> metaProperties = col.getMetaProperties()
            for (Map.Entry<String, Object> entry : metaProperties.entrySet())
            {
                newColumn.setMetaProperty(entry.getKey(), entry.getValue())
            }

            newColumnMap[col.id] = newColumn
        }

        // Step 2.  Build list of columns that no longer exist (add to deleted list)
        // AND update existing columns that match by ID columns from the passed in DTO.
        List<Column> tempCol = new ArrayList<>(getColumnsWithoutDefault())
        Iterator<Column> i = tempCol.iterator()

        while (i.hasNext())
        {
            Column col = i.next()
            if (newColumnMap.containsKey(col.id))
            {   // Update case - matches existing column
                Column newCol = newColumnMap[col.id]
                col.setValue(newCol.getValue())

                Map<String, Object> metaProperties = newCol.getMetaProperties()
                for (Map.Entry<String, Object> entry : metaProperties.entrySet())
                {
                    col.setMetaProperty(entry.getKey(), entry.getValue())
                }
            }
            else
            {   // Delete case - existing column id no longer found
                colsToDelete.add(col.id)
                i.remove()
            }
        }

        clear()
        for (Column column : tempCol)
        {
            addColumnInternal(column)
        }

        Map<Long, Column> realColumnMap = new LinkedHashMap<>()

        for (Column column : columns)
        {
            realColumnMap[column.id] = column
        }
        int displayOrder = 0

        // Step 4. Add new columns (they exist in the passed in Axis, but not in this Axis) and
        // set display order to match the columns coming in from the DTO axis (argument).
        for (Column col : newCols)
        {
            if (col.getValue() == null)
            {   // Skip Default column
                continue
            }
            long realId = col.id
            if (col.id < 0)
            {   // Add case - negative id, add new column to 'columns' List.
                Column newCol = addColumnInternal(newColumnMap[col.id])
                realId = newCol.id
                realColumnMap[realId] = newCol
            }
            Column realColumn = realColumnMap[realId]
            if (realColumn == null)
            {
                throw new IllegalArgumentException("Columns to be added should have negative ID values.")
            }
            realColumn.setDisplayOrder(displayOrder++)
        }

        if (type == AxisType.RULE)
        {   // required because RULE columns are stored in execution order
            sortColumnsByDisplayOrder(columns)
        }

        // Put default column back if it was already there.
        if (hasDefaultColumn())
        {
            columns.add(defaultCol)
        }

        // index
        reIndex()
        return colsToDelete
    }

//    private Set<Long> mergeDiscreteColumns(final Axis axisTomerge)
//    {
//        Set<Long> colsToDelete = new LongHashSet()
//        colsToDelete.addAll(idToCol.keySet())
//        Map<Long, Column> newColumnMap = new LinkedHashMap<>()
//        int displayOrder = 0
//
//        // Step 1. Map all columns coming in from "DTO" Axis by ID
//        for (Column col : axisTomerge.getColumns())
//        {
//            if (idToCol.containsKey(col.id))
//            {   // Update
//                Column column = idToCol[col.id]
//                column.setValue(col.getValue())
//
//                Map<String, Object> metaProperties = col.getMetaProperties()
//                for (Map.Entry<String, Object> entry : metaProperties.entrySet())
//                {
//                    column.setMetaProperty(entry.getKey(), entry.getValue())
//                }
//
//                colsToDelete.remove(col.id) // remove from delete list
//            }
//            else
//            {
//
//            }
//            column.setDisplayOrder(displayOrder++)
//        }
//
//
//        return colsToDelete
//    }

    /**
     * Sorted this way to allow for CopyOnWriteArrayList or regular ArrayLists to be sorted.
     * CopyOnWriteArrayList does not support iterator operations .set() for example, which
     * would be called by Collections.sort()
     * @param cols List of Columns to sort
     */
    private static void sortColumns(List cols, Comparator comparator)
    {
        Object[] colArray = cols.toArray()
        Arrays.sort(colArray, comparator)

        final int len = colArray.length
        for (int i=0; i < len; i++)
        {
            cols.set(i, colArray[i])
        }
    }

    // Take the passed in value, and prepare it to be allowed on a given axis type.
    Comparable convertStringToColumnValue(String value)
    {
        if (StringUtilities.isEmpty(value))
        {
            throw new IllegalArgumentException("Column value cannot be empty, axis: " + name)
        }
        value = value.trim()

        switch(type)
        {
            case AxisType.DISCRETE:
                return standardizeColumnValue(value)

            case AxisType.RANGE:
                return standardizeColumnValue(parseRange(value))

            case AxisType.SET:
                try
                {   // input we always be comma delimited list of items and ranges (we add the final array brackets)
                    value = '[' + value + ']'
                    Map options = new HashMap()
                    options[JsonReader.USE_MAPS] = true
                    Object[] list = (Object[]) JsonReader.jsonToJava(value, options)
                    final RangeSet set = new RangeSet()

                    for (Object item : list)
                    {
                        if (item instanceof Object[])
                        {   // Convert to Range
                            Object[] subList = (Object[]) item
                            if (subList.length != 2)
                            {
                                throw new IllegalArgumentException("Range inside set must have exactly two (2) entries.")
                            }
                            Range range = new Range((Comparable)subList[0], (Comparable)subList[1])
                            set.add(range)
                        }
                        else if (item == null)
                        {
                            throw new IllegalArgumentException("Set cannot have null value inside.")
                        }
                        else
                        {
                            set.add((Comparable)item)
                        }
                    }
                    if (set.size() > 0)
                    {
                        return standardizeColumnValue(set)
                    }
                    throw new IllegalArgumentException("Value: " + value + " cannot be parsed as a Set. Must have at least one element within the set, axis: " + name)
                }
                catch (IllegalArgumentException e)
                {
                    throw e
                }
                catch (Exception e)
                {
                    throw new IllegalArgumentException("Value: " + value + " cannot be parsed as a Set.  Use v1, v2, [low, high], v3, ... , axis: " + name, e)
                }

            case AxisType.NEAREST:
                return standardizeColumnValue(value)

            case AxisType.RULE:
                return createExpressionFromValue(value)

            default:
                throw new IllegalStateException("Unsupported axis type (" + type + ") for axis '" + name + "', trying to parse value: " + value)
        }
    }

    private GroovyExpression createExpressionFromValue(String value)
    {
        value = value.trim()
        boolean isUrl = false
        boolean cache = false
        boolean madeChange

        while (true) {
            madeChange = false
            // Place in loop to allow url|cache|http://... OR cache|url|http://...   OR url|http://  OR  cache|http://...
            if (value.startsWith("url|"))
            {
                isUrl = true
                value = value.substring(4)
                madeChange = true
            }

            if (value.startsWith("cache|"))
            {
                cache = true
                value = value.substring(6)
                madeChange = true
            }

            if (!madeChange)
            {
                break
            }
        }

        if (isUrl)
        {
            return new GroovyExpression(null, value, cache)
        }
        return new GroovyExpression(value, null, cache)
    }

    private Range parseRange(String value)
    {
        value = value.trim()
        if (value.startsWith("[") && value.endsWith("]"))
        {   // Remove surrounding brackets
            value = value.substring(1, value.length() - 1)
        }
        Matcher matcher = Regexes.rangePattern.matcher(value)
        if (matcher.matches())
        {
            String one = matcher.group(1)
            String two = matcher.group(2)
            return new Range(trimQuotes(one), trimQuotes(two))
        }
        else
        {
            throw new IllegalArgumentException("Value (" + value + ") cannot be parsed as a Range.  Use [value1, value2], axis: " + name)
        }
    }

    private static String trimQuotes(String value)
    {
        if (value.startsWith("\"") && value.endsWith("\""))
        {
            return value.substring(1, value.length() - 1)
        }
        if (value.startsWith("'") && value.endsWith("'"))
        {
            return value.substring(1, value.length() - 1)
        }
        return value.trim()
    }

    int getColumnOrder()
    {
        return preferredOrder
    }

    void setColumnOrder(int order)
    {
        preferredOrder = order
    }

    /**
     * @param cols List of Column instances to be sorted.
     */
    private static void sortColumnsByDisplayOrder(List<Column> cols)
    {
        sortColumns(cols, new Comparator<Column>()
        {
            int compare(Column c1, Column c2)
            {
                return c1.getDisplayOrder() - c2.getDisplayOrder()
            }
        })
    }

    /**
     * @return int total number of columns on this axis.  Default column (if present) counts as 1.
     */
    int size()
    {
        return idToCol.size()
    }

    /**
     * This method takes the input value (could be Number, String, Range, etc.)
     * and 'promotes' it to the same type as the Axis.
     * @param value Comparable value to promote (to highest of it's type [e.g., short to long])
     * @return Comparable promoted value.  For example, a Long would be returned a
     * Byte value were passed in, and this was a LONG axis.
     */
    Comparable standardizeColumnValue(Comparable value)
    {
        if (value == null)
        {
            return null
        }

        if (type == AxisType.DISCRETE)
        {
            return promoteValue(valueType, value)
        }
        else if (type == AxisType.RANGE)
        {
            if (!(value instanceof Range))
            {
                throw new IllegalArgumentException("Must only add Range values to " + type + " axis '" + name + "' - attempted to add: " + value.getClass().getName())
            }
            return promoteRange(new Range(((Range)value).low, ((Range)value).high))
        }
        else if (type == AxisType.SET)
        {
            if (!(value instanceof RangeSet))
            {
                throw new IllegalArgumentException("Must only add RangeSet values to " + type + " axis '" + name + "' - attempted to add: " + value.getClass().getName())
            }
            RangeSet set = new RangeSet()
            Iterator<Comparable> i = ((RangeSet)value).iterator()
            while (i.hasNext())
            {
                Comparable val = i.next()
                if (val instanceof Range)
                {
                    promoteRange((Range)val)
                }
                else
                {
                    val = promoteValue(valueType, val)
                }
                set.add(val)
            }
            return set
        }
        else if (type == AxisType.NEAREST)
        {	// Standardizing a NEAREST axis entails ensuring conformity amongst values (must all be Point2D, LatLon, Date, Long, String, etc.)
            value = promoteValue(valueType, value)
            if (!getColumnsWithoutDefault().isEmpty())
            {
                Column col = columns[0]
                if (value.getClass() != col.getValue().getClass())
                {
                    throw new IllegalArgumentException("Value '" + value.getClass().getName() + "' cannot be added to axis '" + name + "' where the values are of type: " + col.getValue().getClass().getName())
                }
            }
            return value	// First value added does not need to be checked
        }
        else
        {
            throw new IllegalArgumentException("New AxisType added '" + type + "' but code support for it is not there.")
        }
    }

    /**
     * Promote passed in range's low and high values to the largest
     * data type of their 'kinds' (e.g., byte to long).
     * @param range Range to be promoted
     * @return Range with the low and high values promoted and in proper order (low < high)
     */
    private Range promoteRange(Range range)
    {
        final Comparable low = promoteValue(valueType, range.low)
        final Comparable high = promoteValue(valueType, range.high)
        ensureOrder(range, low, high)
        return range
    }

    private static void ensureOrder(Range range, final Comparable lo, final Comparable hi)
    {
        if (lo.compareTo(hi) > 0)
        {
            range.low = hi
            range.high = lo
        }
        else
        {
            range.low = lo
            range.high = hi
        }
    }

    /**
     * Convert passed in value to a similar value of the highest type.  If the
     * valueType is not the same basic type as the value passed in, intelligent
     * conversions will happen, and the result will be of the requested type.
     *
     * An intelligent conversion example - String to date, it will parse the String
     * attempting to convert it to a date.  Or a String to a long, it will try to
     * parse the String as a long.  Long to String, it will .toString() the long,
     * and so on.
     * @return promoted value, or the same value if no promotion occurs.
     */
    static Comparable promoteValue(AxisValueType srcValueType, Comparable value)
    {
        switch(srcValueType)
        {
            case AxisValueType.STRING:
                return (String) Converter.convert(value, String.class)
            case AxisValueType.LONG:
                return (Long) Converter.convert(value, Long.class)
            case AxisValueType.BIG_DECIMAL:
                return (BigDecimal) Converter.convert(value, BigDecimal.class)
            case AxisValueType.DOUBLE:
                return (Double) Converter.convert(value, Double.class)
            case AxisValueType.DATE:
                return (Date) Converter.convert(value, Date.class)
            case AxisValueType.COMPARABLE:
                if (value instanceof String)
                {
                    Matcher m = Regexes.valid2Doubles.matcher((String) value)
                    if (m.matches())
                    {   // No way to determine if it was supposed to be a Point2D. Specify as JSON for Point2D
                        return new LatLon((Double)Converter.convert(m.group(1), double.class), (Double)Converter.convert(m.group(2), double.class))
                    }

                    m = Regexes.valid3Doubles.matcher((String) value)
                    if (m.matches())
                    {
                        return new Point3D((Double)Converter.convert(m.group(1), double.class), (Double)Converter.convert(m.group(2), double.class), (Double)Converter.convert(m.group(3), double.class))
                    }

                    try
                    {   // Try as JSON
                        return (Comparable) JsonReader.jsonToJava((String) value)
                    }
                    catch (Exception ignored)
                    {
                        return value
                    }
                }
                return value
            case AxisValueType.EXPRESSION:
                return value
            default:
                throw new IllegalArgumentException("AxisValueType '" + srcValueType + "' added but no code to support it.")
        }
    }

    boolean hasDefaultColumn()
    {
        return defaultCol != null
    }

    /**
     * @param value to test against this Axis
     * @return boolean true if the value will be found along the axis, false
     * if the value does not match anything along the axis.
     */
    boolean contains(Comparable value)
    {
        try
        {
            return findColumn(value) != null
        }
        catch (Exception ignored)
        {
            return false
        }
    }

    Column getDefaultColumn()
    {
        return defaultCol
    }

    protected List<Column> getRuleColumnsStartingAt(String ruleName)
    {
        if (StringUtilities.isEmpty(ruleName))
        {   // Since no rule name specified, all rule columns are returned to have their conditions evaluated.
            return getColumns()
        }

        List<Column> cols = new ArrayList<>()
        Column firstRule = findColumn(ruleName)
        if (firstRule == null)
        {   // A name was specified for a rule, but did not match any rule names and there is no default column.
            throw new CoordinateNotFoundException("Rule named '" + ruleName + "' matches no column names on the rule axis '" + name + "', and there is no default column.")
        }
        else if (firstRule == defaultCol)
        {   // Matched no names, but there is a default column
            cols.add(defaultCol)
            return cols
        }

        int pos = firstRule.getDisplayOrder()
        final List<Column> allColumns = getColumns()
        final int len = allColumns.size()

        for (int i=pos; i < len; i++)
        {
            cols.add(allColumns[i])
        }
        return cols
    }

    /**
     * Locate the column (AvisValue) along an axis.
     * @param value Comparable - A value that can be checked against the axis
     * @return Column that 'matches' the passed in value, or null if no column
     * found.  'Matches' because matches depends on AxisType.
     */
    Column findColumn(Comparable value)
    {
        if (value == null)
        {   // By returning defaultCol, this lets null match it if there is one, or null if there is none.
            return defaultCol
        }

        if (value instanceof Range)
        {   // Linearly locate - used when finding by column during delta processing.
            if (type != AxisType.RANGE)
            {
                throw new IllegalArgumentException("Attempt to search non-Range axis to match a Range")
            }
            Range thatRange = (Range) value
            for (Column column : columns)
            {
                Range thisRange = (Range)column.getValue()
                if (thisRange.equals(thatRange))
                {
                    return column
                }
            }
            return null
        }

        if (value instanceof RangeSet)
        {   // Linearly locate - used when finding by column during delta processing.
            if (type != AxisType.SET)
            {
                throw new IllegalArgumentException("Attempt to search non-Set axis to match a Set")
            }
            RangeSet thatSet = (RangeSet) value
            for (Column column : columns)
            {
                RangeSet thisSet = (RangeSet)column.getValue()
                if (thisSet.equals(thatSet))
                {
                    return column
                }
            }
            return null
        }

        final Comparable promotedValue = promoteValue(valueType, value)
        int pos

        if (type == AxisType.DISCRETE)
        {
            Column colToFind = discreteToCol[promotedValue]
            return colToFind == null ? defaultCol : colToFind
        }
        else if (type == AxisType.RANGE)
        {	// DISCRETE and RANGE axis searched in O(Log n) time using a binary search
            pos = binarySearchAxis(promotedValue)
        }
        else if (type == AxisType.SET)
        {	// The SET axis searched in O(Log n)
            return findOnSetAxis(promotedValue)
        }
        else if (type == AxisType.NEAREST)
        {   // The NEAREST axis type must be searched linearly O(n)
            pos = findNearest(promotedValue)
        }
        else if (type == AxisType.RULE)
        {
            if (promotedValue instanceof Long)
            {
                return idToCol[(Long)promotedValue]
            }
            else if (promotedValue instanceof String)
            {
                return findColumnByName((String)promotedValue)
            }
            else
            {
                throw new IllegalArgumentException("A column on a rule axis can only be located by the 'name' attribute (String) or ID (long), axis: " + name + ", value: " + promotedValue)
            }
        }
        else
        {
            throw new IllegalArgumentException("Axis type '" + type + "' added but no code supporting it.")
        }

        if (pos >= 0)
        {
            return columns[pos]
        }

        return defaultCol
    }

    /**
     * Locate a column on an axis using the 'name' meta property.  If the value passed in matches no names, then
     * null will be returned.
     * Note: This is a case-insensitive match.
     */
    Column findColumnByName(String colName)
    {
        Column col = colNameToCol[colName]
        if (col != null)
        {
            return col
        }
        return null
    }

    private Column findOnSetAxis(final Comparable promotedValue)
    {
        if (discreteToCol.containsKey(promotedValue))
        {
            return discreteToCol[promotedValue]
        }

        int pos = binarySearchRanges(promotedValue)
        if (pos >= 0)
        {
            return rangeToCol[pos].column
        }

        return defaultCol
    }

    private int binarySearchRanges(final Comparable promotedValue)
    {
        return Collections.binarySearch(rangeToCol, promotedValue, new Comparator()
        {
            int compare(Object r1, Object key)
            {   // key not used as promoteValue, already of type Comparable, is the exact same thing, and already final
                RangeToColumn rc = (RangeToColumn) r1
                Range range = rc.getRange()
                return -1 * range.isWithin(promotedValue)
            }
        })
    }

    private int binarySearchAxis(final Comparable promotedValue)
    {
        List cols = getColumnsWithoutDefault()
        return Collections.binarySearch(cols, promotedValue, new Comparator()
        {
            int compare(Object o1, Object key)
            {   // key not used as promoteValue, already of type Comparable, is the exact same thing, and already final
                Column column = (Column) o1

                if (type == AxisType.DISCRETE)
                {
                    return column.compareTo(promotedValue)
                }
                else if (type == AxisType.RANGE)
                {
                    Range range = (Range)column.getValue()
                    return -1 * range.isWithin(promotedValue)
                }
                else
                {
                    throw new IllegalStateException("Cannot binary search axis type: '" + type + "'")
                }
            }
        })
    }

    private int findNearest(final Comparable promotedValue)
    {
        double min = Double.MAX_VALUE
        int savePos = -1
        int pos = 0

        for (Column column : getColumnsWithoutDefault())
        {
            double d = Proximity.distance(promotedValue, column.getValue())
            if (d < min)
            {	// Record column that set's new minimum record
                min = d
                savePos = pos
            }
            pos++
        }
        return savePos
    }

    /**
     * Ensure that the passed in range does not overlap an existing Range on this
     * 'Range-type' axis.  Test low range limit to see if it is valid.
     * Axis is already a RANGE type before this method is called.
     * @param value Range (value) that is intended to be a new low range limit.
     * @return true if the Range overlaps this axis, false otherwise.
     */
    private boolean doesOverlap(Range value)
    {
        // Start just before where this range would be inserted.
        int where = binarySearchAxis(value.low)
        if (where < 0)
        {
            where = Math.abs(where + 1)
        }
        where = Math.max(0, where - 1)
        int size = getColumnsWithoutDefault().size()

        for (int i = where; i < size; i++)
        {
            Column column = getColumnsWithoutDefault()[i]
            Range range = (Range) column.getValue()
            if (value.overlap(range))
            {
                return true
            }

            if (value.low.compareTo(range.low) <= 0)
            {   // No need to continue, once the passed in low is less or equals to the low of the next column
                break
            }
        }
        return false
    }

    /**
     * Test RangeSet to see if it overlaps any of the existing columns on
     * this cube.  Axis is already a RangeSet type before this method is called.
     * @param value RangeSet (value) to be checked
     * @return true if the RangeSet overlaps this axis, false otherwise.
     */
    private boolean doesOverlap(RangeSet value)
    {
        for (Column column : getColumnsWithoutDefault())
        {
            RangeSet set = (RangeSet) column.getValue()
            if (value.overlap(set))
            {
                return true
            }
        }
        return false
    }

    private void doesMatchExistingValue(Comparable v)
    {
        if (binarySearchAxis(v) >= 0)
        {
            throw new AxisOverlapException("Passed in value '" + v + "' matches a value already on axis '" + name + "'")
        }
    }

    private void doesMatchNearestValue(Comparable v)
    {
        for (Column col : columns)
        {
            Object val = col.getValue()
            if (v.equals(val))
            {
                throw new AxisOverlapException("Passed in value '" + v + "' matches a value already on axis '" + name + "'")
            }
        }
    }

    /**
     * @return List<Column> that contains all Columns on this axis (excluding the Default Column if it exists).  The
     * Columns will be returned in sorted order.  It is a copy of the internal list, therefore operations on the
     * returned List are safe, however, no changes should be made to the contained Column instances, as it would
     * violate internal indexing structures of the Axis.
     */
    List<Column> getColumnsWithoutDefault()
    {
        if (type == AxisType.DISCRETE)
        {
            List<Column> cols = new ArrayList<>(size())
            cols.addAll(discreteToCol.values())
            return cols
        }

        if (columns.size() == 0)
        {
            return columns
        }
        if (hasDefaultColumn())
        {
            if (columns.size() == 1)
            {
                return new ArrayList<>()
            }
            return columns.subList(0, columns.size() - 1)
        }
        return columns
    }

    private static final class RangeToColumn implements Comparable<RangeToColumn>
    {
        private final Range range
        private final Column column

        protected RangeToColumn(Range range, Column column)
        {
            this.range = range
            this.column = column
        }

        private Range getRange()
        {
            return range
        }

        int compareTo(RangeToColumn rc)
        {
            return range.compareTo(rc.range)
        }
    }

    boolean areAxisPropsEqual(Object o)
    {
        if (this == o)
        {
            return true
        }
        if (!(o instanceof Axis))
        {
            return false
        }

        Axis axis = (Axis) o

        if (preferredOrder != axis.preferredOrder)
        {
            return false
        }
        if (defaultCol != null ? !defaultCol.equals(axis.defaultCol) : axis.defaultCol != null)
        {
            return false
        }
        if (!name.equals(axis.name))
        {
            return false
        }
        if (type != axis.type)
        {
            return false
        }
        if (valueType != axis.valueType)
        {
            return false
        }
        return fireAll == axis.fireAll
    }
}
