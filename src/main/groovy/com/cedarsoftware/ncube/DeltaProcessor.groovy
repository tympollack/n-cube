package com.cedarsoftware.ncube

import com.cedarsoftware.ncube.util.LongHashSet
import com.cedarsoftware.util.CaseInsensitiveMap
import com.cedarsoftware.util.CaseInsensitiveSet
import com.cedarsoftware.util.DeepEquals
import com.cedarsoftware.util.StringUtilities
import groovy.transform.CompileStatic
import groovy.transform.PackageScope

/**
 * This class represents any cell that needs to return content from a URL.
 * For example, String or Binary content.
 *
 * @author John DeRegnaucourt (jdereg@gmail.com)
 *         <br/>
 *         Copyright (c) Cedar Software LLC
 *         <br/><br/>
 *         Licensed under the Apache License, Version 2.0 (the "License")
 *         you may not use this file except in compliance with the License.
 *         You may obtain a copy of the License at
 *         <br/><br/>
 *         http://www.apache.org/licenses/LICENSE-2.0
 *         <br/><br/>
 *         Unless required by applicable law or agreed to in writing, software
 *         distributed under the License is distributed on an "AS IS" BASIS,
 *         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *         See the License for the specific language governing permissions and
 *         limitations under the License.
 */
@CompileStatic
class DeltaProcessor
{
    public static final String DELTA_CELLS = "~cell-deltas~"
    public static final String DELTA_AXES_COLUMNS = "~col-deltas~"
    public static final String DELTA_COLUMN_ADD = "~col-add~"
    public static final String DELTA_COLUMN_REMOVE = "~col-del~"
    public static final String DELTA_COLUMN_CHANGE = "~col-upd~"
    public static final String DELTA_CELL_REMOVE = "~rem-cell~"

    /**
     * Fetch the difference between this cube and the passed in cube.  The two cubes must have the same number of axes
     * with the same names.  If those conditions are met, then this method will return a Map with keys for each delta
     * type.
     *
     * The key DELTA_CELLS, will have an associated value that is a Map<Set<Long>, T> which are the cell contents
     * that are different.  These cell differences when applied to 'this' will result in this cube's cells matching
     * the passed in 'other'. If the value is NCUBE.REMOVE_CELL, then that indicates a cell that needs to be removed.
     * All other cell values are actual cell value changes.
     *
     * The key DELTA_AXES_COLUMNS, contains the column differences.  The value associated to this key is a Map, that
     * maps axis name (case-insensitively) to a Map where the key is a column and the associated value is
     * either the 'true' (new) or false (if it should be removed).
     *
     * In the future, meta-property differences may be reported.
     *
     * @param other NCube to compare to this ncube.
     * @return Map containing the differences as described above.  If different number of axes, or different axis names,
     * then null is returned.
     */
    static <T> Map<String, Object> getDelta(NCube<T> thisCube, NCube<T> other)
    {
        Map<String, Object> delta = [:]

        if (!thisCube.isComparableCube(other))
        {
            return null
        }

        // Step 1: Build axial differences
        Map<String, Map<Comparable, ColumnDelta>> deltaMap = [:] as CaseInsensitiveMap
        delta[DELTA_AXES_COLUMNS] = deltaMap

        for (Axis axis : thisCube.getAxes())
        {
            Axis otherAxis = other.getAxis(axis.getName())
            deltaMap[axis.getName()] = getColumnDelta(axis, otherAxis)
        }

        // Store updates-to-be-made so that if cell equality tests pass, these can be 'played' at the end to
        // transactionally apply the merge.  We do not want a partial merge.
        delta[DELTA_CELLS] = getCellDelta(thisCube, other)
        return delta
    }

    /**
     * Merge the passed in cell change-set into this n-cube.  This will apply all of the cell changes
     * in the passed in change-set to the cells of this n-cube, including adds and removes.
     * @param deltaSet Map containing cell change-set.  The cell change-set contains cell coordinates
     * mapped to the associated value to set (or remove) for the given coordinate.
     */
    static <T> void mergeDeltaSet(NCube<T> target, Map<String, Object> deltaSet)
    {
        // Step 1: Merge column-level changes
        Map<String, Map<Long, ColumnDelta>> deltaMap = (Map<String, Map<Long,ColumnDelta>>) deltaSet[DELTA_AXES_COLUMNS]
        for (Map.Entry<String, Map<Long, ColumnDelta>> entry : deltaMap.entrySet())
        {
            String axisName = entry.getKey()
            Map<Long, ColumnDelta> colChanges = entry.getValue()

            for (ColumnDelta colDelta : colChanges.values())
            {
                Column column = colDelta.column
                if (DELTA_COLUMN_ADD.equals(colDelta.changeType))
                {
                    Axis axis = target.getAxis(axisName)
                    Column findCol
                    if (axis.getType() == AxisType.RULE)
                    {
                        if (StringUtilities.hasContent(column.getColumnName()))
                        {
                            findCol = axis.findColumnByName(column.getColumnName())
                        }
                        else
                        {
                            findCol = axis.findColumn(column.id)
                        }
                    }
                    else
                    {   // If the value is not already on the Axis, add it.
                        findCol = axis.findColumn(column.getValue())
                    }

                    if (findCol == null)
                    {
                        target.addColumn(axisName, column.getValue(), column.getColumnName(), column.id)
                    }
                }
                else if (DELTA_COLUMN_REMOVE.equals(colDelta.changeType))
                {
                    Axis axis = target.getAxis(axisName)
                    if (axis.getType() == AxisType.RULE)
                    {   // Rule axis - delete by locating column by ID
                        String name = column.getColumnName()
                        if (name == null)
                        {
                            target.deleteColumn(axisName, column.id)
                        }
                        else
                        {
                            target.deleteColumn(axisName, name)
                        }
                    }
                    else
                    {   // Non-rule axes, delete column by locating value
                        target.deleteColumn(axisName, column.getValue())
                    }
                }
                else if (DELTA_COLUMN_CHANGE.equals(colDelta.changeType))
                {
                    target.updateColumn(column.id, column.getValue())
                }
            }
        }

        // Step 2: Merge cell-level changes
        Map<Map<String, Object>, T> cellDelta = (Map<Map<String, Object>, T>) deltaSet[DELTA_CELLS]
        // Passed all cell conflict tests, update 'this' cube with the new cells from the other cube (merge)
        for (Map.Entry<Map<String, Object>, T> entry : cellDelta.entrySet())
        {
            Set<Long> cols = deltaCoordToSetOfLong(target, entry.getKey())
            if (cols != null && cols.size() > 0)
            {
                T value = entry.getValue()
                if (DELTA_CELL_REMOVE.equals(value))
                {   // Remove cell
                    target.removeCellById(cols)
                }
                else
                {   // Add/Update cell
                    target.setCellById(value, cols)
                }
            }
        }

        target.clearSha1()
    }

    /**
     * Test the compatibility of two 'delta change-set' maps.  This method determines if these two
     * change sets intersect properly or intersect with conflicts.  Used internally when merging
     * two ncubes together in branch-merge operations.
     * @param completeDelta1 Map of cell coordinates to values generated from comparing two cubes (A -> B)
     * @param completeDelta2 Map of cell coordinates to values generated from comparing two cubes (A -> C)
     * @return boolean true if the two cell change-sets are compatible, false otherwise.
     */
    static boolean areDeltaSetsCompatible(Map<String, Object> completeDelta1, Map<String, Object> completeDelta2)
    {
        if (completeDelta1 == null || completeDelta2 == null)
        {
            return false
        }
        // Step 1: Do any column-level updates conflict?
        Map<String, Map<Comparable, ColumnDelta>> deltaMap1 = (Map<String, Map<Comparable, ColumnDelta>>) completeDelta1[DELTA_AXES_COLUMNS]
        Map<String, Map<Comparable, ColumnDelta>> deltaMap2 = (Map<String, Map<Comparable, ColumnDelta>>) completeDelta2[DELTA_AXES_COLUMNS]
        if (deltaMap1.size() != deltaMap2.size())
        {   // Must have same number of axis (axis name is the outer Map key).
            return false
        }

        CaseInsensitiveSet<String> a1 = new CaseInsensitiveSet<>(deltaMap1.keySet())
        CaseInsensitiveSet<String> a2 = new CaseInsensitiveSet<>(deltaMap2.keySet())
        a1.removeAll(a2)

        if (!a1.isEmpty())
        {   // Axis names must be all be the same (ignoring case)
            return false
        }

        // Column change maps must be compatible (on Rule axes)
        for (Map.Entry<String, Map<Comparable, ColumnDelta>> entry1 : deltaMap1.entrySet())
        {
            String axisName = entry1.getKey()
            Map<Comparable, ColumnDelta> changes1 = entry1.getValue()
            Map<Comparable, ColumnDelta> changes2 = deltaMap2[axisName]

            for (Map.Entry<Comparable, ColumnDelta> colEntry1 : changes1.entrySet())
            {
                ColumnDelta delta1 = colEntry1.getValue()
                if (delta1.axisType == AxisType.RULE)
                {   // Only RULE axes need to have their columns compared (because they are ID compared)
                    String colName = delta1.column.getColumnName()
                    ColumnDelta delta2 = colName == null ? changes2[delta1.column.id] : changes2[colName]

                    if (delta2 == null)
                        continue   // no column changed with same ID, delta1 is OK

                    if (delta2.axisType != delta1.axisType)
                        return false   // different axis types

                    if (!delta1.column.getValue().equals(delta2.column.getValue()))
                        return false   // value is different for column with same ID

                    if (!delta1.changeType.equals(delta2.changeType))
                        return false   // different change type (REMOVE vs ADD, CHANGE vs REMOVE, etc.)
                }
            }
        }

        // Step 2: Do any cell-level updates conflict?
        Map<Map<String, Object>, Object> delta1 = (Map<Map<String, Object>, Object>) completeDelta1[DELTA_CELLS]
        Map<Map<String, Object>, Object> delta2 = (Map<Map<String, Object>, Object>) completeDelta2[DELTA_CELLS]
        Map<Map<String, Object>, Object> smallerChangeSet
        Map<Map<String, Object>, Object> biggerChangeSet

        // Performance optimization: determine which cell change set is smaller.
        if (delta1.size() < delta2.size())
        {
            smallerChangeSet = delta1;
            biggerChangeSet = delta2;
        }
        else
        {
            smallerChangeSet = delta2;
            biggerChangeSet = delta1;
        }

        for (Map.Entry<Map<String, Object>, Object> entry : smallerChangeSet.entrySet())
        {
            Map<String, Object> deltaCoord = entry.getKey()

            if (biggerChangeSet.containsKey(deltaCoord))
            {
                CellInfo info1 = new CellInfo(entry.getValue())
                CellInfo info2 = new CellInfo(biggerChangeSet[deltaCoord])

                if (!info1.equals(info2))
                {
                    return false;
                }
            }
        }
        return true;
    }

    private static Map<Comparable, ColumnDelta> getColumnDelta(Axis thisAxis, Axis other)
    {
        Map<Comparable, ColumnDelta> deltaColumns = new CaseInsensitiveMap<>()
        Map<Comparable, Column> copyColumns = new LinkedHashMap<>()
        Map<Comparable, Column> copyRuleColumns = new LinkedHashMap<>()

        if (thisAxis.getType() == AxisType.RULE)
        {   // Have to look up RULE columns by name (if set) or ID if not (no choice, they could all have the value 'true' for example.)
            for (Column column : thisAxis.getColumnsInternal())
            {
                String name = column.getColumnName()
                // Use rule-name if it exists.
                Comparable key = name != null ? name : column.id
                copyRuleColumns[key] = column
            }

            for (Column otherColumn : other.getColumnsInternal())
            {
                String name = otherColumn.getColumnName()
                Column foundCol
                if (name == null)
                {
                    foundCol = thisAxis.getColumnById(otherColumn.id)

                    if (foundCol == null)
                    {   // Not found, the 'other' axis has a column 'this' axis does not have
                        deltaColumns[otherColumn.id] = new ColumnDelta(AxisType.RULE, otherColumn, DELTA_COLUMN_ADD)
                    }
                    else if (otherColumn.getValue() == foundCol.getValue() || otherColumn.getValue().equals(foundCol.getValue()))   // handles default (null) column
                    {   // Matched (id and value same) - this column will not be added to the delta map.  Remove by ID.
                        copyRuleColumns.remove(foundCol.getColumnName() != null ? foundCol.getColumnName() : foundCol.id)
                    }
                    else
                    {   // Column exists on both (same IDs), but the value is different
                        deltaColumns[otherColumn.id] = new ColumnDelta(AxisType.RULE, otherColumn, DELTA_COLUMN_CHANGE)
                        copyRuleColumns.remove(foundCol.getColumnName() != null ? foundCol.getColumnName() : foundCol.id)
                    }
                }
                else
                {
                    foundCol = thisAxis.findColumnByName(name)

                    if (foundCol == null)
                    {   // Not found, the 'other' axis has a column 'this' axis does not have
                        deltaColumns[name] = new ColumnDelta(thisAxis.getType(), otherColumn, DELTA_COLUMN_ADD)
                    }
                    else
                    {   // Matched name
                        if (otherColumn.getValue() == foundCol.getValue() || otherColumn.getValue().equals(foundCol.getValue()))
                        {   // Matched value - this column will not be added to the delta map.  Remove by name.
                            copyRuleColumns.remove(name)
                        }
                        else
                        {   // Value did not match - need to update column
                            deltaColumns[name] = new ColumnDelta(thisAxis.getType(), otherColumn, DELTA_COLUMN_CHANGE)
                            copyRuleColumns.remove(name)    // Since this is a change, don't leave column in 'remove' list.
                        }
                    }
                }
            }

            // Columns left over - these are columns 'this' axis has that the 'other' axis does not have.
            for (Column column : copyRuleColumns.values())
            {   // If 'this' axis has columns 'other' axis does not, then mark these to be removed (like we do with cells).
                Comparable key = column.getColumnName() != null ? column.getColumnName() : column.id
                deltaColumns[key] = new ColumnDelta(AxisType.RULE, column, DELTA_COLUMN_REMOVE)
            }
        }
        else
        {   // Handle non-rule columns
            for (Column column : thisAxis.getColumnsInternal())
            {
                copyColumns[column.getValue()] = column
            }

            for (Column otherColumn : other.getColumnsInternal())
            {
                final Comparable otherColumnValue = otherColumn.getValue()
                Column foundCol = thisAxis.findColumn(otherColumnValue)

                if (foundCol == null || foundCol == other.getDefaultColumn())
                {   // Not found, the 'other' axis has a column 'this' axis does not have
                    deltaColumns[otherColumn.id] = new ColumnDelta(thisAxis.getType(), otherColumn, DELTA_COLUMN_ADD)
                }
                else
                {   // Matched - this column will not be added to the delta map.
                    copyColumns.remove(otherColumnValue)
                }
            }

            // Columns left over - these are columns 'this' axis has that the 'other' axis does not have.
            for (Column column : copyColumns.values())
            {   // If 'this' axis has columns 'other' axis does not, then mark these to be removed (like we do with cells).
                deltaColumns[column.id] = new ColumnDelta(thisAxis.getType(), column, DELTA_COLUMN_REMOVE)
            }
        }

        return deltaColumns
    }

    /**
     * Get all cellular differences between two n-cubes.
     * @param other NCube from which to generate the delta.
     * @return Map containing a Map of cell coordinates [key is Map<String, Object> and value (T)].
     */
    private static <T> Map<Map<String, Object>, T> getCellDelta(NCube<T> thisCube, NCube<T> other)
    {
        Map<Map<String, Object>, T> delta = new HashMap<>()
        Set<Map<String, Object>> copyCells = new HashSet<>()

        for (Map.Entry<Set<Long>, Object> entry : thisCube.getCellMap().entrySet())
        {
            copyCells.add(thisCube.getCoordinateFromIds(new LongHashSet(entry.getKey())))
        }

        // At this point, the cubes have the same number of axes and same axis types.
        // Now, compute cell deltas.
        for (Map.Entry<Set<Long>, T> otherEntry : other.getCellMap().entrySet())
        {
            Set<Long> ids = new LongHashSet(otherEntry.getKey())
            Map<String, Object> deltaCoord = other.getCoordinateFromIds(ids)
            Set<Long> idKey = deltaCoordToSetOfLong(other, deltaCoord)
            if (idKey != null)
            {   // Was able to bind deltaCoord between cubes
                T content = thisCube.getCellByIdNoExecute(idKey)
                T otherContent = otherEntry.getValue()
                copyCells.remove(deltaCoord)

                CellInfo info = new CellInfo(content)
                CellInfo otherInfo = new CellInfo(otherContent)

                if (!info.equals(otherInfo))
                {
                    delta[deltaCoord] = otherContent
                }
            }
        }

        for (Map<String, Object> coord : copyCells)
        {
            delta[coord] = (T) DELTA_CELL_REMOVE
        }

        return delta
    }

    /**
     * Return a list of Delta objects describing the differences between two n-cubes.
     * @param other NCube to compare 'this' n-cube to
     * @return List<Delta> object.  The Delta class contains a Location (loc) which describes the
     * part of an n-cube that differs (ncube, axis, column, or cell) and the Type (type) of difference
     * (ADD, UPDATE, or DELETE).  Finally, it includes an English description of the difference as well.
     */
    public static List<Delta> getDeltaDescription(NCube thisCube, NCube other)
    {
        List<Delta> changes = new ArrayList<>()

        if (!thisCube.name.equalsIgnoreCase(other.name))
        {
            String s = "Name changed from '" + other.name + "' to '" + thisCube.name + "'"
            changes.add(new Delta(Delta.Location.NCUBE, Delta.Type.UPDATE, s))
        }

        List<Delta> metaChanges = compareMetaProperties(other.getMetaProperties(), thisCube.getMetaProperties(), Delta.Location.NCUBE_META, "n-cube '" + thisCube.name + "'")
        changes.addAll(metaChanges)

        Set<String> a1 = thisCube.getAxisNames()
        Set<String> a2 = other.getAxisNames()
        a1.removeAll(a2)

        boolean axesChanged = false
        if (!a1.isEmpty())
        {
            String s = "Added axis: " + a1
            changes.add(new Delta(Delta.Location.AXIS, Delta.Type.ADD, s))
            axesChanged = true
        }

        a1 = thisCube.getAxisNames()
        a2.removeAll(a1)
        if (!a2.isEmpty())
        {
            String s = "Removed axis: " + a2
            changes.add(new Delta(Delta.Location.AXIS, Delta.Type.DELETE, s))
            axesChanged = true
        }

        for (Axis newAxis : thisCube.getAxes())
        {
            Axis oldAxis = other.getAxis(newAxis.getName())
            if (oldAxis == null)
            {
                continue
            }
            if (!newAxis.areAxisPropsEqual(oldAxis))
            {
                String s = "Axis properties changed from " + oldAxis.getAxisPropString() + " to " + newAxis.getAxisPropString()
                changes.add(new Delta(Delta.Location.AXIS, Delta.Type.UPDATE, s))
            }

            metaChanges = compareMetaProperties(oldAxis.getMetaProperties(), newAxis.getMetaProperties(), Delta.Location.AXIS_META, "axis: " + newAxis.getName())
            changes.addAll(metaChanges)

            for (Column newCol : newAxis.getColumns())
            {
                Column oldCol = oldAxis.getColumnById(newCol.id)
                if (oldCol == null)
                {
                    String s = "Column: " + newCol.getValue() + " added to axis: " + newAxis.getName()
                    changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.ADD, s))
                }
                else
                {   // Check Column meta properties
                    metaChanges = compareMetaProperties(oldCol.getMetaProperties(), newCol.getMetaProperties(), Delta.Location.COLUMN_META, "column '" + newAxis.getName() + "'")
                    changes.addAll(metaChanges)

                    if (!DeepEquals.deepEquals(oldCol.getValue(), newCol.getValue()))
                    {
                        String s = "Column value changed from: " + oldCol.getValue() + " to: " + newCol.getValue()
                        changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.UPDATE, s))
                    }
                }
            }

            for (Column oldCol : oldAxis.getColumns())
            {
                Column newCol = newAxis.getColumnById(oldCol.id)
                if (newCol == null)
                {
                    String s = "Column: " + oldCol.getValue() + " removed"
                    changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.DELETE, s))
                }
            }
        }

        // Different dimensionality, don't compare cells
        if (axesChanged)
        {
            return changes
        }

        for (Map.Entry<Set<Long>, Object> entry : thisCube.getCellMap().entrySet())
        {
            Collection<Long> newCellKey = entry.getKey()
            Object newCellValue = entry.getValue()

            if (other.getCellMap().containsKey(newCellKey))
            {
                Object oldCellValue = other.getCellMap()[newCellKey]
                if (!DeepEquals.deepEquals(newCellValue, oldCellValue))
                {
                    Map<String, Object> properCoord = thisCube.getDisplayCoordinateFromIds(newCellKey)
                    String s = "Cell changed at location: " + properCoord + ", from: " +
                            (oldCellValue == null ? null : oldCellValue.toString()) + ", to: " +
                            (newCellValue == null ? null : newCellValue.toString())
                    changes.add(new Delta(Delta.Location.CELL, Delta.Type.UPDATE, s))
                }
            }
            else
            {
                Map<String, Object> properCoord = thisCube.getDisplayCoordinateFromIds(newCellKey)
                String s = "Cell added at location: " + properCoord + ", value: " + (newCellValue == null ? null : newCellValue.toString())
                changes.add(new Delta(Delta.Location.CELL, Delta.Type.ADD, s))
            }
        }

        for (Map.Entry<Set<Long>, Object> entry : other.getCellMap().entrySet())
        {
            Collection<Long> oldCellKey = entry.getKey()
            Object oldCellValue = entry.getValue()

            if (!thisCube.getCellMap().containsKey(oldCellKey))
            {
                boolean allColsStillExist = true
                for (Long colId : oldCellKey)
                {
                    Axis axis = thisCube.getAxisFromColumnId(colId)
                    if (axis == null)
                    {
                        allColsStillExist = false
                        break
                    }
                }

                // Make sure all columns for this cell still exist before reporting it as removed.  Otherwise, a
                // dropped column would report a ton of removed cells.
                if (allColsStillExist)
                {
                    Map<String, Object> properCoord = thisCube.getDisplayCoordinateFromIds(oldCellKey)
                    String s = "Cell removed at location: " + properCoord + ", value: " + (oldCellValue == null ? null : oldCellValue.toString())
                    changes.add(new Delta(Delta.Location.CELL, Delta.Type.DELETE, s))
                }
            }
        }
        return changes
    }

    @PackageScope
    static List<Delta> compareMetaProperties(Map<String, Object> oldMeta, Map<String, Object> newMeta, Delta.Location location, String locName)
    {
        List<Delta> changes = new ArrayList<>()
        Set<String> oldKeys = new CaseInsensitiveSet<>(oldMeta.keySet())
        Set<String> sameKeys = new CaseInsensitiveSet<>(newMeta.keySet())
        sameKeys.retainAll(oldKeys)

        Set<String> addedKeys  = new CaseInsensitiveSet<>(newMeta.keySet())
        addedKeys.removeAll(sameKeys)
        if (!addedKeys.isEmpty())
        {
            StringBuilder s = makeMap(newMeta, addedKeys)
            String entry = addedKeys.size() > 1 ? "meta-entries" : "meta-entry"
            changes.add(new Delta(location, Delta.Type.ADD, locName + " " + entry + " added: " + s))
        }

        Set<String> deletedKeys  = new CaseInsensitiveSet<>(oldMeta.keySet())
        deletedKeys.removeAll(sameKeys)
        if (!deletedKeys.isEmpty())
        {
            StringBuilder s = makeMap(oldMeta, deletedKeys)
            String entry = deletedKeys.size() > 1 ? "meta-entries" : "meta-entry"
            changes.add(new Delta(location, Delta.Type.DELETE, locName + " " + entry + " deleted: " + s))
        }

        int i = 0
        StringBuilder s = new StringBuilder()
        for (String key : sameKeys)
        {
            if (!DeepEquals.deepEquals(oldMeta[key], newMeta[key]))
            {
                s.append(key).append("->").append(oldMeta[key]).append(" ==> ").append(key).append("->").append(newMeta[key]).append(", ")
                i++
            }
        }
        if (i > 0)
        {
            s.setLength(s.length() - 2)     // remove extra ", " at end
            String entry = i > 1 ? "meta-entries" : "meta-entry"
            changes.add(new Delta(location, Delta.Type.UPDATE, locName + " " + entry + " changed: " + s))
        }

        return changes
    }

    /**
     * Convert a DeltaCoord to a Set<Long>.  A 'deltaCoord' is a coordinate which has String axis name
     * keys and associated values (to match against standard axes), but for Rule axes it has the Long ID
     * for the associated value.  These deltaCoord's are used during cube merging to allow coordinates from
     * one cube to bind into another cube.
     * @param deltaCoord Map<String, Object> where the String keys are axis names, and the object is the associated
     * value to bind to the axis.  For RULE axes, the associated value is a Long ID (or rule name, or null
     * for default column - note: long ID can be used for default too).
     * @return Set<Long> that can be used with any n-cube API that binds by ID (getCellById, etc.) or null
     * if the deltaCoord could not bind to this n-cube.
     */
    private static <T> Set<Long> deltaCoordToSetOfLong(NCube<T> target, final Map<String, Object> deltaCoord)
    {
        final Set<Long> key = new LongHashSet()

        for (final Axis axis : target.getAxes())
        {
            final Object value = deltaCoord[axis.getName()]

            if (axis.getType() == AxisType.RULE)
            {
                if (value instanceof Long)
                {
                    if (axis.getColumnById((Long) value) != null)
                    {   // Verify the ID is good
                        key.add((Long) value)
                    }
                    else
                    {
                        return null
                    }
                }
                else if (value instanceof String)
                {
                    Column column = axis.findColumnByName((String)value)
                    if (column != null)
                    {
                        key.add(column.id)
                    }
                    else
                    {
                        return null
                    }
                }
                else if (value == null)
                {   // You can get the Default column by passing in null
                    Column column = axis.findColumn(null)
                    if (column != null)
                    {
                        key.add(column.id)
                    }
                    else
                    {
                        return null
                    }
                }
                else
                {   // error case: only a Long, String, or null can be associated to the name of a rule axis in a deltaCoord.
                    return null
                }
            }
            else
            {
                final Column column = axis.findColumn((Comparable) value)
                if (column == null)
                {
                    return null
                }
                key.add(column.id)
            }
        }
        return key
    }

    private static StringBuilder makeMap(Map<String, Object> newMeta, Set<String> addedKeys)
    {
        StringBuilder s = new StringBuilder()
        Iterator<String> i = addedKeys.iterator()
        while (i.hasNext())
        {
            String key = i.next()
            s.append(key)
            s.append('->')
            s.append(newMeta[key])
            if (i.hasNext())
            {
                s.append(', ')
            }
        }
        return s
    }
}
