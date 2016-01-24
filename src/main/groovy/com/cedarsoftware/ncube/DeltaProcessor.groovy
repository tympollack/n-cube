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
    /**
     * Merge the passed in cell change-set into this n-cube.  This will apply all of the cell changes
     * in the passed in change-set to the cells of this n-cube, including adds and removes.
     * @param deltaSet Map containing cell change-set.  The cell change-set contains cell coordinates
     * mapped to the associated value to set (or remove) for the given coordinate.
     */
    static <T> void mergeDeltaSet(NCube<T> target, Map<String, Object> deltaSet)
    {
        // Step 1: Merge column-level changes
        Map<String, Map<Long, ColumnDelta>> deltaMap = (Map<String, Map<Long,ColumnDelta>>) deltaSet.get(NCube.DELTA_AXES_COLUMNS)
        for (Map.Entry<String, Map<Long, ColumnDelta>> entry : deltaMap.entrySet())
        {
            String axisName = entry.getKey()
            Map<Long, ColumnDelta> colChanges = entry.getValue()

            for (ColumnDelta colDelta : colChanges.values())
            {
                Column column = colDelta.column;
                if (NCube.DELTA_COLUMN_ADD.equals(colDelta.changeType))
                {
                    Axis axis = target.getAxis(axisName)
                    Column findCol;
                    if (axis.getType() == AxisType.RULE)
                    {
                        if (StringUtilities.hasContent(column.getColumnName()))
                        {
                            findCol = axis.findColumnByName(column.getColumnName());
                        }
                        else
                        {
                            findCol = axis.findColumn(column.id)
                        }
                    }
                    else
                    {   // If the value is not already on the Axis, add it.
                        findCol = axis.findColumn(column.getValue());
                    }

                    if (findCol == null)
                    {
                        target.addColumn(axisName, column.getValue(), column.getColumnName(), column.id)
                    }
                }
                else if (NCube.DELTA_COLUMN_REMOVE.equals(colDelta.changeType))
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
                        target.deleteColumn(axisName, column.getValue());
                    }
                }
                else if (NCube.DELTA_COLUMN_CHANGE.equals(colDelta.changeType))
                {
                    target.updateColumn(column.id, column.getValue());
                }
            }
        }

        // Step 2: Merge cell-level changes
        Map<Map<String, Object>, T> cellDelta = (Map<Map<String, Object>, T>) deltaSet.get(NCube.DELTA_CELLS)
        // Passed all cell conflict tests, update 'this' cube with the new cells from the other cube (merge)
        for (Map.Entry<Map<String, Object>, T> entry : cellDelta.entrySet())
        {
            Set<Long> cols = target.deltaCoordToSetOfLong(entry.getKey());
            if (cols != null && cols.size() > 0)
            {
                T value = entry.getValue()
                if (NCube.DELTA_CELL_REMOVE.equals(value))
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
            return false;
        }
        // Step 1: Do any column-level updates conflict?
        Map<String, Map<Comparable, ColumnDelta>> deltaMap1 = (Map<String, Map<Comparable, ColumnDelta>>) completeDelta1.get(NCube.DELTA_AXES_COLUMNS)
        Map<String, Map<Comparable, ColumnDelta>> deltaMap2 = (Map<String, Map<Comparable, ColumnDelta>>) completeDelta2.get(NCube.DELTA_AXES_COLUMNS)
        if (deltaMap1.size() != deltaMap2.size())
        {   // Must have same number of axis (axis name is the outer Map key).
            return false;
        }

        CaseInsensitiveSet<String> a1 = new CaseInsensitiveSet<>(deltaMap1.keySet());
        CaseInsensitiveSet<String> a2 = new CaseInsensitiveSet<>(deltaMap2.keySet());
        a1.removeAll(a2)

        if (!a1.isEmpty())
        {   // Axis names must be all be the same (ignoring case)
            return false;
        }

        // Column change maps must be compatible (on Rule axes)
        for (Map.Entry<String, Map<Comparable, ColumnDelta>> entry1 : deltaMap1.entrySet())
        {
            String axisName = entry1.getKey()
            Map<Comparable, ColumnDelta> changes1 = entry1.getValue()
            Map<Comparable, ColumnDelta> changes2 = deltaMap2.get(axisName)

            for (Map.Entry<Comparable, ColumnDelta> colEntry1 : changes1.entrySet())
            {
                ColumnDelta delta1 = colEntry1.getValue()
                if (delta1.axisType == AxisType.RULE)
                {   // Only RULE axes need to have their columns compared (because they are ID compared)
                    String colName = delta1.column.getColumnName()
                    ColumnDelta delta2 = colName == null ? changes2.get(delta1.column.id) : changes2.get(colName)

                    if (delta2 == null)
                        continue;   // no column changed with same ID, delta1 is OK

                    if (delta2.axisType != delta1.axisType)
                        return false;   // different axis types

                    if (!delta1.column.getValue().equals(delta2.column.getValue()))
                        return false;   // value is different for column with same ID

                    if (!delta1.changeType.equals(delta2.changeType))
                        return false;   // different change type (REMOVE vs ADD, CHANGE vs REMOVE, etc.)
                }
            }
        }

        // Step 2: Do any cell-level updates conflict?
        Map<Map<String, Object>, Object> delta1 = (Map<Map<String, Object>, Object>) completeDelta1.get(NCube.DELTA_CELLS)
        Map<Map<String, Object>, Object> delta2 = (Map<Map<String, Object>, Object>) completeDelta2.get(NCube.DELTA_CELLS)
        Map<Map<String, Object>, Object> smallerChangeSet;
        Map<Map<String, Object>, Object> biggerChangeSet;

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
                CellInfo info1 = new CellInfo(entry.getValue());
                CellInfo info2 = new CellInfo(biggerChangeSet.get(deltaCoord));

                if (!info1.equals(info2))
                {
                    return false;
                }
            }
        }
        return true;
    }

    @PackageScope
    static Map<Comparable, ColumnDelta> getAxisDelta(Axis thisAxis, Axis other)
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
                copyRuleColumns.put(name != null ? name : column.id, column)
            }

            for (Column otherColumn : other.getColumnsInternal())
            {
                String name = otherColumn.getColumnName()
                Column foundCol;
                if (name == null)
                {
                    foundCol = thisAxis.getColumnById(otherColumn.id)

                    if (foundCol == null)
                    {   // Not found, the 'other' axis has a column 'this' axis does not have
                        deltaColumns.put(otherColumn.id, new ColumnDelta(AxisType.RULE, otherColumn, NCube.DELTA_COLUMN_ADD));
                    }
                    else if (otherColumn.getValue() == foundCol.getValue() || otherColumn.getValue().equals(foundCol.getValue()))   // handles default (null) column
                    {   // Matched (id and value same) - this column will not be added to the delta map.  Remove by ID.
                        copyRuleColumns.remove(foundCol.getColumnName() != null ? foundCol.getColumnName() : foundCol.id)
                    }
                    else
                    {   // Column exists on both (same IDs), but the value is different
                        deltaColumns.put(otherColumn.id, new ColumnDelta(AxisType.RULE, otherColumn, NCube.DELTA_COLUMN_CHANGE));
                        copyRuleColumns.remove(foundCol.getColumnName() != null ? foundCol.getColumnName() : foundCol.id)
                    }
                }
                else
                {
                    foundCol = thisAxis.findColumnByName(name)

                    if (foundCol == null)
                    {   // Not found, the 'other' axis has a column 'this' axis does not have
                        deltaColumns.put(name, new ColumnDelta(thisAxis.getType(), otherColumn, NCube.DELTA_COLUMN_ADD));
                    }
                    else
                    {   // Matched name
                        if (otherColumn.getValue() == foundCol.getValue() || otherColumn.getValue().equals(foundCol.getValue()))
                        {   // Matched value - this column will not be added to the delta map.  Remove by name.
                            copyRuleColumns.remove(name)
                        }
                        else
                        {   // Value did not match - need to update column
                            deltaColumns.put(name, new ColumnDelta(thisAxis.getType(), otherColumn, NCube.DELTA_COLUMN_CHANGE));
                            copyRuleColumns.remove(name)    // Since this is a change, don't leave column in 'remove' list.
                        }
                    }
                }
            }

            // Columns left over - these are columns 'this' axis has that the 'other' axis does not have.
            for (Column column : copyRuleColumns.values())
            {   // If 'this' axis has columns 'other' axis does not, then mark these to be removed (like we do with cells).
                deltaColumns.put(column.getColumnName() != null ? column.getColumnName() : column.id, new ColumnDelta(AxisType.RULE, column, NCube.DELTA_COLUMN_REMOVE));
            }
        }
        else
        {   // Handle non-rule columns
            for (Column column : thisAxis.getColumnsInternal())
            {
                copyColumns.put(column.getValue(), column)
            }

            for (Column otherColumn : other.getColumnsInternal())
            {
                final Comparable otherColumnValue = otherColumn.getValue()
                Column foundCol = thisAxis.findColumn(otherColumnValue)

                if (foundCol == null || foundCol == other.getDefaultColumn())
                {   // Not found, the 'other' axis has a column 'this' axis does not have
                    deltaColumns.put(otherColumn.id, new ColumnDelta(thisAxis.getType(), otherColumn, NCube.DELTA_COLUMN_ADD));
                }
                else
                {   // Matched - this column will not be added to the delta map.
                    copyColumns.remove(otherColumnValue)
                }
            }

            // Columns left over - these are columns 'this' axis has that the 'other' axis does not have.
            for (Column column : copyColumns.values())
            {   // If 'this' axis has columns 'other' axis does not, then mark these to be removed (like we do with cells).
                deltaColumns.put(column.id, new ColumnDelta(thisAxis.getType(), column, NCube.DELTA_COLUMN_REMOVE));
            }
        }

        return deltaColumns;
    }

    /**
     * Get all cellular differences between two n-cubes.
     * @param other NCube from which to generate the delta.
     * @return Map containing a Map of cell coordinates [key is Map<String, Object> and value (T)].
     */
    @PackageScope
    static <T> Map<Map<String, Object>, T> getCellDelta(NCube<T> thisCube, NCube<T> other)
    {
        Map<Map<String, Object>, T> delta = new HashMap<>()
        Set<Map<String, Object>> copyCells = new HashSet<>()

        for (Map.Entry<Set<Long>, Object> entry : thisCube.getCellMap().entrySet())
        {
            copyCells.add(thisCube.getCoordinateFromIds(new LongHashSet(entry.getKey())));
        }

        // At this point, the cubes have the same number of axes and same axis types.
        // Now, compute cell deltas.
        for (Map.Entry<Set<Long>, T> otherEntry : other.getCellMap().entrySet())
        {
            Set<Long> ids = new LongHashSet(otherEntry.getKey());
            Map<String, Object> deltaCoord = other.getCoordinateFromIds(ids)
            Set<Long> idKey = other.deltaCoordToSetOfLong(deltaCoord)
            if (idKey != null)
            {   // Was able to bind deltaCoord between cubes
                T content = thisCube.getCellByIdNoExecute(idKey)
                T otherContent = otherEntry.getValue()
                copyCells.remove(deltaCoord)

                CellInfo info = new CellInfo(content)
                CellInfo otherInfo = new CellInfo(otherContent)

                if (!info.equals(otherInfo))
                {
                    delta.put(deltaCoord, otherContent)
                }
            }
        }

        for (Map<String, Object> coord : copyCells)
        {
            delta.put(coord, (T) NCube.DELTA_CELL_REMOVE)
        }

        return delta;
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
            String s = "Name changed from '" + other.name + "' to '" + thisCube.name + "'";
            changes.add(new Delta(Delta.Location.NCUBE, Delta.Type.UPDATE, s))
        }

        List<Delta> metaChanges = compareMetaProperties(other.getMetaProperties(), thisCube.getMetaProperties(), Delta.Location.NCUBE_META, "n-cube '" + thisCube.name + "'")
        changes.addAll(metaChanges)

        Set<String> a1 = thisCube.getAxisNames()
        Set<String> a2 = other.getAxisNames()
        a1.removeAll(a2)

        boolean axesChanged = false;
        if (!a1.isEmpty())
        {
            String s = "Added axis: " + a1;
            changes.add(new Delta(Delta.Location.AXIS, Delta.Type.ADD, s))
            axesChanged = true;
        }

        a1 = thisCube.getAxisNames()
        a2.removeAll(a1)
        if (!a2.isEmpty())
        {
            String s = "Removed axis: " + a2;
            changes.add(new Delta(Delta.Location.AXIS, Delta.Type.DELETE, s))
            axesChanged = true;
        }

        for (Axis newAxis : thisCube.getAxes())
        {
            Axis oldAxis = other.getAxis(newAxis.getName())
            if (oldAxis == null)
            {
                continue;
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
                    String s = "Column: " + oldCol.getValue() + " removed";
                    changes.add(new Delta(Delta.Location.COLUMN, Delta.Type.DELETE, s))
                }
            }
        }

        // Different dimensionality, don't compare cells
        if (axesChanged)
        {
            return changes;
        }

        for (Map.Entry<Set<Long>, Object> entry : thisCube.getCellMap().entrySet())
        {
            Collection<Long> newCellKey = entry.getKey()
            Object newCellValue = entry.getValue()

            if (other.getCellMap().containsKey(newCellKey))
            {
                Object oldCellValue = other.getCellMap().get(newCellKey)
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
                boolean allColsStillExist = true;
                for (Long colId : oldCellKey)
                {
                    Axis axis = thisCube.getAxisFromColumnId(colId)
                    if (axis == null)
                    {
                        allColsStillExist = false;
                        break;
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
        return changes;
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
            String entry = addedKeys.size() > 1 ? "meta-entries" : "meta-entry";
            changes.add(new Delta(location, Delta.Type.ADD, locName + " " + entry + " added: " + s))
        }

        Set<String> deletedKeys  = new CaseInsensitiveSet<>(oldMeta.keySet())
        deletedKeys.removeAll(sameKeys)
        if (!deletedKeys.isEmpty())
        {
            StringBuilder s = makeMap(oldMeta, deletedKeys)
            String entry = deletedKeys.size() > 1 ? "meta-entries" : "meta-entry";
            changes.add(new Delta(location, Delta.Type.DELETE, locName + " " + entry + " deleted: " + s))
        }

        int i = 0;
        StringBuilder s = new StringBuilder()
        for (String key : sameKeys)
        {
            if (!DeepEquals.deepEquals(oldMeta.get(key), newMeta.get(key)))
            {
                s.append(key).append("->").append(oldMeta.get(key)).append(" ==> ").append(key).append("->").append(newMeta.get(key)).append(", ")
                i++;
            }
        }
        if (i > 0)
        {
            s.setLength(s.length() - 2)     // remove extra ", " at end
            String entry = i > 1 ? "meta-entries" : "meta-entry";
            changes.add(new Delta(location, Delta.Type.UPDATE, locName + " " + entry + " changed: " + s))
        }

        return changes;
    }

    private static StringBuilder makeMap(Map<String, Object> newMeta, Set<String> addedKeys)
    {
        StringBuilder s = new StringBuilder()
        Iterator<String> i = addedKeys.iterator()
        while (i.hasNext())
        {
            String key = i.next()
            s.append(key)
            s.append("->")
            s.append(newMeta.get(key))
            if (i.hasNext())
            {
                s.append(", ")
            }
        }
        return s;
    }
}
