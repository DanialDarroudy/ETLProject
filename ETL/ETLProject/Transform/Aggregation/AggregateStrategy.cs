using System.Data;

namespace ETLProject.Transform.Aggregation;

public abstract class Aggregation(List<DataColumn> groupedBys, DataColumn aggregated , IAggregateStrategy strategy)
{

    public DataTable Aggregate(DataTable table)
    {
        CheckNull(table);
        CheckEmpty(table);
        var resultTable = InitializeResultTable();
        var groupedRows = GroupByColumns(table);
        foreach (var (groupKey, rowsInGroup) in groupedRows)
        {
            var newRow = resultTable.NewRow();
            PopulateRowWithGroupValues(newRow, groupKey);
            var aggregateResult = strategy.DoSpecificAggregate(rowsInGroup , aggregated);
            newRow[groupedBys.Count] = aggregateResult;
            resultTable.Rows.Add(newRow);
        }

        return resultTable;
    }

    private void CheckNull(DataTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
    }

    private void CheckEmpty(DataTable table)
    {
        if (table.Rows.Count == 0)
        {
            throw new ArgumentException("The input DataTable cannot be empty.");
        }
    }

    private DataTable InitializeResultTable()
    {
        var resultTable = new DataTable();

        foreach (var column in groupedBys)
        {
            resultTable.Columns.Add(column.ColumnName, column.DataType);
        }

        resultTable.Columns.Add(aggregated.ColumnName, aggregated.DataType);
        return resultTable;
    }

    private Dictionary<string, List<DataRow>> GroupByColumns(DataTable table)
    {
        var groupedRowsDict = new Dictionary<string, List<DataRow>>();

        foreach (DataRow row in table.Rows)
        {
            var groupKey = string.Join(",", groupedBys.Select(g => row[g].ToString()));

            if (!groupedRowsDict.ContainsKey(groupKey))
            {
                groupedRowsDict[groupKey] = [];
            }

            groupedRowsDict[groupKey].Add(row);
        }

        return groupedRowsDict;
    }

    private void PopulateRowWithGroupValues(DataRow newRow, string groupKey)
    {
        var groupValues = groupKey.Split(',');

        for (var i = 0; i < groupedBys.Count; i++)
        {
            newRow[i] = groupValues[i];
        }
    }
}