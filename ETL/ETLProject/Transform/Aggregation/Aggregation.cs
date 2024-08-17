using System.Data;
using ETLProject.Transform.Aggregation.Strategy;

namespace ETLProject.Transform.Aggregation;

public abstract class Aggregation(List<DataColumn> groupedBys, DataColumn aggregated , IAggregateStrategy strategy)
{

    public DataTable Aggregate(DataTable table)
    {
        CheckNull(table);
        CheckEmpty(table);
        var resultTable = TableInitializer.InitializeTable(groupedBys , aggregated);
        var groupedRows = GroupByColumns(table);
        foreach (var (groupKey, rowsInGroup) in groupedRows)
        {
            var newRow = resultTable.NewRow();
            PopulateRowWithGroupValues(newRow, groupKey);
            newRow[groupedBys.Count] = strategy.DoSpecificAggregate(rowsInGroup , aggregated);
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