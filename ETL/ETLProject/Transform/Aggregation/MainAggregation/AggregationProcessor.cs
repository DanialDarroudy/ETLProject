using System.Data;

namespace ETLProject.Transform.Aggregation.MainAggregation;

public class AggregationProcessor
{

    public Dictionary<string, List<DataRow>> GroupByColumns(DataTable table , List<DataColumn> groupedBys)
    {
        var groupedRowsDict = new Dictionary<string, List<DataRow>>();
        foreach (DataRow row in table.Rows)
        {
            var groupKey = string.Join(",", groupedBys.Select(column => row[column].ToString()));
            if (!groupedRowsDict.ContainsKey(groupKey))
            {
                groupedRowsDict[groupKey] = [];
            }

            groupedRowsDict[groupKey].Add(row);
        }

        return groupedRowsDict;
    }

    public void PopulateRowWithGroupValues(DataRow newRow, string groupKey)
    {
        var groupValues = groupKey.Split(',');

        for (var i = 0; i < groupValues.Length; i++)
        {
            newRow[i] = groupValues[i];
        }
    }
}