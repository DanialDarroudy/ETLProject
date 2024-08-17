using System.Data;

namespace ETLProject.Transform.Aggregation;

public static class TableInitializer
{
    public static DataTable InitializeTable(List<DataColumn> groupedBys, DataColumn aggregated)
    {
        var resultTable = new DataTable();

        foreach (var column in groupedBys)
        {
            resultTable.Columns.Add(column.ColumnName, column.DataType);
        }
        
        resultTable.Columns.Add(aggregated.ColumnName, aggregated.DataType);
        return resultTable;
    }
}