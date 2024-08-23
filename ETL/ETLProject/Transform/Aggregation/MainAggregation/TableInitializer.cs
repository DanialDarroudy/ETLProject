using System.Data;

namespace ETLProject.Transform.Aggregation.MainAggregation;

public class TableInitializer
{
    public DataTable InitializeTable(List<DataColumn> groupedBys, DataColumn aggregated)
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