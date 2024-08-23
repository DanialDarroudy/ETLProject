using System.Data;
using ETLProject.Transform.Aggregation.AggregateStrategy;

namespace ETLProject.Transform.Aggregation.MainAggregation;

public class Aggregation
{
    private readonly DataTable _table;
    private readonly List<DataColumn> _groupedBys;
    private readonly DataColumn _aggregated;
    private readonly IAggregateStrategy _strategy;

    public Aggregation(DataTable table, List<DataColumn> groupedBys, DataColumn aggregated, IAggregateStrategy strategy)
    {
        _table = table;
        _groupedBys = groupedBys;
        _aggregated = aggregated;
        _strategy = strategy;
    }

    public DataTable Aggregate()
    {
        ObjectCheck.CheckNull(_table);
        ObjectCheck.CheckEmpty(_table);
        ObjectCheck.CheckEmpty(_groupedBys);

        return GenerateAggregatedTable();
    }

    private DataTable GenerateAggregatedTable()
    {
        var resultTable = new TableInitializer().InitializeTable(_groupedBys, _aggregated);
        var processor = new AggregationProcessor();
        var groupedRows = processor.GroupByColumns(_table , _groupedBys);
        foreach (var (groupKey, rowsInGroup) in groupedRows)
        {
            var newRow = resultTable.NewRow();
            processor.PopulateRowWithGroupValues(newRow, groupKey);
            newRow[_groupedBys.Count] = _strategy.DoSpecificAggregate(rowsInGroup, _aggregated);
            resultTable.Rows.Add(newRow);
        }

        resultTable.TableName = _table.TableName;
        return resultTable;
    }
}