using System.Data;
using ETLProject.Transform.Aggregation.AggregateStrategy;

namespace ETLProject.Transform.Aggregation;

public class AggregationDTO
{
    public string TableName { get; }
    public List<string> GroupedBysColumnNames { get; }
    public string AggregatedColumnName { get; }
    public string StrategyType { get; }

    public DataTable GetTable(List<DataTable> dataTables)
    {
        InitialCheck.CheckHasTableName(dataTables, TableName);
        return dataTables.Find(table => table.TableName == TableName)!;
    }

    public List<DataColumn> GetGroupedBysColumn(DataTable table)
    {
        InitialCheck.CheckHasColumnNames(table, GroupedBysColumnNames);
        return GroupedBysColumnNames.Select(name => table.Columns[name]!).ToList();
    }

    public DataColumn GetAggregatedColumn(DataTable table)
    {
        InitialCheck.CheckHasColumnNames(table, [AggregatedColumnName]);
        return table.Columns[AggregatedColumnName]!;
    }

    public IAggregateStrategy GetStrategy()
    {
        return AggregateStrategyFactory.CreateStrategy(StrategyType);
    }
}