using System.Data;
using ETLProject.Transform.Aggregation;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.Composite;
using ETLProject.Transform.Condition.Composite.ComparisonStrategy;
using ETLProject.Transform.Condition.Composite.OperatorStrategy;

namespace ETLProject.Transform;

public static class ConvertStringToObject
{
    public static DataTable GetDataTable(List<DataTable> dataTables,string tableName)
    {
        EnsureCheck.CheckHasTableName(dataTables, tableName);
        return dataTables.Find(table => table.TableName == tableName)!;
    }
    public static List<DataColumn> GetGroupedBysColumn(DataTable table , List<string> groupedBysColumnNames)
    {
        EnsureCheck.CheckHasColumnNames(table, groupedBysColumnNames);
        return groupedBysColumnNames.Select(name => table.Columns[name]!).ToList();
    }

    public static DataColumn GetAggregatedColumn(DataTable table , string aggregatedColumnName)
    {
        EnsureCheck.CheckHasColumnNames(table, [aggregatedColumnName]);
        return table.Columns[aggregatedColumnName]!;
    }

    public static IAggregateStrategy GetAggregateStrategy(string strategyType)
    {
        return AggregateStrategyFactory.CreateStrategy(strategyType);
    }
    public static IComparisonStrategy GetComparisonStrategy(string condition)
    {
        return ComparisonStrategyFactory.CreateStrategy(SplitCondition.GetOperator(condition));
    }
    public static IOperatorStrategy GetOperatorStrategy(string strategyType)
    {
        return OperatorStrategyFactory.CreateStrategy(strategyType);
    }
    
}