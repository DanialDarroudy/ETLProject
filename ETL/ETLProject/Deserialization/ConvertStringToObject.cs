using System.Data;
using ETLProject.Transform;
using ETLProject.Transform.Aggregation;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.ComparisonStrategy;
using ETLProject.Transform.Condition.MainCondition;
using ETLProject.Transform.Condition.OperatorStrategy;

namespace ETLProject.Deserialization;

public static class ConvertStringToObject
{
    public static DataTable GetDataTable(List<DataTable> dataTables,string tableName)
    {
        ObjectCheck.CheckEmpty(dataTables);
        ObjectCheck.CheckHasTableName(dataTables, tableName);
        return dataTables.Find(table => table.TableName == tableName)!;
    }
    public static List<DataColumn> GetGroupedBysColumn(DataTable table , List<string> groupedBysColumnNames)
    {
        ObjectCheck.CheckHasColumnNames(table, groupedBysColumnNames);
        return groupedBysColumnNames.Select(name => table.Columns[name]!).ToList();
    }

    public static DataColumn GetAggregatedColumn(DataTable table , string aggregatedColumnName)
    {
        ObjectCheck.CheckHasColumnNames(table, [aggregatedColumnName]);
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