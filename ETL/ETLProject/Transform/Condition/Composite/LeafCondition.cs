using System.Data;
using ETLProject.Extract;

namespace ETLProject.Transform.Condition.Composite;

public class LeafCondition(string tableName , string condition , string strategyType) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        var strategy = ComparisonStrategyFactory.CreateStrategy(strategyType);
        var table = DataBase.GetDataTable(tableName);
        InitialCheck.CheckNull(table);
        InitialCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(condition);
        var value = SplitCondition.GetValue(condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList(); 
    }
}