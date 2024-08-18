using System.Data;

namespace ETLProject.Transform.Condition.Composite;

public class LeafCondition(DataTable table , string condition , string strategyType) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        var strategy = ComparisonStrategyFactory.CreateStrategy(strategyType);
        InitialCheck.CheckNull(table);
        InitialCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(condition);
        var value = SplitCondition.GetValue(condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList(); 
    }
}