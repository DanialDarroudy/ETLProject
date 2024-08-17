using System.Data;
using ETLProject.Transform.Condition.Composite.ComparisonStrategy;

namespace ETLProject.Transform.Condition.Composite;

public class LeafCondition(DataTable table , string condition , IComparisonStrategy strategy) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        InitialCheck.CheckNull(table);
        InitialCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(condition);
        var value = SplitCondition.GetValue(condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList(); 
    }
}