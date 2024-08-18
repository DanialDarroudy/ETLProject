using System.Data;

namespace ETLProject.Transform.Condition.Composite;

public class LeafCondition(string condition) : IComponentCondition
{
    public List<DataRow> PerformFilter(DataTable table)
    {
        var strategy = ConvertStringToObject.GetComparisonStrategy(condition);
        InitialCheck.CheckNull(table);
        InitialCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(condition);
        var value = SplitCondition.GetValue(condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList(); 
    }
}