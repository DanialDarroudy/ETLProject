using System.Data;
using ETLProject.Deserialization;
using ETLProject.Transform.Condition.MainCondition;
using Newtonsoft.Json;

namespace ETLProject.Transform.Condition.Composite;

[JsonConverter(typeof(ComponentConditionConverter))]
public class LeafCondition : IComponentCondition
{
    private readonly string _condition;

    public LeafCondition(string condition)
    {
        _condition = condition;
    }
    public List<DataRow> PerformFilter(DataTable table)
    {
        var strategy = ConvertStringToObject.GetComparisonStrategy(_condition);
        ObjectCheck.CheckNull(table);
        ObjectCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(_condition);
        var value = SplitCondition.GetValue(_condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList();
    }
}