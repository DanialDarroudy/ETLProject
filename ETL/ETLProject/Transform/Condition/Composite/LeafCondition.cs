using System.Data;
using ETLProject.Controllers.Deserialization;
using Newtonsoft.Json;

namespace ETLProject.Transform.Condition.Composite;

[JsonConverter(typeof(ComponentConditionConverter))]
public class LeafCondition(string condition) : IComponentCondition
{
    public List<DataRow> PerformFilter(DataTable table)
    {
        var strategy = ConvertStringToObject.GetComparisonStrategy(condition);
        EnsureCheck.CheckNull(table);
        EnsureCheck.CheckEmpty(table);
        var columnName = SplitCondition.GetColumnName(condition);
        var value = SplitCondition.GetValue(condition);
        
        return table.AsEnumerable().Where(row => strategy.Compare(row[columnName], value)).ToList();
    }
}