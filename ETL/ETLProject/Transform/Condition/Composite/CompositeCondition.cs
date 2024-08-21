using System.Data;
using ETLProject.Controllers.Deserialization;
using Newtonsoft.Json;

namespace ETLProject.Transform.Condition.Composite;

[JsonConverter(typeof(ComponentConditionConverter))]
public class CompositeCondition(List<IComponentCondition> children , string strategyType) : IComponentCondition
{
    public List<DataRow> PerformFilter(DataTable table)
    {
        var strategy = ConvertStringToObject.GetOperatorStrategy(strategyType);
        var result = new List<List<DataRow>>();
        children.ForEach(child => result.Add(child.PerformFilter(table)));
        return strategy.Operate(result);
    }
}