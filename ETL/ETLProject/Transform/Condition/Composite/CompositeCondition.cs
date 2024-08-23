using System.Data;
using ETLProject.Deserialization;
using Newtonsoft.Json;

namespace ETLProject.Transform.Condition.Composite;

[JsonConverter(typeof(ComponentConditionConverter))]
public class CompositeCondition : IComponentCondition
{
    private readonly List<IComponentCondition> _children;
    private readonly string _strategyType;

    public CompositeCondition(List<IComponentCondition> children, string strategyType)
    {
        _children = children;
        _strategyType = strategyType;
    }
    public List<DataRow> PerformFilter(DataTable table)
    {
        ObjectCheck.CheckEmpty(_children);
        var strategy = ConvertStringToObject.GetOperatorStrategy(_strategyType);
        var result = new List<List<DataRow>>();
        _children.ForEach(child => result.Add(child.PerformFilter(table)));
        return strategy.Operate(result);
    }
}