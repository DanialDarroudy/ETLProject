using System.Data;
using ETLProject.Transform.Condition.Composite.Strategy;

namespace ETLProject.Transform.Condition.Composite;

public class CompositeCondition(List<IComponentCondition> children , IOperatorStrategy strategy) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        var result = new List<List<DataRow>>();
        children.ForEach(child => result.Add(child.PerformFilter()));
        return strategy.Operate(result);
    }
}