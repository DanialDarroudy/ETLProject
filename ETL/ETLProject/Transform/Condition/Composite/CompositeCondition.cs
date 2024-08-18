using System.Data;
using ETLProject.Transform.Condition.Composite.OperatorStrategy;

namespace ETLProject.Transform.Condition.Composite;

public class CompositeCondition(List<IComponentCondition> children , string strategyType) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        var strategy = OperatorStrategyFactory.CreateStrategy(strategyType);
        var result = new List<List<DataRow>>();
        children.ForEach(child => result.Add(child.PerformFilter()));
        return strategy.Operate(result);
    }
}