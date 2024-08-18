using ETLProject.Transform.Condition.Composite.ComparisonStrategy;

namespace ETLProject.Transform.Condition.Composite;

public class ComparisonStrategyFactory
{
    public static IComparisonStrategy CreateStrategy(string type)
    {
        if (type.Equals("=", StringComparison.OrdinalIgnoreCase))
        {
            return new EqualStrategy();
        }

        if (type.Equals("<", StringComparison.OrdinalIgnoreCase))
        {
            return new LessThanStrategy();
        }

        if (type.Equals(">", StringComparison.OrdinalIgnoreCase))
        {
            return new MoreThanStrategy();
        }

        throw new ArgumentException($"Unknown Comparison strategy type: {type}");
    }
}