using ETLProject.Transform.Condition.ComparisonStrategy;

namespace ETLProject.Transform.Condition;

public static class ComparisonStrategyFactory
{
    public static IComparisonStrategy CreateStrategy(char type)
    {
        return type switch
        {
            '=' => new EqualStrategy(),
            '<' => new LessThanStrategy(),
            '>' => new MoreThanStrategy(),
            _ => throw new ArgumentException(NotExistTypeError(type))
        };
    }
    public static string NotExistTypeError(char type)
    {
        return $"Unknown Comparison strategy type: {type}";
    }
}