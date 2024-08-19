using ETLProject.Transform.Condition.Composite.OperatorStrategy;

namespace ETLProject.Transform.Condition.Composite;

public static class OperatorStrategyFactory
{
    public static IOperatorStrategy CreateStrategy(string type)
    {
        if (type.Equals("AND", StringComparison.OrdinalIgnoreCase))
        {
            return new AndStrategy();
        }
        if (type.Equals("OR", StringComparison.OrdinalIgnoreCase))
        {
            return new OrStrategy();
        }
        throw new ArgumentException(NotExistTypeError(type));
    }
    public static string NotExistTypeError(string type)
    {
        return $"Unknown Operator strategy type: {type}";
    }
}