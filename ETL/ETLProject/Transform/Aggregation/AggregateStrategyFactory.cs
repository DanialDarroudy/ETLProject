using ETLProject.Transform.Aggregation.AggregateStrategy;

namespace ETLProject.Transform.Aggregation;

public static class AggregateStrategyFactory
{
    public static IAggregateStrategy CreateStrategy(string type)
    {
        if (type.Equals("AVG", StringComparison.OrdinalIgnoreCase))
        {
            return new AverageStrategy();
        }
        if (type.Equals("MAX", StringComparison.OrdinalIgnoreCase))
        {
            return new MaxStrategy();
        }
        if (type.Equals("MIN", StringComparison.OrdinalIgnoreCase))
        {
            return new MinStrategy();
        }
        if (type.Equals("SUM", StringComparison.OrdinalIgnoreCase))
        {
            return new SumStrategy();
        }
        if (type.Equals("COUNT", StringComparison.OrdinalIgnoreCase))
        {
            return new CountStrategy();
        }
        throw new ArgumentException($"Unknown aggregation strategy type: {type}");
    }
}