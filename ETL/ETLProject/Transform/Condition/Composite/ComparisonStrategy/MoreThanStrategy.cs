namespace ETLProject.Transform.Condition.Composite.ComparisonStrategy;

public class MoreThanStrategy : IComparisonStrategy
{
    public bool Compare(object source, string target)
    {
        return Convert.ToDecimal(source) > Convert.ToDecimal(target);
    }
}