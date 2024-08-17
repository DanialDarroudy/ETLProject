namespace ETLProject.Transform.Condition.Composite.ComparisonStrategy;

public class EqualStrategy : IComparisonStrategy
{
    public bool Compare(object source, string target)
    {
        return source.ToString() == target;
    }
}