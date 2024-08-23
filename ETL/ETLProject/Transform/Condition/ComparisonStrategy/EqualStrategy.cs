namespace ETLProject.Transform.Condition.ComparisonStrategy;

public class EqualStrategy : IComparisonStrategy
{
    public bool Compare(object source, string target)
    {
        return source.ToString()!.Equals(target);
    }
}