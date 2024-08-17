namespace ETLProject.Transform.Condition.Composite.ComparisonStrategy;

public interface IComparisonStrategy
{
    public bool Compare(object source, string target);
}