namespace ETLProject.Transform.Condition.ComparisonStrategy;

public interface IComparisonStrategy
{
    public bool Compare(object source, string target);
}