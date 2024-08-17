using System.Data;

namespace ETLProject.Transform.Condition.Composite;

public interface IComponentCondition
{
    public List<DataRow> PerformFilter();
}