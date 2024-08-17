using System.Data;

namespace ETLProject.Transform.Condition.Composite;

public class LeafCondition(DataTable table , string condition) : IComponentCondition
{
    public List<DataRow> PerformFilter()
    {
        // TODO
    }
}