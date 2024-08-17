using System.Data;
using ETLProject.Transform.Condition.Composite;

namespace ETLProject.Transform.Condition;

public class Condition(IComponentCondition root)
{
    public DataTable ApplyCondition()
    {
        return root.PerformFilter().CopyToDataTable();
    }
}