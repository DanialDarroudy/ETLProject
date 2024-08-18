using System.Data;
using ETLProject.Transform.Condition.Composite;

namespace ETLProject.Transform.Condition;

public class Condition(IComponentCondition root)
{
    public DataTable ApplyCondition(DataTable table)
    {
        return root.PerformFilter(table).CopyToDataTable();
    }
}