using System.Data;
using ETLProject.Transform.Condition.Composite;

namespace ETLProject.Transform.Condition;

public class Condition(IComponentCondition root)
{
    public DataTable ApplyCondition(DataTable table)
    {
        try
        {
            return root.PerformFilter(table).CopyToDataTable();
        }
        catch (Exception e)
        {
            return new DataTable(table.TableName);
        }
    }
}