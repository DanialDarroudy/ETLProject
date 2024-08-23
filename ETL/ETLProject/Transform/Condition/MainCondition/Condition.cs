using System.Data;
using ETLProject.Transform.Condition.Composite;

namespace ETLProject.Transform.Condition.MainCondition;

public class Condition(IComponentCondition root)
{
    public DataTable ApplyCondition(DataTable table)
    {
        DataTable result;
        try
        {
            result = root.PerformFilter(table).CopyToDataTable();
            result.TableName = table.TableName;
        }
        catch (Exception)
        {
            result = new DataTable(table.TableName);
            for (var i = 0; i < table.Columns.Count; i++)
            {
                result.Columns.Add(table.Columns[i].ColumnName, table.Columns[i].DataType);
            }
        }

        return result;
    }
}