using System.Data;

namespace ETLProject.Transform;

public static class InitialCheck
{
    public static void CheckNull(DataTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
    }

    public static void CheckEmpty(DataTable table)
    {
        if (table.Rows.Count == 0)
        {
            throw new ArgumentException("The input DataTable cannot be empty.");
        }
    }
}