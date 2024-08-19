using System.Data;

namespace ETLProject.Transform;

public static class InitialCheck
{
    private const string NullTableError = "The input DataTable cannot be null.";
    public static void CheckNull(DataTable table)
    {
        if (table == null)
        {
            throw new ArgumentException(NullTableError);
        }
    }

    public static void CheckEmpty(DataTable table)
    {
        if (table.Rows.Count == 0)
        {
            throw new ArgumentException("The input DataTable cannot be empty.");
        }
    }

    public static void CheckHasTableName(List<DataTable> dataTables, string tableName)
    {
        if (!dataTables.Exists(table => table.TableName == tableName))
        {
            throw new ArgumentException($"Table {tableName} not found");
        }
    }

    public static void CheckHasColumnNames(DataTable table, List<string> columnNames)
    {
        foreach (var columnName in columnNames)
        {
            if (!table.Columns.Contains(columnName))
            {
                throw new ArgumentException($"Column '{columnName}' does not exist in the DataTable.");
            }
        }
    }
}