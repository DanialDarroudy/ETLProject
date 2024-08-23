using System.Data;
using System.Text.Json;

namespace ETLProject.Transform;

public static class ObjectCheck
{
    public const string NullTableError = "The input DataTable cannot be null.";
    public const string EmptyTableError = "The input DataTable cannot be empty.";

    public static string NotExistTableError(string tableName)
    {
        return $"Table {tableName} not found";
    }

    public static string NotExistColumnError(string columnName)
    {
        return $"Column '{columnName}' does not exist in the DataTable.";
    }

    public static string EmptyListError(Type type)
    {
        return $"The input List Of {type} cannot be empty.";
    }

    public static string NotExistProperty(string propertyName)
    {
        return $"Unknown type or missing property: {propertyName}";
    }

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
            throw new ArgumentException(EmptyTableError);
        }
    }

    public static void CheckEmpty<T>(List<T> target)
    {
        if (target.Count == 0)
        {
            throw new ArgumentException(EmptyListError(typeof(T)));
        }
    }

    public static void EnsurePropertyExist(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out _))
        {
            throw new ArgumentException(NotExistProperty(propertyName));
        }
    }

    public static void CheckHasTableName(List<DataTable> dataTables, string tableName)
    {
        if (!dataTables.Exists(table => table.TableName == tableName))
        {
            throw new ArgumentException(NotExistTableError(tableName));
        }
    }

    public static void CheckHasColumnNames(DataTable table, List<string> columnNames)
    {
        foreach (var columnName in columnNames)
        {
            if (!table.Columns.Contains(columnName))
            {
                throw new ArgumentException(NotExistColumnError(columnName));
            }
        }
    }
}