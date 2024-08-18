using System.Data;
using ETLProject.Transform;

namespace ETLProject.Extract;

public static class DataBase
{
    public static List<DataTable> DataTables { get; set; }

    public static DataTable GetDataTable(string tableName)
    {
        InitialCheck.CheckHasTableName(DataTables, tableName);
        return DataTables.Find(table => table.TableName == tableName)!;
    }
}