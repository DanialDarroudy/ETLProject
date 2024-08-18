using System.Data;
using ETLProject.Transform;

namespace ETLProject.Extract;

public static class DataBase
{
    public static List<DataTable> DataTables { get; set; } = [];

    public static DataTable GetDataTable(string tableName)
    {
        InitialCheck.CheckHasTableName(DataTables, tableName);
        return DataTables.Find(table => table.TableName == tableName)!;
    }
    
    public static string ConvertSourceToConnectionString(string source)
    {
        var server = source.Split(' ')[0];
        var username = source.Split(' ')[1];
        var password = source.Split(' ')[2];
        var database = source.Split(' ')[3];
        return $"Host={server};Username={username};Password={password};Database={database}";
    }
}