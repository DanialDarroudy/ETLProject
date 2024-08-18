using System.Data;
using Npgsql;

namespace ETLProject.Extract.DataConverterAdaptor;

public class SqlDataConverter : IDataConverter
{
    private const string QueryForGetAllTableName = 
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";

    public List<DataTable> ConvertToDataTables(string source)
    {
        var connection = ConvertSourceToConnectionString(source);
        var npgsql = new NpgsqlConnection(connection);
        npgsql.Open();
        var command = new NpgsqlCommand(QueryForGetAllTableName, npgsql);
        var reader = command.ExecuteReader();
        var tables = new List<DataTable>();
        while (reader.Read())
        {
            var tableName = reader.GetString(0);
            tables.Add(GetDataTable(npgsql, tableName));
        }

        return tables;
    }

    private DataTable GetDataTable(NpgsqlConnection npgsql, string tableName)
    {
        var dataTable = new DataTable();
        var query = $"SELECT * FROM {tableName}";
        var command = new NpgsqlCommand(query, npgsql);
        var adapter = new NpgsqlDataAdapter(command);
        adapter.Fill(dataTable);
        dataTable.TableName = tableName;
        return dataTable;
    }
    private string ConvertSourceToConnectionString(string source)
    {
        var server = source.Split(' ')[0];
        var username = source.Split(' ')[1];
        var password = source.Split(' ')[2];
        var database = source.Split(' ')[3];
        return $"Host={server};Username={username};Password={password};Database={database}";
    }
}