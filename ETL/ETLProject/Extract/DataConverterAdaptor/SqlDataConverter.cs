using System.Data;
using Npgsql;

namespace ETLProject.Extract.DataConverterAdaptor;

public class SqlDataConverter : IDataConverter
{
    public List<DataTable> ConvertToDataTables(string source)
    {
        var connectionString = ConvertSourceToConnectionString(source);
        var tables = new List<DataTable>();
        using (var npgsql = new NpgsqlConnection(connectionString))
        {
            npgsql.Open();
            using (var command = new NpgsqlCommand(
                       "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                       , npgsql))
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    tables.Add(GetDataTable(connectionString, tableName));
                }
            }
        }

        return tables;
    }

    private DataTable GetDataTable(string connectionString, string tableName)
    {
        var dataTable = new DataTable();
        using (var npgsql = new NpgsqlConnection(connectionString))
        {
            npgsql.Open();
            using (var command = new NpgsqlCommand($"SELECT * FROM \"{tableName}\"", npgsql))
            using (var adapter = new NpgsqlDataAdapter(command))
            {
                adapter.Fill(dataTable);
                dataTable.TableName = tableName;
            }
        }

        return dataTable;
    }

    private string ConvertSourceToConnectionString(string source)
    {
        var parts = source.Split('.');
        return $"Host={parts[0]};Username={parts[1]};Password={parts[2]};Database={parts[3]}";
    }
}