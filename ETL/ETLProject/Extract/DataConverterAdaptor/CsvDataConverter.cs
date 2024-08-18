using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;

namespace ETLProject.Extract.DataConverterAdaptor;

public class CsvDataConverter : IDataConverter
{
    public List<DataTable> ConvertToDataTables(string source)
    {
        var dataTable = new DataTable();
        var reader = new StreamReader(source);
        var csvReader = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture));
        var dataReader = new CsvDataReader(csvReader);
        dataTable.Load(dataReader);
        
        return [dataTable];
    }
}