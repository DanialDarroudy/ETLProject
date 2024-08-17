using System.Data;

namespace ETLProject.Extract.DataConverterAdaptor;

public interface IDataConverter
{
    public List<DataTable> ConvertToDataTables(string source);
}