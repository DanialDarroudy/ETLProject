using System.Data;
using IronXL;

namespace ETLProject.Extract.DataConverterAdaptor;

public class CsvDataConverter : IDataConverter
{
    public List<DataTable> ConvertToDataTables(string source)
    {
        var workbook = WorkBook.LoadCSV(source);
        var sheet = workbook.DefaultWorkSheet;
        var dt = sheet.ToDataTable(true);
        return [dt];
    }
}