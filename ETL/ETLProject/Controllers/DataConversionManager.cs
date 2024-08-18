using System.Data;
using ETLProject.Extract;
using ETLProject.Extract.DataConverterAdaptor;

namespace ETLProject.Controllers;

public static class DataConversionManager
{
    public static List<IDataConverter> CreateConvertersFromSources(List<Tuple<string , string>> sources)
    {
        return sources.Select(tuple =>
            DataConverterFactory.CreateConverter(tuple.Item1)).ToList();
    }

    public static List<DataTable> AddConvertedTablesToList(List<IDataConverter> converters , List<Tuple<string , string>> sources)
    {
        return converters.Select((converter, index) => converter.ConvertToDataTables(sources[index].Item2))
            .SelectMany(tables => tables).ToList();
    }
}