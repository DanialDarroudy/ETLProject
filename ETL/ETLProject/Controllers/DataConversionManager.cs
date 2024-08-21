using System.Data;
using ETLProject.Extract;
using ETLProject.Extract.DataConverterAdaptor;
using ETLProject.Transform;

namespace ETLProject.Controllers;

public static class DataConversionManager
{
    public static List<IDataConverter> CreateConvertersFromSources(List<string> sources)
    {
        InitialCheck.CheckEmpty(sources);
        return sources.Select(str =>
            DataConverterFactory.CreateConverter(str[^3..])).ToList(); 
    }

    public static List<DataTable> AddConvertedTablesToList(List<IDataConverter> converters
        , List<string> sources)
    {
        return converters.Select((converter, index) => converter.ConvertToDataTables(sources[index]))
            .SelectMany(tables => tables).ToList();
    }
}