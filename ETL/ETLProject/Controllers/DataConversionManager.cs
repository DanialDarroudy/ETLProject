using System.Data;
using ETLProject.Extract;
using ETLProject.Extract.DataConverterAdaptor;
using ETLProject.Transform;

namespace ETLProject.Controllers;

public class DataConversionManager
{
    public List<IDataConverter> CreateConvertersFromSources(List<string> sources)
    {
        ObjectCheck.CheckEmpty(sources);
        return sources.Select(str =>
            DataConverterFactory.CreateConverter(str[^3..])).ToList(); 
    }

    public List<DataTable> AddConvertedTablesToList(List<IDataConverter> converters
        , List<string> sources)
    {
        return converters.Select((converter, index) => converter.ConvertToDataTables(sources[index]))
            .SelectMany(tables => tables).ToList();
    }
}