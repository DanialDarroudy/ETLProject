using ETLProject.Extract.DataConverterAdaptor;

namespace ETLProject.Extract;

public static class DataConverterFactory
{
    public static IDataConverter GetConverter(string type)
    {
        if (type.Equals("CSV", StringComparison.OrdinalIgnoreCase))
        {
            return new CsvDataConverter();
        }
        if (type.Equals("SQL", StringComparison.OrdinalIgnoreCase))
        {
            return new SqlDataConverter();
        }
        throw new ArgumentException("Invalid converter type");
    }
}