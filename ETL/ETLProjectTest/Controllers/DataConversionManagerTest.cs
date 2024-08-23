using System.Data;
using ETLProject.Controllers;
using ETLProject.Extract.DataConverterAdaptor;
using ETLProject.Transform;
using FluentAssertions;

namespace ETLProjectTest.Controllers;

public class DataConversionManagerTest
{
    [Theory]
    [MemberData(nameof(ConvertersAndSources))]
    public void CreateConvertersFromSources_ShouldCreateConvertersWithSpecificType_WhenFirstItemOfParameterSpecifyType(
        List<IDataConverter> expected, List<string> sources)
    {
        // Arrange
        var manager = new DataConversionManager();
        // Act
        var actual = manager.CreateConvertersFromSources(sources);
        // Assert
        actual.Should().HaveCount(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            actual[i].Should().BeOfType(expected[i].GetType());
        }
    }

    public static IEnumerable<object[]> ConvertersAndSources()
    {
        var firstConverters = new List<IDataConverter>()
        {
            new CsvDataConverter(),
            new SqlDataConverter(),
            new SqlDataConverter()
        };
        var firstSources = new List<string>()
        {
            new("RandomPath.CsV"),
            new("RandomPath.SqL"),
            new("RandomPath.Sql")
        };
        yield return [firstConverters, firstSources];

        var secondConverters = new List<IDataConverter>()
        {
            new SqlDataConverter(),
            new SqlDataConverter()
        };
        var secondSources = new List<string>()
        {
            new("RandomPath.sqL"),
            new("RandomPath.sQl")
        };
        yield return [secondConverters, secondSources];

        var thirdConverters = new List<IDataConverter>()
        {
            new CsvDataConverter(),
            new CsvDataConverter()
        };
        var thirdSources = new List<string>()
        {
            new("RandomPath.cSv"),
            new("RandomPath.Csv")
        };
        yield return [thirdConverters, thirdSources];
    }

    [Fact]
    public void CreateConvertersFromSources_ShouldThrowArgumentException_WhenParameterIsEmpty()
    {
        // Arrange
        var sources = new List<string>();
        var manager = new DataConversionManager();
        // Act
        var action = () => manager.CreateConvertersFromSources(sources);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.EmptyListError(typeof(string)));
    }

    [Theory]
    [MemberData(nameof(TablesAndConvertersAndSources))]
    public void AddConvertedTablesToList_ShouldReturnAllTablesFromDifferentSources_WhenParametersAreProvided(
        List<DataTable> expected, List<IDataConverter> converters, List<string> sources)
    {
        // Arrange
        var manager = new DataConversionManager();
        // Act
        var actual = manager.AddConvertedTablesToList(converters, sources);
        // Assert
        actual.Should().HaveCount(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            AssertTwoTable.AreEqualTwoTable(expected[i], actual[i]);
        }
    }

    public static IEnumerable<object[]> TablesAndConvertersAndSources()
    {
        var firstTable = new DataTable("Students");
        firstTable.Columns.Add("StudentID", typeof(string));
        firstTable.Columns.Add("FirstName", typeof(string));
        firstTable.Columns.Add("Age", typeof(string));
        firstTable.Columns.Add("Average", typeof(string));
        firstTable.Rows.Add(1001, "John", 15, 4);
        firstTable.Rows.Add(1002, "Jane", 16, 6);
        firstTable.Rows.Add(1003, "Alice", 17, 8);
        firstTable.Rows.Add(1004, "Bob", 16, 10);
        firstTable.Rows.Add(1005, "Charlie", 15, 20);

        var secondTable = new DataTable("Customers");
        secondTable.Columns.Add("Balance", typeof(string));
        secondTable.Columns.Add("Name", typeof(string));
        secondTable.Columns.Add("Address", typeof(string));
        secondTable.Rows.Add(6000, "John Doe", "Tarasht");
        secondTable.Rows.Add(10000, "Jane Smith", "Azadi");
        secondTable.Rows.Add(2500, "Bob Johnson", "Tarasht");
        secondTable.Rows.Add(7500, "Sarah Lee", "Azadi");
        secondTable.Rows.Add(1000, "Tom Wilson", "Azadi");
        secondTable.Rows.Add(15000, "Emily Davis", "Azadi");
        secondTable.Rows.Add(3000, "Michael Brown", "Tarasht");
        secondTable.Rows.Add(8000, "Jessica Taylor", "Tarasht");
        secondTable.Rows.Add(4500, "David Anderson", "Tarasht");
        secondTable.Rows.Add(12000, "Olivia Martinez", "Tarasht");

        var tables = new List<DataTable>()
        {
            firstTable,
            secondTable
        };
        var sources = new List<string>()
        {
            "Students.csv",
            "Customers.csv"
        };
        var converters = new List<IDataConverter>()
        {
            new CsvDataConverter(),
            new CsvDataConverter()
        };
        yield return [tables, converters, sources];
    }
}