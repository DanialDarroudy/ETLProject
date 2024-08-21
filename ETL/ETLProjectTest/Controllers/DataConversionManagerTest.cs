using System.Data;
using ETLProject.Controllers;
using ETLProject.Extract.DataConverterAdaptor;
using ETLProject.Transform;
using FluentAssertions;

namespace ETLProjectTest.Controllers;

public class DataConversionManagerTest
{
    // [Theory]
    // [MemberData(nameof(ConvertersAndSources))]
    // public void CreateConvertersFromSources_ShouldCreateConvertersWithSpecificType_WhenFirstItemOfParameterSpecifyType(
    //     List<IDataConverter> expected, List<string> sources)
    // {
    //     // Arrange
    //
    //     // Act
    //     var actual = DataConversionManager.CreateConvertersFromSources(sources);
    //     // Assert
    //     actual.Should().HaveCount(expected.Count);
    //     for (var i = 0; i < actual.Count; i++)
    //     {
    //         actual[i].Should().BeOfType(expected[i].GetType());
    //     }
    // }
    //
    // public static IEnumerable<object[]> ConvertersAndSources()
    // {
    //     var firstConverters = new List<IDataConverter>()
    //     {
    //         new CsvDataConverter(),
    //         new SqlDataConverter(),
    //         new SqlDataConverter()
    //     };
    //     var firstSources = new List<string>()
    //     {
    //         new("Csv", "RandomPath"),
    //         new("Sql", "RandomPath"),
    //         new("sQl", "RandomPath")
    //     };
    //     yield return [firstConverters, firstSources];
    //
    //     var secondConverters = new List<IDataConverter>()
    //     {
    //         new SqlDataConverter(),
    //         new SqlDataConverter()
    //     };
    //     var secondSources = new List<string>()
    //     {
    //         new("SQL", "RandomPath"),
    //         new("sqL", "RandomPath")
    //     };
    //     yield return [secondConverters, secondSources];
    //
    //     var thirdConverters = new List<IDataConverter>()
    //     {
    //         new CsvDataConverter(),
    //         new CsvDataConverter()
    //     };
    //     var thirdSources = new List<string>()
    //     {
    //         new("cSv", "RandomPath"),
    //         new("csv", "RandomPath")
    //     };
    //     yield return [thirdConverters, thirdSources];
    // }

    // [Fact]
    // public void CreateConvertersFromSources_ShouldThrowArgumentException_WhenParameterIsEmpty()
    // {
    //     // Arrange
    //     var sources = new List<Tuple<string, string>>();
    //     // Act
    //     var action = () => DataConversionManager.CreateConvertersFromSources(sources);
    //     // Assert
    //     action.Should().Throw<ArgumentException>().WithMessage(
    //         InitialCheck.EmptyListError(typeof(Tuple<string, string>)));
    // }

    // [Theory]
    // [MemberData(nameof(TablesAndConvertersAndSources))]
    // public void AddConvertedTablesToList_ShouldReturnAllTablesFromDifferentSources_WhenParametersAreProvided(
    //     List<DataTable> expected, List<IDataConverter> converters, List<Tuple<string, string>> sources)
    // {
    //     // Arrange
    //
    //     // Act
    //     var actual = DataConversionManager.AddConvertedTablesToList(converters, sources);
    //     // Assert
    //     
    // }

    // public static IEnumerable<object[]> TablesAndConvertersAndSources()
    // {
    //     var firstTables = new List<DataTable>()
    //     {
    //         new(),
    //         new()
    //     };
    //     
    // }
}