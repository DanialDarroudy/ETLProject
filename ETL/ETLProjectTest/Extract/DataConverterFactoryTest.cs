using ETLProject.Extract;
using ETLProject.Extract.DataConverterAdaptor;
using FluentAssertions;

namespace ETLProjectTest.Extract;

public class DataConverterFactoryTest
{
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectDataConverter))]
    public void CreateConverter_ShouldReturnCorrectDataConverterType_WhenParameterSpecifyType(
        IDataConverter expected, string type)
    {
        // Arrange

        // Act
        var actual = DataConverterFactory.CreateConverter(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectDataConverter()
    {
        yield return [new CsvDataConverter(), "Csv"];
        yield return [new SqlDataConverter(), "Sql"];
    }
    [Fact]
    public void CreateConverter_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const string type = "Xml";
        // Act
        var action = () => DataConverterFactory.CreateConverter(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            DataConverterFactory.NotExistTypeError(type));
    }

    [Fact]
    public void NotExistTypeError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string type = "Xml";
        const string expected = $"converter type {type} is Invalid";
        // Act
        var actual = DataConverterFactory.NotExistTypeError(type);
        // Assert
        actual.Should().Be(expected);
    }
}