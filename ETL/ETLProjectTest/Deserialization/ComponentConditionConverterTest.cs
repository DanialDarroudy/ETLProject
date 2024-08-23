using System.Text;
using System.Text.Json;
using ETLProject.Deserialization;
using ETLProject.Transform.Condition.Composite;
using FluentAssertions;

namespace ETLProjectTest.Deserialization;

public class ComponentConditionConverterTest
{
    [Theory]
    [MemberData(nameof(ProvideJsonAndType))]
    public void Read_ShouldDeserializeIComponentCondition_WhenParameterIsJson(Type expected, string json)
    {
        // Arrange
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        var converter = new ComponentConditionConverter();
        var options = new JsonSerializerOptions();
        // Act
        var result = converter.Read(ref reader, typeof(IComponentCondition), options);

        // Assert
        result.Should().BeOfType(expected);
    }

    public static IEnumerable<object[]> ProvideJsonAndType()
    {
        const string firstJson = """
                                  
                                              {
                                                  "Type": "LeafCondition",
                                                  "Condition": "Average > 10"
                                              }
                                  """;
        var firstType = typeof(LeafCondition);
        yield return [firstType, firstJson];
    }
}