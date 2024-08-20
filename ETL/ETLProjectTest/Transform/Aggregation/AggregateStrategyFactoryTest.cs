using ETLProject.Transform.Aggregation;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation;

public class AggregateStrategyFactoryTest
{
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectAggregateStrategy))]
    public void CreateStrategy_ShouldReturnCorrectAggregateStrategyType_WhenParameterSpecifyType(
        IAggregateStrategy expected, string type)
    {
        // Arrange

        // Act
        var actual = AggregateStrategyFactory.CreateStrategy(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectAggregateStrategy()
    {
        yield return [new SumStrategy(), "Sum"];
        yield return [new MinStrategy(), "Min"];
        yield return [new MaxStrategy(), "Max"];
        yield return [new CountStrategy(), "Count"];
        yield return [new AverageStrategy(), "Avg"];
    }

    [Fact]
    public void CreateStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const string type = "Average";
        // Act
        var action = () => AggregateStrategyFactory.CreateStrategy(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            AggregateStrategyFactory.NotExistTypeError(type));
    }

    [Fact]
    public void NotExistTypeError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string type = "Average";
        const string expected = $"Unknown aggregation strategy type: {type}";
        // Act
        var actual = AggregateStrategyFactory.NotExistTypeError(type);
        // Assert
        actual.Should().Be(expected);
    }
}