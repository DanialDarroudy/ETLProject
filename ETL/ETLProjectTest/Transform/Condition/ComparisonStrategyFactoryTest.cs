using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.ComparisonStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition;

public class ComparisonStrategyFactoryTest
{
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectComparisonStrategy))]
    public void CreateStrategy_ShouldReturnCorrectComparisonStrategyType_WhenParameterSpecifyType(
        IComparisonStrategy expected, char type)
    {
        // Arrange

        // Act
        var actual = ComparisonStrategyFactory.CreateStrategy(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectComparisonStrategy()
    {
        yield return [new EqualStrategy(), '='];
        yield return [new LessThanStrategy(), '<'];
        yield return [new MoreThanStrategy(), '>'];
    }

    [Fact]
    public void CreateStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const char type = '!';
        // Act
        var action = () => ComparisonStrategyFactory.CreateStrategy(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ComparisonStrategyFactory.NotExistTypeError(type));
    }

    [Fact]
    public void NotExistTypeError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const char type = '?';
        var expected = $"Unknown Comparison strategy type: {type}";
        // Act
        var actual = ComparisonStrategyFactory.NotExistTypeError(type);
        // Assert
        actual.Should().Be(expected);
    }
}