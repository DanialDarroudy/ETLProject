using ETLProject.Transform.Condition.Composite;
using ETLProject.Transform.Condition.Composite.OperatorStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.Composite;

public class OperatorStrategyFactoryTest
{
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectOperatorStrategy))]
    public void CreateStrategy_ShouldReturnCorrectOperatorStrategyType_WhenParameterSpecifyType(
        IOperatorStrategy expected, string type)
    {
        // Arrange

        // Act
        var actual = OperatorStrategyFactory.CreateStrategy(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectOperatorStrategy()
    {
        yield return [new AndStrategy(), "And"];
        yield return [new OrStrategy(), "Or"];
    }

    [Fact]
    public void CreateStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const string type = "Xor";
        // Act
        var action = () => OperatorStrategyFactory.CreateStrategy(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            OperatorStrategyFactory.NotExistTypeError(type));
    }

    [Fact]
    public void NotExistTypeError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string type = "And";
        const string expected = $"Unknown Operator strategy type: {type}";
        // Act
        var actual = OperatorStrategyFactory.NotExistTypeError(type);
        // Assert
        actual.Should().Be(expected);
    }
}