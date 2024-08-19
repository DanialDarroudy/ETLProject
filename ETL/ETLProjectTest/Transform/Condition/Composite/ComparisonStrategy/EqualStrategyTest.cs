using ETLProject.Transform.Condition.Composite.ComparisonStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.Composite.ComparisonStrategy;

public class EqualStrategyTest
{
    [Theory]
    [MemberData(nameof(ProvideParametersWhichBeTrue))]
    public void Compare_ShouldReturnTrue_WhenTwoParametersIsEqual(object source, string target)
    {
        // Arrange
        var equal = new EqualStrategy();
        // Act
        var result = equal.Compare(source, target);
        // Assert
        result.Should().BeTrue();
    }

    public static IEnumerable<object[]> ProvideParametersWhichBeTrue()
    {
        const string firstSource = "Ali";
        const string firstTarget = "Ali";
        yield return [firstSource, firstTarget];

        const int secondSource = 25;
        const string secondTarget = "25";
        yield return [secondSource, secondTarget];

        const double thirdSource = 25.7;
        const string thirdTarget = "25.7";
        yield return [thirdSource, thirdTarget];
        
        const char fourthSource = '!';
        const string fourthTarget = "!";
        yield return [fourthSource, fourthTarget];
        
        const double fifthSource = 25.0;
        const string fifthTarget = "25";
        yield return [fifthSource, fifthTarget];
    }
    
    [Theory]
    [MemberData(nameof(ProvideParametersWhichBeFalse))]
    public void Compare_ShouldReturnFalse_WhenTwoParametersIsNotEqual(object source, string target)
    {
        // Arrange
        var equal = new EqualStrategy();
        // Act
        var result = equal.Compare(source, target);
        // Assert
        result.Should().BeFalse();
    }
    public static IEnumerable<object[]> ProvideParametersWhichBeFalse()
    {
        const string firstSource = "'Ali'";
        const string firstTarget = "Ali";
        yield return [firstSource, firstTarget];

        const int secondSource = 25;
        const string secondTarget = "25.0";
        yield return [secondSource, secondTarget];

        const double thirdSource = 25.1;
        const string thirdTarget = "25";
        yield return [thirdSource, thirdTarget];
        
        const char fourthSource = '!';
        const string fourthTarget = "?";
        yield return [fourthSource, fourthTarget];
    }
}