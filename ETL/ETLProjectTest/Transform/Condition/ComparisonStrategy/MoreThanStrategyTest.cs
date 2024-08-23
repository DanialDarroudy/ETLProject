using ETLProject.Transform.Condition.ComparisonStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.ComparisonStrategy;

public class MoreThanStrategyTest
{
    [Theory]
    [MemberData(nameof(ProvideParametersWhichBeTrue))]
    public void Compare_ShouldReturnTrue_WhenFirstMoreThanSecond(object source, string target)
    {
        // Arrange
        var moreThan = new MoreThanStrategy();
        // Act
        var result = moreThan.Compare(source, target);
        // Assert
        result.Should().BeTrue();
    }

    public static IEnumerable<object[]> ProvideParametersWhichBeTrue()
    {
        const string firstSource = "25";
        const string firstTarget = "20";
        yield return [firstSource, firstTarget];

        const int secondSource = 25;
        const string secondTarget = "20";
        yield return [secondSource, secondTarget];

        const double thirdSource = 25.8;
        const string thirdTarget = "25.7";
        yield return [thirdSource, thirdTarget];

        const string fourthSource = "20.1";
        const string fourthTarget = "20";
        yield return [fourthSource, fourthTarget];

        const int fifthSource = 25;
        const string fifthTarget = "24.4";
        yield return [fifthSource, fifthTarget];
    }

    [Theory]
    [MemberData(nameof(ProvideParametersWhichBeFalse))]
    public void Compare_ShouldReturnFalse_WhenFirstEqualOrLessThanSecond(object source, string target)
    {
        // Arrange
        var moreThan = new MoreThanStrategy();
        // Act
        var result = moreThan.Compare(source, target);
        // Assert
        result.Should().BeFalse();
    }

    public static IEnumerable<object[]> ProvideParametersWhichBeFalse()
    {
        const string firstSource = "24";
        const string firstTarget = "25";
        yield return [firstSource, firstTarget];

        const int secondSource = 25;
        const string secondTarget = "25.0";
        yield return [secondSource, secondTarget];

        const double thirdSource = 25.1;
        const string thirdTarget = "26";
        yield return [thirdSource, thirdTarget];

        const string fourthSource = "25.4";
        const string fourthTarget = "26";
        yield return [fourthSource, fourthTarget];

        const double fifthSource = 25.0;
        const string fifthTarget = "25";
        yield return [fifthSource, fifthTarget];
    }
}