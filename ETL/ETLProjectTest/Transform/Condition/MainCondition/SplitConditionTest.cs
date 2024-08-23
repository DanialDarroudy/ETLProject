using ETLProject.Transform.Condition.MainCondition;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.MainCondition;

public class SplitConditionTest
{
    [Theory]
    [InlineData("Name", "Name = 'Ali'")]
    [InlineData("LastName", "LastName = 'Alavi'")]
    [InlineData("Age", "Age > 25")]
    [InlineData("FatherName", "FatherName = 'Ali'")]
    [InlineData("MotherName", "MotherName = 'Zahra'")]
    public void GetColumnName_ShouldReturnColumnName_WhenParameterIsCondition(string expected, string condition)
    {
        // Arrange

        // Act
        var actual = SplitCondition.GetColumnName(condition);
        // Assert
        actual.Should().Be(expected);
    }
    [Theory]
    [InlineData('=', "Name = 'Ali'")]
    [InlineData('=', "LastName = 'Alavi'")]
    [InlineData('>', "Age > 25")]
    [InlineData('=', "FatherName = 'Ali'")]
    [InlineData('=', "MotherName = 'Zahra'")]
    [InlineData('<', "Age < 25")]
    [InlineData('!', "Age ! 25")]

    public void GetOperator_ShouldReturnOperator_WhenParameterIsCondition(char expected, string condition)
    {
        // Arrange

        // Act
        var actual = SplitCondition.GetOperator(condition);
        // Assert
        actual.Should().Be(expected);
    }
    [Theory]
    [InlineData("Ali", "Name = 'Ali'")]
    [InlineData("Alavi", "LastName = 'Alavi'")]
    [InlineData("25", "Age > 25")]
    [InlineData("Ali", "FatherName = 'Ali'")]
    [InlineData("Zahra", "MotherName = 'Zahra'")]
    [InlineData("Ali Reza", "FatherName = 'Ali Reza'")]
    [InlineData("Math 3", "Course = 'Math 3'")]
    public void GetValue_ShouldReturnValue_WhenParameterIsCondition(string expected, string condition)
    {
        // Arrange

        // Act
        var actual = SplitCondition.GetValue(condition);
        // Assert
        actual.Should().Be(expected);
    }
}