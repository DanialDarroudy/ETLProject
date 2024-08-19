using System.Data;
using ETLProject.Transform;
using FluentAssertions;
using NSubstitute;

namespace ETLProjectTest.Transform;

public class InitialCheckTest
{
    [Fact]
    public void CheckNull_ShouldThrowArgumentException_WhenParameterIsNull()
    {
        // Arrange
        DataTable table = null!;
        // Act
        var action = () => InitialCheck.CheckNull(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage("The input DataTable cannot be null.");
    }
    [Fact]
    public void CheckNull_ShouldNotThrowArgumentException_WhenParameterInitialized()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => InitialCheck.CheckNull(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    [Fact]
    public void CheckEmpty_ShouldThrowArgumentException_WhenParameterDoNotHaveAnyRows()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => InitialCheck.CheckEmpty(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage("The input DataTable cannot be empty.");
    }
    [Fact]
    public void CheckEmpty_ShouldNotThrowArgumentException_WhenParameterHasRows()
    {
        // Arrange
        var table = new DataTable();
        var row = table.NewRow();
        table.Merge(row.Table);
        table.AcceptChanges();
        // Act
        var action = () => InitialCheck.CheckEmpty(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }
}