using System.Data;
using ETLProject.Transform;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.Composite;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.Composite;

public class LeafConditionTest
{
    [Theory]
    [MemberData(nameof(RowsAndConditionAndTable))]
    public void PerformFilter_ShouldReturnRowsOfAnswer_WhenApplyCondition(List<DataRow> expected
        , string condition , DataTable table)
    {
        // Arrange
        var leaf = new LeafCondition(condition);
        // Act
        var actual = leaf.PerformFilter(table);
        // Assert
        actual.Count.Should().Be(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            actual[i].Should().Be(expected[i]);
        }
    }

    public static IEnumerable<object[]> RowsAndConditionAndTable()
    {
        var table = new DataTable("Students");
        table.Columns.Add("FirstName", typeof(string));
        table.Columns.Add("Age", typeof(int));
        table.Columns.Add("Average", typeof(double));
        table.Rows.Add("John", 15, 4);
        table.Rows.Add("Jane", 16, 6);
        table.Rows.Add("Alice", 17, 8);
        table.Rows.Add("Bob", 16, 10);
        table.Rows.Add("Charlie", 15, 20);

        const string firstCondition = "Age < 16";
        var firstRows = new List<DataRow>()
        {
            table.Rows[0],
            table.Rows[4]
        };
        yield return [firstRows , firstCondition , table];
        
        const string secondCondition = "Average > 16";
        var secondRows = new List<DataRow>()
        {
            table.Rows[4]
        };
        yield return [secondRows , secondCondition , table];
        
        const string thirdCondition = "FirstName = 'John'";
        var thirdRows = new List<DataRow>()
        {
            table.Rows[0]
        };
        yield return [thirdRows , thirdCondition , table];
        
        const string fourthCondition = "FirstName = 'Danial'";
        var fourthRows = new List<DataRow>();
        yield return [fourthRows , fourthCondition , table];
    }

    [Fact]
    public void PerformFilter_ShouldThrowArgumentException_WhenComparisonOfConditionIsInvalid()
    {
        // Arrange
        const char type = '?';
        var leaf = new LeafCondition($"Age {type} 15");
        // Act
        var action = () => leaf.PerformFilter(new DataTable());
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ComparisonStrategyFactory.NotExistTypeError(type));
    }
    [Fact]
    public void PerformFilter_ShouldThrowArgumentException_WhenTableIsNull()
    {
        // Arrange
        var leaf = new LeafCondition("Age = 15");
        // Act
        var action = () => leaf.PerformFilter(null!);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.NullTableError);
    }
    [Fact]
    public void PerformFilter_ShouldThrowArgumentException_WhenTableIsEmpty()
    {
        // Arrange
        var leaf = new LeafCondition("Age = 15");
        // Act
        var action = () => leaf.PerformFilter(new DataTable());
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.EmptyTableError);
    }
}