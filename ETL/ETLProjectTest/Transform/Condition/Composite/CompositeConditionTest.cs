using System.Data;
using ETLProject.Transform;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.Composite;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.Composite;

public class CompositeConditionTest
{
    [Theory]
    [MemberData(nameof(RowsAndChildrenAndTypeAndTableThatAnswerHasRows))]
    public void PerformFilter_ShouldReturnRowsOfAnswer_WhenOperateOnChildren(List<DataRow> expected
        , List<IComponentCondition> children, string strategyType, DataTable table)
    {
        // Arrange
        var composite = new CompositeCondition(children, strategyType);
        // Act
        var actual = composite.PerformFilter(table);
        // Assert
        actual.Count.Should().Be(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            actual[i].Should().Be(expected[i]);
        }
    }

    public static IEnumerable<object[]> RowsAndChildrenAndTypeAndTableThatAnswerHasRows()
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

        var firstChildren = new List<IComponentCondition>()
        {
            new LeafCondition("Age = 17"),
            new LeafCondition("Average = 20")
        };
        const string firstStrategyType = "Or";
        var firstRows = new List<DataRow>()
        {
            table.Rows[2],
            table.Rows[4]
        };
        yield return [firstRows, firstChildren, firstStrategyType, table];

        var secondChildren = new List<IComponentCondition>()
        {
            new CompositeCondition([new LeafCondition("Age > 15"), new LeafCondition("Average < 8")], "And"),
            new LeafCondition("FirstName = 'Bob'")
        };
        const string secondStrategyType = "Or";
        var secondRows = new List<DataRow>()
        {
            table.Rows[1],
            table.Rows[3]
        };
        yield return [secondRows, secondChildren, secondStrategyType, table];

        var thirdChildren = new List<IComponentCondition>()
        {
            new LeafCondition("FirstName = 'Alice'")
        };
        const string thirdStrategyType = "Or";
        var thirdRows = new List<DataRow>()
        {
            table.Rows[2],
        };
        yield return [thirdRows, thirdChildren, thirdStrategyType, table];
    }

    [Theory]
    [MemberData(nameof(RowsAndChildrenAndTypeAndTableThatAnswerDoesNotHaveRows))]
    public void PerformFilter_ShouldReturnEmptyRowList_WhenOperateOnChildrenHasNoRows(List<IComponentCondition> children
        , string strategyType, DataTable table)
    {
        // Arrange
        var composite = new CompositeCondition(children, strategyType);
        // Act
        var actual = composite.PerformFilter(table);
        // Assert
        actual.Should().BeEmpty();
    }

    public static IEnumerable<object[]> RowsAndChildrenAndTypeAndTableThatAnswerDoesNotHaveRows()
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

        var firstChildren = new List<IComponentCondition>()
        {
            new LeafCondition("Age = 18")
        };
        const string firstStrategyType = "Or";
        yield return [firstChildren, firstStrategyType, table];

        var secondChildren = new List<IComponentCondition>()
        {
            new CompositeCondition(
                [new LeafCondition("Age = 16"), new LeafCondition("Average > 10")]
                , "And"),
            new LeafCondition("FirstName = 'Danial'")
        };
        const string secondStrategyType = "Or";
        yield return [secondChildren, secondStrategyType, table];
    }

    [Fact]
    public void PerformFilter_ShouldReturnArgumentException_WhenDoesNotHaveChild()
    {
        // Arrange
        var composite = new CompositeCondition([], "Or");
        // Act
        var action = () => composite.PerformFilter(new DataTable());
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.EmptyListError(typeof(IComponentCondition)));
    }

    [Fact]
    public void PerformFilter_ShouldReturnArgumentException_WhenOperatorIsInvalid()
    {
        // Arrange
        const string strategyType = "Xor";
        var composite = new CompositeCondition([new LeafCondition("Age = 15")], strategyType);
        // Act
        var action = () => composite.PerformFilter(new DataTable());
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            OperatorStrategyFactory.NotExistTypeError(strategyType));
    }
}