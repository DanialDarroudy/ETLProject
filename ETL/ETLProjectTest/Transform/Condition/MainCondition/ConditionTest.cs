using System.Data;
using ETLProject.Transform.Condition.Composite;

namespace ETLProjectTest.Transform.Condition.MainCondition;

public class ConditionTest
{
    [Theory]
    [MemberData(nameof(RootAndTableThatPerformFilterHasRows))]
    public void ApplyCondition_ShouldReturnNotEmptyTable_WhenPerformFilterHasRows(DataTable expected
        , DataTable table, IComponentCondition root)
    {
        // Arrange
        var condition = new ETLProject.Transform.Condition.MainCondition.Condition(root);
        // Act
        var actual = condition.ApplyCondition(table);
        // Assert
        AssertTwoTable.AreEqualTwoTable(expected, actual);
    }

    public static IEnumerable<object[]> RootAndTableThatPerformFilterHasRows()
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

        var firstRoot = new LeafCondition("Age = 15");
        var firstExpected = new DataTable("Students");
        firstExpected.Columns.Add("FirstName", typeof(string));
        firstExpected.Columns.Add("Age", typeof(int));
        firstExpected.Columns.Add("Average", typeof(double));
        firstExpected.Rows.Add("John", 15, 4);
        firstExpected.Rows.Add("Charlie", 15, 20);
        yield return [firstExpected, table, firstRoot];

        var secondRoot =
            new CompositeCondition([new LeafCondition("Age > 15"), new LeafCondition("Average < 8")], "And");
        var secondExpected = new DataTable("Students");
        secondExpected.Columns.Add("FirstName", typeof(string));
        secondExpected.Columns.Add("Age", typeof(int));
        secondExpected.Columns.Add("Average", typeof(double));
        secondExpected.Rows.Add("Jane", 16, 6);
        yield return [secondExpected, table, secondRoot];

        var thirdRoot = new CompositeCondition([
            new CompositeCondition(
                [new LeafCondition("Age = 16"), new LeafCondition("Average > 9")]
                , "And"),
            new LeafCondition("FirstName = 'Charlie'")
        ], "Or");
        var thirdExpected = new DataTable("Students");
        thirdExpected.Columns.Add("FirstName", typeof(string));
        thirdExpected.Columns.Add("Age", typeof(int));
        thirdExpected.Columns.Add("Average", typeof(double));
        thirdExpected.Rows.Add("Bob", 16, 10);
        thirdExpected.Rows.Add("Charlie", 15, 20);
        yield return [thirdExpected, table, thirdRoot];

        var fourthRoot = new CompositeCondition([
            new CompositeCondition(
                [new LeafCondition("Age = 16"), new LeafCondition("Average > 10")]
                , "And"),
            new LeafCondition("FirstName = 'Charlie'")
        ], "Or");
        var fourthExpected = new DataTable("Students");
        fourthExpected.Columns.Add("FirstName", typeof(string));
        fourthExpected.Columns.Add("Age", typeof(int));
        fourthExpected.Columns.Add("Average", typeof(double));
        fourthExpected.Rows.Add("Charlie", 15, 20);
        yield return [fourthExpected, table, fourthRoot];
    }


    [Theory]
    [MemberData(nameof(RootAndTableThatPerformFilterDoesNotHaveRows))]
    public void ApplyCondition_ShouldReturnEmptyTable_WhenPerformFilterDoesNotHaveRows(DataTable expected
        , DataTable table, IComponentCondition root)
    {
        // Arrange
        var condition = new ETLProject.Transform.Condition.MainCondition.Condition(root);
        // Act
        var actual = condition.ApplyCondition(table);
        // Assert
        AssertTwoTable.AreEqualTwoTable(expected, actual);
    }

    public static IEnumerable<object[]> RootAndTableThatPerformFilterDoesNotHaveRows()
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

        var firstRoot = new LeafCondition("Age = 20");
        var firstExpected = new DataTable("Students");
        firstExpected.Columns.Add("FirstName", typeof(string));
        firstExpected.Columns.Add("Age", typeof(int));
        firstExpected.Columns.Add("Average", typeof(double));
        yield return [firstExpected, table, firstRoot];

        var secondRoot =
            new CompositeCondition([new LeafCondition("Age > 15"), new LeafCondition("Average = 20")], "And");
        var secondExpected = new DataTable("Students");
        secondExpected.Columns.Add("FirstName", typeof(string));
        secondExpected.Columns.Add("Age", typeof(int));
        secondExpected.Columns.Add("Average", typeof(double));
        yield return [secondExpected, table, secondRoot];
        
        var fourthRoot = new CompositeCondition([
            new CompositeCondition(
                [new LeafCondition("Age = 16"), new LeafCondition("Average > 10")]
                , "And"),
            new LeafCondition("FirstName = 'Danial'")
        ], "Or");
        var fourthExpected = new DataTable("Students");
        fourthExpected.Columns.Add("FirstName", typeof(string));
        fourthExpected.Columns.Add("Age", typeof(int));
        fourthExpected.Columns.Add("Average", typeof(double));
        yield return [fourthExpected, table, fourthRoot];
    }
}