using System.Data;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation.AggregateStrategy;

public class CountStrategyTest
{
    [Theory]
    [MemberData(nameof(RowsAndColumn))]
    public void DoSpecificAggregate_ShouldReturnCountOfRowsThatInOneGroup_WhenParameterAreRowsAndColumn(
        decimal expected , List<DataRow> rows , DataColumn column)
    {
        // Arrange
        var strategy = new CountStrategy();
        // Act
        var actual = strategy.DoSpecificAggregate(rows, column);
        // Assert
        actual.Should().Be(expected);
    }

    public static IEnumerable<object[]> RowsAndColumn()
    {
        var firstTable = new DataTable();
        firstTable.Columns.Add("FirstName");
        firstTable.Columns.Add("Address");
        firstTable.Columns.Add("Score");
        var firstRows = new List<DataRow>()
        {
            firstTable.NewRow(),
            firstTable.NewRow(),
            firstTable.NewRow(),
            firstTable.NewRow(),
            firstTable.NewRow()
        };
        firstRows[0].ItemArray = ["Danial", "Tarasht", "18"];
        firstRows[1].ItemArray = ["Danial", "Tarasht", "19"];
        firstRows[2].ItemArray = ["Danial", "Tarasht", "20"];
        firstRows[3].ItemArray = ["Danial", "Tarasht", "12"];
        firstRows[4].ItemArray = ["Danial", "Tarasht", "14"];
        yield return [5, firstRows, firstTable.Columns["FirstName"]!];
        
        var secondTable = new DataTable();
        secondTable.Columns.Add("FirstName");
        secondTable.Columns.Add("Course");
        secondTable.Columns.Add("Score");
        var secondRows = new List<DataRow>()
        {
            secondTable.NewRow(),
            secondTable.NewRow(),
            secondTable.NewRow(),
            secondTable.NewRow(),
            secondTable.NewRow()
        };
        secondRows[0].ItemArray = ["Danial", "Math", "18"];
        secondRows[1].ItemArray = ["Ali", "Math", "19"];
        secondRows[2].ItemArray = ["Mamad", "Math", "20"];
        secondRows[3].ItemArray = ["Jason", "Math", "16"];
        secondRows[4].ItemArray = ["Mary", "Math", "14"];
        yield return [5, secondRows, secondTable.Columns["Course"]!];
        
        var fourthTable = new DataTable();
        fourthTable.Columns.Add("Score");
        var fourthRows = new List<DataRow>()
        {
            fourthTable.NewRow()
        };
        fourthRows[0].ItemArray = ["18"];
        yield return [1, fourthRows, fourthTable.Columns["Score"]!];
    }
}