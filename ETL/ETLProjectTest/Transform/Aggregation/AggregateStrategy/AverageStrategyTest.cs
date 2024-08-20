using System.Data;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation.AggregateStrategy;

public class AverageStrategyTest
{
    [Theory]
    [MemberData(nameof(RowsAndColumn))]
    public void DoSpecificAggregate_ShouldReturnAverageValueOfSpecificColumnOfRows_WhenParameterAreRowsAndColumn(
        decimal expected , List<DataRow> rows , DataColumn column)
    {
        // Arrange
        var strategy = new AverageStrategy();
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
        yield return [16.6, firstRows, firstTable.Columns["Score"]!];
        
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
        yield return [17.4, secondRows, secondTable.Columns["Score"]!];

        var thirdTable = new DataTable();
        thirdTable.Columns.Add("Score");
        var thirdRows = new List<DataRow>()
        {
            thirdTable.NewRow(),
            thirdTable.NewRow(),
            thirdTable.NewRow(),
            thirdTable.NewRow(),
            thirdTable.NewRow()
        };
        thirdRows[0].ItemArray = ["18"];
        thirdRows[1].ItemArray = ["19"];
        thirdRows[2].ItemArray = ["20"];
        thirdRows[3].ItemArray = ["16"];
        thirdRows[4].ItemArray = ["14"];
        yield return [17.4, thirdRows, thirdTable.Columns["Score"]!];
        
        var fourthTable = new DataTable();
        fourthTable.Columns.Add("Score");
        var fourthRows = new List<DataRow>()
        {
            fourthTable.NewRow()
        };
        fourthRows[0].ItemArray = ["18"];
        yield return [18, fourthRows, fourthTable.Columns["Score"]!];
    }
}