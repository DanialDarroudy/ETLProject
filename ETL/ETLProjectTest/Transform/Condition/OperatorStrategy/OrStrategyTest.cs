using System.Data;
using ETLProject.Transform.Condition.OperatorStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Condition.OperatorStrategy;

public class OrStrategyTest
{
    [Theory]
    [MemberData(nameof(CreateListOfRowsAndAnswer))]
    public void Operate_ShouldReturnUnionOfParameter_WhenParameterIsListOfRowList(List<DataRow> expected
        , List<List<DataRow>> rows)
    {
        // Arrange
        var strategy = new OrStrategy();
        // Act
        var actual = strategy.Operate(rows);
        // Assert
        actual.Count.Should().Be(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            actual[i].Should().Be(expected[i]);
        }
    }

    public static IEnumerable<object[]> CreateListOfRowsAndAnswer()
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

        var firstList = new List<List<DataRow>>()
        {
            new()
            {
                table.Rows[0],
                table.Rows[1],
            },
            new()
            {
                table.Rows[0],
                table.Rows[2],
            }
        };
        var firstExpected = new List<DataRow>()
        {
            table.Rows[0],
            table.Rows[1],
            table.Rows[2]
        };
        yield return [firstExpected, firstList];
        
        var secondList = new List<List<DataRow>>()
        {
            new()
            {
                table.Rows[0],
                table.Rows[1],
            },
            new()
            {
                table.Rows[2],
                table.Rows[3],
            }
        };
        var secondExpected = new List<DataRow>()
        {
            table.Rows[0],
            table.Rows[1],
            table.Rows[2],
            table.Rows[3]
        };
        yield return [secondExpected, secondList];
        
        var thirdList = new List<List<DataRow>>()
        {
            new(),
            new()
            {
                table.Rows[2],
                table.Rows[3],
            }
        };
        var thirdExpected = new List<DataRow>()
        {
            table.Rows[2],
            table.Rows[3]
        };
        yield return [thirdExpected, thirdList];
        
        var fourthList = new List<List<DataRow>>()
        {
            new()
            {
                table.Rows[2],
                table.Rows[3],
            }
        };
        var fourthExpected = new List<DataRow>()
        {
            table.Rows[2],
            table.Rows[3]
        };
        yield return [fourthExpected, fourthList];
        
        var fifthList = new List<List<DataRow>>()
        {
            new(),
            new()
        };
        var fifthExpected = new List<DataRow>();
        yield return [fifthExpected, fifthList];
    }
}