using System.Data;
using ETLProject.Transform.Aggregation;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation;

public class TableInitializerTest
{
    [Theory]
    [MemberData(nameof(TableThatHaveColumns))]
    public void InitializeTable_ShouldCreateEmptyTableWithSpecifiedColumns_WhenParametersAreColumns(DataTable expected
        , List<DataColumn> groupedBys, DataColumn aggregated)
    {
        // Arrange
        
        // Act
        
        var actual = TableInitializer.InitializeTable(groupedBys, aggregated);
        //Assert
        
        actual.Should().NotBeNull();
        actual.Columns.Count.Should().Be(expected.Columns.Count);
        for (var i = 0; i < actual.Columns.Count; i++)
        {
            actual.Columns[i].ColumnName.Should().Be(expected.Columns[i].ColumnName);
        }
    }

    public static IEnumerable<object[]> TableThatHaveColumns()
    {
        var firstGroupedBysColumns = new List<DataColumn>()
        {
            new("ID"),
            new("Grade"),
            new("FirstName")
        };
        var firstAggregatedColumn = new DataColumn("Average");
        var firstTable = new DataTable();
        firstGroupedBysColumns.ForEach(column => firstTable.Columns.Add(column));
        firstTable.Columns.Add(firstAggregatedColumn);
        
        yield return [firstTable, firstGroupedBysColumns, firstAggregatedColumn];
        
        var secondGroupedBysColumns = new List<DataColumn>();
        var secondAggregatedColumn = new DataColumn("Average");
        var secondTable = new DataTable();
        secondTable.Columns.Add(secondAggregatedColumn);
        
        yield return [secondTable, secondGroupedBysColumns, secondAggregatedColumn];
    }
}