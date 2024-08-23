using System.Data;
using ETLProject.Transform.Aggregation.MainAggregation;

namespace ETLProjectTest.Transform.Aggregation.MainAggregation;

public class TableInitializerTest
{
    [Theory]
    [MemberData(nameof(TableThatHaveColumns))]
    public void InitializeTable_ShouldCreateEmptyTableWithSpecifiedColumns_WhenParametersAreColumns(DataTable expected
        , List<DataColumn> groupedBys, DataColumn aggregated)
    {
        // Arrange
        var initializer = new TableInitializer();
        // Act
        
        var actual = initializer.InitializeTable(groupedBys, aggregated);
        //Assert
        
        AssertTwoTable.AreEqualTwoTable(expected, actual);
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