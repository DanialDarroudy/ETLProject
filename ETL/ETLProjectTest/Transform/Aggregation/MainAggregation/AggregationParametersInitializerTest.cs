using System.Data;
using ETLProject.Deserialization;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using ETLProject.Transform.Aggregation.MainAggregation;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation.MainAggregation;

public class AggregationParametersInitializerTest
{
    [Theory]
    [MemberData(nameof(ProvideTablesAndDto))]
    public void InitializeAggregation_ShouldReturnInitializedAggregated_WhenDtoInParameters(AggregationDto dto
        , ETLProject.Transform.Aggregation.MainAggregation.Aggregation expected, List<DataTable> allTables)
    {
        // Arrange
        var initializer = new AggregationParametersInitializer();
        // Act
        var actual = initializer.InitializeAggregation(allTables, dto);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> ProvideTablesAndDto()
    {
        var firstTable = new DataTable("Students");
        firstTable.Columns.Add("FirstName", typeof(string));
        firstTable.Columns.Add("Age", typeof(int));
        firstTable.Columns.Add("Average", typeof(double));
        firstTable.Rows.Add("John", 15, 4);
        firstTable.Rows.Add("Jane", 16, 6);
        firstTable.Rows.Add("Alice", 17, 8);
        firstTable.Rows.Add("Bob", 16, 10);
        firstTable.Rows.Add("Charlie", 15, 20);
        var secondTable = new DataTable("Customers");
        secondTable.Columns.Add("Balance", typeof(int));
        secondTable.Columns.Add("Name", typeof(string));
        secondTable.Columns.Add("Address", typeof(string));
        secondTable.Rows.Add(6000, "John Doe", "Tarasht");
        secondTable.Rows.Add(10000, "Jane Smith", "Azadi");
        secondTable.Rows.Add(2500, "Bob Johnson", "Tarasht");
        secondTable.Rows.Add(7500, "Sarah Lee", "Azadi");
        secondTable.Rows.Add(1000, "Tom Wilson", "Azadi");
        secondTable.Rows.Add(15000, "Emily Davis", "Azadi");
        secondTable.Rows.Add(3000, "Michael Brown", "Tarasht");
        secondTable.Rows.Add(8000, "Jessica Taylor", "Tarasht");
        secondTable.Rows.Add(4500, "David Anderson", "Tarasht");
        secondTable.Rows.Add(12000, "Olivia Martinez", "Tarasht");
        var allTables = new List<DataTable>()
        {
            firstTable,
            secondTable
        };

        var firstDto = new AggregationDto
        {
            TableName = "Students",
            StrategyType = "Sum",
            AggregatedColumnName = "Average",
            GroupedByColumnNames = ["Age"]
        };
        var firstAggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(firstTable
            , [firstTable.Columns["Age"]!], firstTable.Columns["Average"]! , new SumStrategy());
        yield return [firstDto , firstAggregation , allTables];

        var secondDto = new AggregationDto
        {
            TableName = "Customers",
            StrategyType = "Count",
            AggregatedColumnName = "Name",
            GroupedByColumnNames = ["Address"]
        };
        var secondAggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(secondTable
            , [secondTable.Columns["Address"]!], firstTable.Columns["Name"]! , new CountStrategy());
        yield return [secondDto , secondAggregation , allTables];
    }
}