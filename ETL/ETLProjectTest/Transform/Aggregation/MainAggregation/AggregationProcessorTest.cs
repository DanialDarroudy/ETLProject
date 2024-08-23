using System.Data;
using ETLProject.Transform.Aggregation.MainAggregation;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation.MainAggregation;

public class AggregationProcessorTest
{
    [Theory]
    [MemberData(nameof(ProvideDictionaryAndTableAndColumns))]
    public void GroupByColumns_ShouldReturnDictionaryOfGroupedRows_WhenParametersAreProvided(
        Dictionary<string, List<DataRow>> expected, DataTable table, List<DataColumn> groupedBys)
    {
        // Arrange
        var processor = new AggregationProcessor();
        // Act
        var actual = processor.GroupByColumns(table, groupedBys);
        // Assert
        actual.Should().BeEquivalentTo(expected);
    }

    public static IEnumerable<object[]> ProvideDictionaryAndTableAndColumns()
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

        var firstGroupedBys = new List<DataColumn>()
        {
            firstTable.Columns["Age"]!,
            firstTable.Columns["FirstName"]!
        };
        var firstDictionary = new Dictionary<string, List<DataRow>>()
        {
            { "15,John", [firstTable.Rows[0]] },
            { "16,Jane", [firstTable.Rows[1]] },
            { "17,Alice", [firstTable.Rows[2]] },
            { "16,Bob", [firstTable.Rows[3]] },
            { "15,Charlie", [firstTable.Rows[4]] }
        };
        yield return [firstDictionary, firstTable, firstGroupedBys];

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
        var secondGroupedBys = new List<DataColumn>()
        {
            secondTable.Columns["Address"]!
        };
        var secondDictionary = new Dictionary<string, List<DataRow>>()
        {
            {
                "Tarasht",
                [
                    secondTable.Rows[0], secondTable.Rows[2], secondTable.Rows[6], secondTable.Rows[7],
                    secondTable.Rows[8], secondTable.Rows[9]
                ]
            },
            {
                "Azadi",
                [
                    secondTable.Rows[1], secondTable.Rows[3], secondTable.Rows[4], secondTable.Rows[5]
                ]
            }
        };
        yield return [secondDictionary, secondTable, secondGroupedBys];
    }

    [Theory]
    [MemberData(nameof(ProvideRowAndGroupKey))]
    public void PopulateRowWithGroupValues_ShouldUpdateRow_WhenParameterIsGroupedValue(DataRow expected
        , DataTable table, string groupKey)
    {
        // Arrange
        var processor = new AggregationProcessor();
        var actual = table.NewRow();
        // Act
        processor.PopulateRowWithGroupValues(actual, groupKey);
        // Assert
        actual.Should().BeEquivalentTo(expected);
    }

    public static IEnumerable<object[]> ProvideRowAndGroupKey()
    {
        var firstTable = new DataTable("Students");
        firstTable.Columns.Add("Age", typeof(int));
        firstTable.Columns.Add("FirstName", typeof(string));
        firstTable.Columns.Add("Average", typeof(double));

        var firstRow = firstTable.NewRow();
        firstRow.ItemArray = [15, "John"];
        const string firstKey = "15,John";
        yield return [firstRow, firstTable, firstKey];
        
        var secondTable = new DataTable("Customers");
        secondTable.Columns.Add("Address", typeof(string));
        secondTable.Columns.Add("Balance", typeof(int));
        
        var secondRow = secondTable.NewRow();
        secondRow.ItemArray = ["Tarasht"];
        const string secondKey = "Tarasht";
        yield return [secondRow, secondTable, secondKey];
    }
}