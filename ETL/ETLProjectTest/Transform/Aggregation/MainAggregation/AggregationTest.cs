using System.Data;
using ETLProject.Transform;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform.Aggregation.MainAggregation;

public class AggregationTest
{
    [Fact]
    public void Aggregate_ShouldReturnArgumentException_WhenTableIsNull()
    {
        // Arrange
        var aggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(null!
            , [], new DataColumn(), new AverageStrategy());
        // Act
        var action = () => aggregation.Aggregate();
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(ObjectCheck.NullTableError);
    }

    [Fact]
    public void Aggregate_ShouldReturnArgumentException_WhenTableIsEmpty()
    {
        // Arrange
        var aggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(new DataTable()
            , [], new DataColumn(), new AverageStrategy());
        // Act
        var action = () => aggregation.Aggregate();
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(ObjectCheck.EmptyTableError);
    }

    [Fact]
    public void Aggregate_ShouldReturnArgumentException_WhenListOfGroupedBysIsEmpty()
    {
        // Arrange
        var table = new DataTable();
        table.Columns.Add("A");
        table.Rows.Add(new object());
        var aggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(table
            , [], new DataColumn(), new AverageStrategy());
        // Act
        var action = () => aggregation.Aggregate();
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.EmptyListError(typeof(DataColumn)));
    }

    [Theory]
    [MemberData(nameof(ProvideParametersOfTable))]
    public void Aggregate_ShouldReturnTableOfAnswer_WhenParametersAreProvided(DataTable expected
        , DataTable table, List<DataColumn> groupedBys, DataColumn aggregated, IAggregateStrategy strategy)
    {
        // Arrange 
        var aggregation = new ETLProject.Transform.Aggregation.MainAggregation.Aggregation(table
            , groupedBys, aggregated, strategy);
        // Act
        var actual = aggregation.Aggregate();
        // Assert
        AssertTwoTable.AreEqualTwoTable(expected, actual);
    }

    public static IEnumerable<object[]> ProvideParametersOfTable()
    {
        var mainFirstTable = new DataTable("Students");
        mainFirstTable.Columns.Add("FirstName", typeof(string));
        mainFirstTable.Columns.Add("Age", typeof(int));
        mainFirstTable.Columns.Add("Average", typeof(double));
        mainFirstTable.Rows.Add("John", 15, 4);
        mainFirstTable.Rows.Add("Jane", 16, 6);
        mainFirstTable.Rows.Add("Alice", 17, 8);
        mainFirstTable.Rows.Add("Bob", 16, 10);
        mainFirstTable.Rows.Add("Charlie", 15, 20);

        var mainSecondTable = new DataTable("Customers");
        mainSecondTable.Columns.Add("Balance", typeof(int));
        mainSecondTable.Columns.Add("Name", typeof(string));
        mainSecondTable.Columns.Add("Address", typeof(string));
        mainSecondTable.Rows.Add(6000, "John Doe", "Tarasht");
        mainSecondTable.Rows.Add(10000, "Jane Smith", "Azadi");
        mainSecondTable.Rows.Add(2500, "Bob Johnson", "Tarasht");
        mainSecondTable.Rows.Add(7500, "Sarah Lee", "Azadi");
        mainSecondTable.Rows.Add(1000, "Tom Wilson", "Azadi");
        mainSecondTable.Rows.Add(15000, "Emily Davis", "Azadi");
        mainSecondTable.Rows.Add(3000, "Michael Brown", "Tarasht");
        mainSecondTable.Rows.Add(8000, "Jessica Taylor", "Tarasht");
        mainSecondTable.Rows.Add(4500, "David Anderson", "Tarasht");
        mainSecondTable.Rows.Add(12000, "Olivia Martinez", "Tarasht");

        var firstGroupedBys = new List<DataColumn>() { mainFirstTable.Columns["Age"]! };
        var firstAggregated = mainFirstTable.Columns["Average"]!;
        var firstStrategy = new MinStrategy();
        var firstTable = new DataTable("Students");
        firstTable.Columns.Add("Age", typeof(int));
        firstTable.Columns.Add("Average", typeof(double));
        firstTable.Rows.Add(15, 4);
        firstTable.Rows.Add(16, 6);
        firstTable.Rows.Add(17, 8);
        yield return [firstTable, mainFirstTable, firstGroupedBys, firstAggregated, firstStrategy];

        var secondGroupedBys = new List<DataColumn>() { mainFirstTable.Columns["Age"]! };
        var secondAggregated = mainFirstTable.Columns["Average"]!;
        var secondStrategy = new MaxStrategy();
        var secondTable = new DataTable("Students");
        secondTable.Columns.Add("Age", typeof(int));
        secondTable.Columns.Add("Average", typeof(double));
        secondTable.Rows.Add(15, 20);
        secondTable.Rows.Add(16, 10);
        secondTable.Rows.Add(17, 8);
        yield return [secondTable, mainFirstTable, secondGroupedBys, secondAggregated, secondStrategy];

        var thirdGroupedBys = new List<DataColumn>()
            { mainFirstTable.Columns["FirstName"]!, mainFirstTable.Columns["Age"]! };
        var thirdAggregated = mainFirstTable.Columns["Average"]!;
        var thirdStrategy = new CountStrategy();
        var thirdTable = new DataTable("Students");
        thirdTable.Columns.Add("FirstName", typeof(string));
        thirdTable.Columns.Add("Age", typeof(int));
        thirdTable.Columns.Add("Average", typeof(double));
        thirdTable.Rows.Add("John", 15, 1);
        thirdTable.Rows.Add("Jane", 16, 1);
        thirdTable.Rows.Add("Alice", 17, 1);
        thirdTable.Rows.Add("Bob", 16, 1);
        thirdTable.Rows.Add("Charlie", 15, 1);
        yield return [thirdTable , mainFirstTable , thirdGroupedBys, thirdAggregated, thirdStrategy];

        var fourthGroupedBys = new List<DataColumn>(){ mainSecondTable.Columns["Address"]! };
        var fourthAggregated = mainSecondTable.Columns["Balance"]!;
        var fourthStrategy = new SumStrategy();
        var fourthTable = new DataTable("Customers");
        fourthTable.Columns.Add("Address", typeof(string));
        fourthTable.Columns.Add("Balance", typeof(int));
        fourthTable.Rows.Add("Tarasht" , 36000);
        fourthTable.Rows.Add("Azadi" , 33500);
        yield return [fourthTable , mainSecondTable , fourthGroupedBys, fourthAggregated, fourthStrategy];
        
        var fifthGroupedBys = new List<DataColumn>(){ mainSecondTable.Columns["Address"]! };
        var fifthAggregated = mainSecondTable.Columns["Balance"]!;
        var fifthStrategy = new AverageStrategy();
        var fifthTable = new DataTable("Customers");
        fifthTable.Columns.Add("Address", typeof(string));
        fifthTable.Columns.Add("Balance", typeof(int));
        fifthTable.Rows.Add("Tarasht" , 6000);
        fifthTable.Rows.Add("Azadi" , 8375);
        yield return [fifthTable , mainSecondTable , fifthGroupedBys, fifthAggregated, fifthStrategy];
    }
}