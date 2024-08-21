using System.Data;
using ETLProject.Transform;
using ETLProject.Transform.Aggregation;
using ETLProject.Transform.Aggregation.AggregateStrategy;
using ETLProject.Transform.Condition.Composite;
using ETLProject.Transform.Condition.Composite.ComparisonStrategy;
using ETLProject.Transform.Condition.Composite.OperatorStrategy;
using FluentAssertions;

namespace ETLProjectTest.Transform;

public class ConvertStringToObjectTest
{
    [Theory]
    [MemberData(nameof(TablesThatReturnCorrectTable))]
    public void GetDataTable_ShouldReturnTable_WhenTableNameIsInTheTableList(DataTable expected,
        List<DataTable> dataTables, string tableName)
    {
        // Arrange

        // Act
        var actual = ConvertStringToObject.GetDataTable(dataTables, tableName);
        // Assert
        actual.Should().Be(expected);
    }

    public static IEnumerable<object[]> TablesThatReturnCorrectTable()
    {
        var firstTable = new DataTable("Customer");
        var firstList = new List<DataTable>()
        {
            firstTable,
            new("Bank"),
            new("Employee")
        };
        yield return [firstTable, firstList, firstTable.TableName];
        var secondTable = new DataTable();
        var secondList = new List<DataTable>()
        {
            secondTable,
            new("Score"),
            new("Teacher")
        };
        yield return [secondTable, secondList, secondTable.TableName];
    }

    [Theory]
    [MemberData(nameof(TablesThatThrowArgumentException))]
    public void GetDataTable_ShouldThrowArgumentException_WhenTableNameNotExistInTheTableList(
        List<DataTable> dataTables, string tableName)
    {
        // Arrange

        // Act
        var action = () => ConvertStringToObject.GetDataTable(dataTables, tableName);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            EnsureCheck.NotExistTableError(tableName));
    }

    public static IEnumerable<object[]> TablesThatThrowArgumentException()
    {
        const string firstTableName = "Customer";
        var firstList = new List<DataTable>()
        {
            new("Bank"),
            new("Employee")
        };
        yield return [firstList, firstTableName];

        const string secondTableName = "Student";
        var secondList = new List<DataTable>();
        yield return [secondList, secondTableName];

        const string thirdTableName = "Bank";
        var thirdList = new List<DataTable>()
        {
            new()
        };
        yield return [thirdList, thirdTableName];
    }

    [Theory]
    [MemberData(nameof(ColumnsThatReturnCorrectColumns))]
    public void GetGroupedBysColumn_ShouldReturnColumns_WhenColumnNamesIsInTheTable(List<DataColumn> expected
        , DataTable table, List<string> groupedBysColumnNames)
    {
        // Arrange

        // Act
        var actual = ConvertStringToObject.GetGroupedBysColumn(table, groupedBysColumnNames);
        // Assert
        actual.Should().Equal(expected);
    }

    public static IEnumerable<object[]> ColumnsThatReturnCorrectColumns()
    {
        var firstColumns = new List<string>()
        {
            "ID",
            "Address",
            "Number"
        };
        var firstTable = new DataTable();
        firstColumns.ForEach(column => firstTable.Columns.Add(column));
        var firstGroupedColumns = new List<string>()
        {
            "Address",
            "Number"
        };
        var firstExpected = new List<DataColumn>()
        {
            firstTable.Columns["Address"]!,
            firstTable.Columns["Number"]!,
        };

        yield return [firstExpected, firstTable, firstGroupedColumns];

        var secondTable = new DataTable();
        var secondGroupedColumns = new List<string>();
        var secondExpected = new List<DataColumn>();
        yield return [secondExpected, secondTable, secondGroupedColumns];
    }

    [Theory]
    [MemberData(nameof(ColumnNamesThatThrowArgumentException))]
    public void GetGroupedBysColumn_ShouldThrowArgumentException_WhenColumnNameNotExistInTheTable(DataTable table
        , List<string> groupedBysColumnNames, List<string> notExistColumnNames)
    {
        // Arrange
        var errorColumn = notExistColumnNames[0];
        // Act
        var action = () => ConvertStringToObject.GetGroupedBysColumn(table, groupedBysColumnNames);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            EnsureCheck.NotExistColumnError(errorColumn));
    }

    public static IEnumerable<object[]> ColumnNamesThatThrowArgumentException()
    {
        var firstTable = new DataTable();
        firstTable.Columns.Add("ID");
        firstTable.Columns.Add("Grade");
        firstTable.Columns.Add("Email");
        var firstColumns = new List<string>()
        {
            "ID",
            "Address",
            "Number"
        };
        var firstNotExistColumns = new List<string>()
        {
            "Address",
            "Number"
        };
        yield return [firstTable, firstColumns, firstNotExistColumns];

        var secondTable = new DataTable();
        var secondColumns = new List<string>()
        {
            "ID"
        };
        var secondNotExistColumns = new List<string>()
        {
            "ID"
        };
        yield return [secondTable, secondColumns, secondNotExistColumns];
    }

    [Theory]
    [MemberData(nameof(ColumnNameThatReturnCorrectColumn))]
    public void GetAggregatedColumn_ShouldReturnColumn_WhenColumnNameIsInTheTable(DataColumn expected
        , DataTable table, string aggregatedColumnName)
    {
        // Arrange

        // Act
        var actual = ConvertStringToObject.GetAggregatedColumn(table, aggregatedColumnName);
        // Assert
        actual.Should().Be(expected);
    }

    public static IEnumerable<object[]> ColumnNameThatReturnCorrectColumn()
    {
        var firstColumns = new List<string>()
        {
            "ID",
            "Address",
            "Number"
        };
        var firstTable = new DataTable();
        firstColumns.ForEach(column => firstTable.Columns.Add(column));

        yield return [firstTable.Columns["Address"]!, firstTable, "Address"];
        yield return [firstTable.Columns["Number"]!, firstTable, "Number"];
    }

    [Theory]
    [MemberData(nameof(ColumnNameThatThrowArgumentException))]
    public void GetAggregatedColumn_ShouldThrowArgumentException_WhenColumnNameNotExistInTheTable(DataTable table
        , string aggregatedColumnName)
    {
        // Arrange
        
        // Act
        var action = () => ConvertStringToObject.GetAggregatedColumn(table, aggregatedColumnName);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            EnsureCheck.NotExistColumnError(aggregatedColumnName));
    }
    public static IEnumerable<object[]> ColumnNameThatThrowArgumentException()
    {
        var firstTable = new DataTable();
        firstTable.Columns.Add("ID");
        firstTable.Columns.Add("Grade");
        firstTable.Columns.Add("Email");
        
        yield return [firstTable,"Address"];
        yield return [firstTable,"Number"];

        var secondTable = new DataTable();
        
        yield return [secondTable,"ID"];
    }

    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectAggregateStrategy))]
    public void GetAggregateStrategy_ShouldReturnCorrectAggregateStrategyType_WhenParameterSpecifyType(
        IAggregateStrategy expected ,string type)
    {
        // Arrange
        
        // Act
        var actual = ConvertStringToObject.GetAggregateStrategy(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectAggregateStrategy()
    {
        yield return [new SumStrategy(), "Sum"];
        yield return [new AverageStrategy(), "Avg"];
        yield return [new MinStrategy(), "Min"];
        yield return [new MaxStrategy(), "Max"];
        yield return [new CountStrategy(), "Count"];
    }

    [Fact]
    public void GetAggregateStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const string type = "Average";
        // Act
        var action = () => ConvertStringToObject.GetAggregateStrategy(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            AggregateStrategyFactory.NotExistTypeError(type));
    }
 
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectComparisonStrategy))]
    public void GetComparisonStrategy_ShouldReturnCorrectComparisonStrategyType_WhenParameterSpecifyType(
        IComparisonStrategy expected ,string condition)
    {
        // Arrange
        
        // Act
        var actual = ConvertStringToObject.GetComparisonStrategy(condition);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }
    
    public static IEnumerable<object[]> TypeThatReturnCorrectComparisonStrategy()
    {
        yield return [new EqualStrategy(), "Email = danial@gmail.com"];
        yield return [new LessThanStrategy(), "Grade < 15.8"];
        yield return [new MoreThanStrategy(), "Age > 14"];
    }
    
    [Theory]
    [InlineData('!' , "Age ! 12")]
    [InlineData('?' , "Email ? danial@gmail.com")]
    public void GetComparisonStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType(char type
        , string condition)
    {
        // Arrange
        
        // Act
        var action = () => ConvertStringToObject.GetComparisonStrategy(condition);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ComparisonStrategyFactory.NotExistTypeError(type));
    }
    
    [Theory]
    [MemberData(nameof(TypeThatReturnCorrectOperatorStrategy))]
    public void GetOperatorStrategy_ShouldReturnCorrectOperatorStrategyType_WhenParameterSpecifyType(
        IOperatorStrategy expected ,string type)
    {
        // Arrange
        
        // Act
        var actual = ConvertStringToObject.GetOperatorStrategy(type);
        // Assert
        actual.Should().BeOfType(expected.GetType());
    }

    public static IEnumerable<object[]> TypeThatReturnCorrectOperatorStrategy()
    {
        yield return [new AndStrategy(), "And"];
        yield return [new OrStrategy(), "Or"];
    }

    [Fact]
    public void GetOperatorStrategy_ShouldThrowArgumentException_WhenParameterNotSpecifyType()
    {
        // Arrange
        const string type = "Xor";
        // Act
        var action = () => ConvertStringToObject.GetOperatorStrategy(type);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            OperatorStrategyFactory.NotExistTypeError(type));
    }
}