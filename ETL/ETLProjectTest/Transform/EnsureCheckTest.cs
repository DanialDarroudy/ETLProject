using System.Data;
using ETLProject.Transform;
using FluentAssertions;

namespace ETLProjectTest.Transform;

public class EnsureCheckTest
{
    [Fact]
    public void NotExistTableError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string tableName = "Student";
        const string expected = $"Table {tableName} not found";
        // Act
        var actual = EnsureCheck.NotExistTableError(tableName);
        // Assert
        actual.Should().Be(expected);
    }
    [Fact]
    public void NotExistColumnError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string columnName = "ID";
        const string expected = $"Column '{columnName}' does not exist in the DataTable.";
        // Act
        var actual = EnsureCheck.NotExistColumnError(columnName);
        // Assert
        actual.Should().Be(expected);
    }
    [Fact]
    public void CheckNull_ShouldThrowArgumentException_WhenParameterIsNull()
    {
        // Arrange
        DataTable table = null!;
        // Act
        var action = () => EnsureCheck.CheckNull(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(EnsureCheck.NullTableError);
    }
    [Fact]
    public void CheckNull_ShouldNotThrowArgumentException_WhenParameterInitialized()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => EnsureCheck.CheckNull(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    [Fact]
    public void CheckEmpty_ShouldThrowArgumentException_WhenParameterDoNotHaveAnyRows()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => EnsureCheck.CheckEmpty(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(EnsureCheck.EmptyTableError);
    }
    [Fact]
    public void CheckEmpty_ShouldNotThrowArgumentException_WhenParameterHasRows()
    {
        // Arrange
        var table = new DataTable();
        table.Rows.Add(table.NewRow());
        // Act
        var action = () => EnsureCheck.CheckEmpty(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    [Theory]
    [MemberData(nameof(TablesThatThrowArgumentException))]
    public void CheckHasTableName_ShouldThrowArgumentException_WhenTableNameNotExistInTheTablesList(
        List<DataTable> dataTables , string tableName)
    {
        // Arrange
        
        // Act
        var action = () => EnsureCheck.CheckHasTableName(dataTables , tableName);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            EnsureCheck.NotExistTableError(tableName));
    }

    public static IEnumerable<object[]> TablesThatThrowArgumentException()
    {
        var firstTables = new List<DataTable>();
        yield return [firstTables, "Teacher"];
        
        var secondTables = new List<DataTable>()
        {
            new("Bank"),
            new("Customer"),
            new("Employee")
        };
        yield return [secondTables, "Client"];
        yield return [secondTables, "Pilot"];
        yield return [secondTables, "AirPort"];
    }
    [Theory]
    [MemberData(nameof(TablesThatNotThrowArgumentException))]
    public void CheckHasTableName_ShouldNotThrowArgumentException_WhenTableNameExistInTheTablesList(
        List<DataTable> dataTables , string tableName)
    {
        // Arrange
        
        // Act
        var action = () => EnsureCheck.CheckHasTableName(dataTables , tableName);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }
    public static IEnumerable<object[]> TablesThatNotThrowArgumentException()
    {
        var firstTables = new List<DataTable>()
        {
            new(),
        };
        yield return [firstTables, ""];
        
        var secondTables = new List<DataTable>()
        {
            new("Bank"),
            new("Customer"),
            new("Employee")
        };
        yield return [secondTables, "Customer"];
        yield return [secondTables, "Employee"];
    }

    [Theory]
    [MemberData(nameof(ColumnsThatThrowArgumentException))]
    public void CheckHasColumnNames_ShouldThrowArgumentException_WhenExistColumnThatNotExistInTheTable(DataTable table
        , List<string> columnNames , List<string> notExistColumnNames)
    {
        // Arrange
        var errorColumn = notExistColumnNames[0];
        // Act
        var action = () => EnsureCheck.CheckHasColumnNames(table , columnNames);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            EnsureCheck.NotExistColumnError(errorColumn));
    }

    public static IEnumerable<object[]> ColumnsThatThrowArgumentException()
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
    [MemberData(nameof(ColumnsThatNotThrowArgumentException))]
    public void CheckHasColumnNames_ShouldNotThrowArgumentException_WhenAllColumnsExistInTheTable(DataTable table
        , List<string> columnNames)
    {
        // Arrange
        
        // Act
        var action = () => EnsureCheck.CheckHasColumnNames(table , columnNames);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }
    public static IEnumerable<object[]> ColumnsThatNotThrowArgumentException()
    {
        var firstTable = new DataTable();
        var firstColumns = new List<string>()
        {
            "ID",
            "Address",
            "Number"
        };
        firstColumns.ForEach(column => firstTable.Columns.Add(column));
        
        yield return [firstTable, firstColumns];
        
        var secondTable = new DataTable();
        var secondColumns = new List<string>();
        
        yield return [secondTable, secondColumns];
    }
}