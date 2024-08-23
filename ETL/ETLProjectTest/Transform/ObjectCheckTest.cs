using System.Data;
using System.Text.Json;
using ETLProject.Transform;
using FluentAssertions;

namespace ETLProjectTest.Transform;

public class ObjectCheckTest
{
    [Fact]
    public void NotExistTableError_ShouldReturnStringError_WhenCalled()
    {
        // Arrange
        const string tableName = "Student";
        const string expected = $"Table {tableName} not found";
        // Act
        var actual = ObjectCheck.NotExistTableError(tableName);
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
        var actual = ObjectCheck.NotExistColumnError(columnName);
        // Assert
        actual.Should().Be(expected);
    }

    [Theory]
    [InlineData(typeof(DataTable))]
    [InlineData(typeof(DataRow))]
    [InlineData(typeof(DataColumn))]
    [InlineData(typeof(string))]
    public void EmptyListError_ShouldReturnStringError_WhenParameterIsType(Type type)
    {
        // Arrange
        var expected = $"The input List Of {type} cannot be empty.";
        // Act
        var actual = ObjectCheck.EmptyListError(type);
        // Assert
        actual.Should().Be(expected);
    }

    [Theory]
    [InlineData("Leaf")]
    [InlineData("Composite")]
    [InlineData("Component")]
    public void NotExistProperty_ShouldReturnStringError_WhenParameterIsProperty(string propertyName)
    {
        // Arrange
        var expected = $"Unknown type or missing property: {propertyName}";
        // Act
        var actual = ObjectCheck.NotExistProperty(propertyName);
        // Assert
        actual.Should().Be(expected);
    }

    [Fact]
    public void CheckNull_ShouldThrowArgumentException_WhenParameterIsNull()
    {
        // Arrange
        DataTable table = null!;
        // Act
        var action = () => ObjectCheck.CheckNull(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(ObjectCheck.NullTableError);
    }

    [Fact]
    public void CheckNull_ShouldNotThrowArgumentException_WhenParameterInitialized()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => ObjectCheck.CheckNull(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    [Fact]
    public void CheckEmpty_ShouldThrowArgumentException_WhenParameterDoNotHaveAnyRows()
    {
        // Arrange
        var table = new DataTable();
        // Act
        var action = () => ObjectCheck.CheckEmpty(table);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(ObjectCheck.EmptyTableError);
    }

    [Fact]
    public void CheckEmpty_ShouldNotThrowArgumentException_WhenParameterHasRows()
    {
        // Arrange
        var table = new DataTable();
        table.Rows.Add(table.NewRow());
        // Act
        var action = () => ObjectCheck.CheckEmpty(table);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    [Theory]
    [MemberData(nameof(EmptyList))]
    public void CheckEmpty_ShouldThrowArgumentException_WhenParameterIsEmpty<T>(List<T> target)
    {
        // Arrange

        // Act
        var action = () => ObjectCheck.CheckEmpty(target);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.EmptyListError(typeof(T)));
    }

    public static IEnumerable<object[]> EmptyList()
    {
        yield return [new List<DataTable>()];
        yield return [new List<DataRow>()];
        yield return [new List<DataColumn>()];
        yield return [new List<string>()];
    }

    [Theory]
    [MemberData(nameof(CreateListOfElements))]
    public void CheckEmpty_ShouldNotThrowArgumentException_WhenParameterHasElements<T>(List<T> target)
    {
        // Arrange

        // Act
        var action = () => ObjectCheck.CheckEmpty(target);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }

    public static IEnumerable<object[]> CreateListOfElements()
    {
        var tables = new List<DataTable>()
        {
            new()
        };
        yield return [tables];
        var rows = new List<DataRow>()
        {
            new DataTable().NewRow()
        };
        yield return [rows];
        var columns = new List<DataColumn>()
        {
            new()
        };
        yield return [columns];
        var sources = new List<string>()
        {
            "Students",
            "Customers"
        };
        yield return [sources];
    }

    [Theory]
    [InlineData("TypeLeaf")]
    [InlineData("TypeComposite")]
    [InlineData("TypeComponent")]
    public void EnsurePropertyExist_ShouldThrowArgumentException_WhenParameterDoesNotHaveProperty(string propertyName)
    {
        // Arrange
        const string json = "{}";
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        // Act
        var action = () => ObjectCheck.EnsurePropertyExist(root, propertyName);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.NotExistProperty(propertyName));
    }

    [Fact]
    public void EnsurePropertyExist_ShouldNotThrowArgumentException_WhenParameterHaveProperty()
    {
        // Arrange
        const string propertyName = "Type";
        const string json = $"{{\"{propertyName}\": \"SomeValue\"}}";
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        // Act
        var action = () => ObjectCheck.EnsurePropertyExist(root, propertyName);
        // Assert
        action.Should().NotThrow<ArgumentException>();
    }


    [Theory]
    [MemberData(nameof(TablesThatThrowArgumentException))]
    public void CheckHasTableName_ShouldThrowArgumentException_WhenTableNameNotExistInTheTablesList(
        List<DataTable> dataTables, string tableName)
    {
        // Arrange

        // Act
        var action = () => ObjectCheck.CheckHasTableName(dataTables, tableName);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.NotExistTableError(tableName));
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
        List<DataTable> dataTables, string tableName)
    {
        // Arrange

        // Act
        var action = () => ObjectCheck.CheckHasTableName(dataTables, tableName);
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
        , List<string> columnNames, List<string> notExistColumnNames)
    {
        // Arrange
        var errorColumn = notExistColumnNames[0];
        // Act
        var action = () => ObjectCheck.CheckHasColumnNames(table, columnNames);
        // Assert
        action.Should().Throw<ArgumentException>().WithMessage(
            ObjectCheck.NotExistColumnError(errorColumn));
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
        var action = () => ObjectCheck.CheckHasColumnNames(table, columnNames);
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