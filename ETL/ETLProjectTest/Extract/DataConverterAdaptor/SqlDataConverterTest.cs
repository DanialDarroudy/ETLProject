using System.Data;
using ETLProject.Extract.DataConverterAdaptor;
using FluentAssertions;

namespace ETLProjectTest.Extract.DataConverterAdaptor;

public class SqlDataConverterTest
{
    // TODO For Morning
    public void ConvertToDataTables_ShouldReturnTables_WhenParameterIsAddressOfPostgreSqlDataBase(
        List<DataTable> expected, string source)
    {
        // Arrange
        var converter = new SqlDataConverter();
        // Act
        var actual = converter.ConvertToDataTables(source);
        // Assert
        actual.Count.Should().Be(expected.Count);
        for (var i = 0; i < actual.Count; i++)
        {
            AssertTwoTable.AreEqualTwoTable(expected[i], actual[i]);
        }
    }


    public static IEnumerable<object[]> ProvideSourceAndTables()
    {
        // TODO For Morning
        const string source = "";
        var table = new DataTable("Products");
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Price", typeof(string));
        table.Columns.Add("Supplier", typeof(string));
        table.Rows.Add();
        table.Rows.Add();
        table.Rows.Add();
        table.Rows.Add();
        table.Rows.Add();
        yield return [new List<DataTable>(){table}, source];
    }
}