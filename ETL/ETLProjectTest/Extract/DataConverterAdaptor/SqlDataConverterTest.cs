using System.Data;
using ETLProject.Extract.DataConverterAdaptor;
using FluentAssertions;

namespace ETLProjectTest.Extract.DataConverterAdaptor;

public class SqlDataConverterTest
{
    [Theory]
    [MemberData(nameof(ProvideSourceAndTables))]
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
        const string source = "localhost.postgres.!@#123qwe.DIA.sql";
        var table = new DataTable("Products");
        table.Columns.Add("Name", typeof(string));
        table.Columns.Add("Price", typeof(int));
        table.Columns.Add("Supplier", typeof(string));
        table.Rows.Add("iPhone11" , 800 , "Apple");
        table.Rows.Add("iPhone12" , 900 , "Apple");
        table.Rows.Add("A35" , 500 , "Samsung");
        table.Rows.Add("A25" , 400 , "Samsung");
        table.Rows.Add("POCOX6" , 300 , "Xiaomi");
        yield return [new List<DataTable>(){table}, source];
    }
}