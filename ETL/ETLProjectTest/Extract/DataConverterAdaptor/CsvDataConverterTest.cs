using System.Data;
using ETLProject.Extract.DataConverterAdaptor;

namespace ETLProjectTest.Extract.DataConverterAdaptor;

public class CsvDataConverterTest
{
    [Theory]
    [MemberData(nameof(ProvidePathAndTable))]
    public void ConvertToDataTables_ShouldReturnTableInList_WhenParameterIsPathOfCsvFile(
        DataTable expected, string path)
    {
        // Arrange
        var converter = new CsvDataConverter();
        // Act
        var actual = converter.ConvertToDataTables(path);
        // Assert
        AssertTwoTable.AreEqualTwoTable(expected, actual[0]);
    }

    public static IEnumerable<object[]> ProvidePathAndTable()
    {
        const string firstPath = "Students.csv";
        var firstTable = new DataTable("Students");
        firstTable.Columns.Add("StudentID", typeof(string));
        firstTable.Columns.Add("FirstName", typeof(string));
        firstTable.Columns.Add("Age", typeof(string));
        firstTable.Columns.Add("Average", typeof(string));
        firstTable.Rows.Add(1001, "John", 15, 4);
        firstTable.Rows.Add(1002, "Jane", 16, 6);
        firstTable.Rows.Add(1003, "Alice", 17, 8);
        firstTable.Rows.Add(1004, "Bob", 16, 10);
        firstTable.Rows.Add(1005, "Charlie", 15, 20);
        yield return [firstTable, firstPath];
        
        const string secondPath = "Customers.csv";
        var secondTable = new DataTable("Customers");
        secondTable.Columns.Add("Balance", typeof(string));
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
        yield return [secondTable, secondPath];
    }
}