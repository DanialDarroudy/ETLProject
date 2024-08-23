using System.Data;
using FluentAssertions;

namespace ETLProjectTest;

public static class AssertTwoTable
{
    public static void AreEqualTwoTable(DataTable expected, DataTable actual)
    {
        actual.TableName.Should().Be(expected.TableName);
        actual.Columns.Count.Should().Be(expected.Columns.Count);
        for (var i = 0; i < actual.Columns.Count; i++)
        {
            actual.Columns[i].ColumnName.Should().Be(expected.Columns[i].ColumnName);
            actual.Columns[i].DataType.Should().Be(expected.Columns[i].DataType);
        }
        
        
        actual.Rows.Count.Should().Be(expected.Rows.Count);
        for (var i = 0; i < actual.Rows.Count; i++)
        {
            var expectedRow = expected.Rows[i];
            var actualRow = actual.Rows[i];

            for (var j = 0; j < expected.Columns.Count; j++)
            {
                var expectedValue = expectedRow[j];
                var actualValue = actualRow[j];
                
                actualValue.Should().Be(expectedValue);
            }
        }
    }
}