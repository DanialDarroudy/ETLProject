using System.Data;

namespace ETLProject.Transform.Aggregation;

public class MaxStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Max(row => Convert.ToDecimal(row[aggregated]));
    }
}