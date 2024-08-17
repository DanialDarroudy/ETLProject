using System.Data;

namespace ETLProject.Transform.Aggregation;

public class MinStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Min(row => Convert.ToDecimal(row[aggregated]));
    }
}