using System.Data;

namespace ETLProject.Transform.Aggregation;

public class SumStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Sum(row => Convert.ToDecimal(row[aggregated]));
    }
}