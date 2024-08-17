using System.Data;

namespace ETLProject.Transform.Aggregation;

public class AverageStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Average(row => Convert.ToDecimal(row[aggregated]));
    }
}