using System.Data;

namespace ETLProject.Transform.Aggregation.AggregateStrategy;

public class AverageStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Average(row => Convert.ToDecimal(row[aggregated]));
    }
}