using System.Data;

namespace ETLProject.Transform.Aggregation.Strategy;

public class AverageStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Average(row => Convert.ToDecimal(row[aggregated]));
    }
}