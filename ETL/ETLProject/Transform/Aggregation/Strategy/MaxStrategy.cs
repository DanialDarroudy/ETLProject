using System.Data;

namespace ETLProject.Transform.Aggregation.Strategy;

public class MaxStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Max(row => Convert.ToDecimal(row[aggregated]));
    }
}