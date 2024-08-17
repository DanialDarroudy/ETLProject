using System.Data;

namespace ETLProject.Transform.Aggregation.AggregateStrategy;

public class MinStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Min(row => Convert.ToDecimal(row[aggregated]));
    }
}