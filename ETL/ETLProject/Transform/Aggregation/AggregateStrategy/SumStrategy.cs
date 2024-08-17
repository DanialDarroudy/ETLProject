using System.Data;

namespace ETLProject.Transform.Aggregation.AggregateStrategy;

public class SumStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Sum(row => Convert.ToDecimal(row[aggregated]));
    }
}