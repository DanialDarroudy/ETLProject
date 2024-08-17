using System.Data;

namespace ETLProject.Transform.Aggregation.Strategy;

public class SumStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
        return rowsInGroup.Sum(row => Convert.ToDecimal(row[aggregated]));
    }
}