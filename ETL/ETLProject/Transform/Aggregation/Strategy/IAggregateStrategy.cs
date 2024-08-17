using System.Data;

namespace ETLProject.Transform.Aggregation.Strategy;

public interface IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated);
}