namespace ETLProject.Transform.Aggregation;

public class AggregationDTO
{
    public string TableName { get; }
    public List<string> GroupedBysColumnNames { get; }
    public string AggregatedColumnName { get; }
    public string StrategyType { get; }
}