namespace ETLProject.Transform.Aggregation;

public class AggregationDto(string tableName, List<string> groupedByColumnNames
    , string aggregatedColumnName, string strategyType)
{
    public string TableName { get; } = tableName;
    public List<string> GroupedByColumnNames { get; } = groupedByColumnNames;
    public string AggregatedColumnName { get; } = aggregatedColumnName;
    public string StrategyType { get; } = strategyType;
}