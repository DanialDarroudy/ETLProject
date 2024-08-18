namespace ETLProject.Transform.Aggregation;

public class AggregationDto(string tableName, List<string> groupedBysColumnNames
    , string aggregatedColumnName, string strategyType)
{
    public string TableName { get; } = tableName;
    public List<string> GroupedBysColumnNames { get; } = groupedBysColumnNames;
    public string AggregatedColumnName { get; } = aggregatedColumnName;
    public string StrategyType { get; } = strategyType;
}