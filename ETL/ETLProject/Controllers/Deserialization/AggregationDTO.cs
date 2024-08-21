namespace ETLProject.Controllers.Deserialization;

public class AggregationDto
{
    public string TableName { get; set; } = null!;
    public List<string> GroupedByColumnNames { get; set; } = null!;
    public string AggregatedColumnName { get; set; } = null!;
    public string StrategyType { get; set; } = null!;
    public List<string> Sources { get; set; } = null!;
}