using System.Data;
using ETLProject.Deserialization;

namespace ETLProject.Transform.Aggregation.MainAggregation;

public class AggregationParametersInitializer
{
    public Aggregation InitializeAggregation(List<DataTable> allTables, AggregationDto dto)
    {
        var table = ConvertStringToObject.GetDataTable(allTables, dto.TableName);
        var groupedBys = ConvertStringToObject.GetGroupedBysColumn(table, dto.GroupedByColumnNames);
        var aggregated = ConvertStringToObject.GetAggregatedColumn(table, dto.AggregatedColumnName);
        var strategy = ConvertStringToObject.GetAggregateStrategy(dto.StrategyType);
        return new Aggregation(table, groupedBys, aggregated, strategy);
    }
}