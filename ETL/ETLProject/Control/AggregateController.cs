using System.Data;
using ETLProject.Transform.Aggregation;
using Microsoft.AspNetCore.Mvc;

namespace ETLProject.Control;

public class AggregateController : Controller
{
    [HttpPost]
    public IActionResult Aggregation([FromBody] AggregationDTO dto)
    {
        var aggregationContext = new AggregationContext();
        var connection = new NpgsqlConnection(aggregationDto.DatabaseConnection);
        var groupByColumnsString = string.Join(", ", aggregationDto.GroupByColumns.Select(col => col));

        SetAggregationStrategy(aggregationDto, aggregationContext);
        connection.Open();

        var command = aggregationContext.Execute(connection, aggregationDto.TableName, groupByColumnsString,
            aggregationDto.AggregationColumn);
        using var dataAdapter = new NpgsqlDataAdapter(command);
        var resultTable = new DataTable(aggregationDto.TableName);

        dataAdapter.Fill(resultTable);
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}