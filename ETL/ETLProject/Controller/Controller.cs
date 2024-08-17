using System.Data;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Npgsql;

namespace DIA_Project.Controllers;

public class DatabaseController : Controller
{
    [HttpPost]
    public IActionResult Aggregation([FromBody] AggregationDTO aggregationDto)
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

    [HttpPost]
    public IActionResult Condition([FromBody] ConditionDTO conditionDto)
    {
        var connection = new NpgsqlConnection(conditionDto.DatabaseConnection);
        var condition = new Condition(conditionDto);
        using var dataAdapter = new NpgsqlDataAdapter(ConvertDatabaseToDataTable(connection, conditionDto.TableName));
        var resultTable = new DataTable(conditionDto.TableName);
        dataAdapter.Fill(resultTable);

        connection.Open();
        resultTable = condition.ApplyCondition(resultTable);
        return Ok(JsonConvert.SerializeObject(resultTable));
    }

    private static void SetAggregationStrategy(AggregationDTO aggregationDto, AggregationContext aggregationContext)
    {
        aggregationContext.AggregationStrategy = aggregationDto.AggregationType.ToLower() switch
        {
            "sum" => new SumAggregationStrategy(),
            "count" => new CountAggregationStrategy(),
            "average" => new AverageAggregationStrategy(),
            "max" => new MaxAggregationStrategy(),
            "min" => new MinAggregationStrategy(),
            _ => aggregationContext.AggregationStrategy
        };
    }

    private static NpgsqlCommand ConvertDatabaseToDataTable(NpgsqlConnection databaseConnection, string tableName)
    {
        return new NpgsqlCommand($"""
                                  Select *
                                  FROM {tableName}
                                  """, databaseConnection);
    }
}
