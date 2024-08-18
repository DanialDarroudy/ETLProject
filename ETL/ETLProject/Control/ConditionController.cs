using System.Data;
using ETLProject.Transform.Condition.Composite;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Npgsql;

namespace ETLProject.Control;

public class ConditionController : Controller
{
    [HttpPost]
    public IActionResult Condition([FromBody] IComponentCondition root)
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
}
