using System.Data;
using ETLProject.Extract;
using ETLProject.Transform.Aggregation;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.Composite;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Npgsql;

namespace ETLProject.Control;

public class ConditionController : Controller
{
    [HttpPost]
    public IActionResult Condition([FromBody] List<Tuple<string, string>> sources, [FromBody] IComponentCondition root)
    {
        var converters = sources.Select(tuple =>
            DataConverterFactory.CreateConverter(tuple.Item1)).ToList();


        for (var i = 0; i < converters.Count; i++)
        {
            var tables = converters[i].ConvertToDataTables(sources[i].Item2);
            foreach (var table in tables)
            {
                DataBase.DataTables.Add(table);
            }
        }

        var resultTable = new Condition(root);
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}