using ETLProject.Extract;
using ETLProject.Transform.Aggregation;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Control;

public class AggregateController : Controller
{
    [HttpPost]
    public IActionResult Aggregation([FromBody] List<Tuple<string, string>> sources, [FromBody] AggregationDTO dto)
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

        var aggregatedTable = DataBase.GetDataTable(dto.TableName);
        var resultTable = new Aggregation(dto.TableName, dto.GetGroupedBysColumn(aggregatedTable)
            , dto.GetAggregatedColumn(aggregatedTable), dto.GetStrategy()).Aggregate();
        resultTable.TableName = dto.TableName;
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}