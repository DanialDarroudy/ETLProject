using ETLProject.Controllers.Deserialization;
using ETLProject.Transform;
using ETLProject.Transform.Aggregation;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Controllers;


[Route("[controller]/[action]")]
public class AggregationController : Controller
{
    [HttpPost]
    public IActionResult Aggregate([FromBody] AggregationDto dto)
    {
        var converters = DataConversionManager.CreateConvertersFromSources(dto.Sources);

        var allTables = DataConversionManager.AddConvertedTablesToList(converters, dto.Sources);


        var resultTable = new Aggregation(dto).Aggregate(ConvertStringToObject.GetDataTable(allTables, dto.TableName));
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}