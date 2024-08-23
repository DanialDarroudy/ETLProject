using ETLProject.Deserialization;
using ETLProject.Transform.Aggregation.MainAggregation;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Controllers;


[Route("[controller]/[action]")]
public class AggregationController : Controller
{
    [HttpPost]
    public IActionResult Aggregate([FromBody] AggregationDto dto)
    {
        var manager = new DataConversionManager();
        var converters = manager.CreateConvertersFromSources(dto.Sources);

        var allTables = manager.AddConvertedTablesToList(converters, dto.Sources);


        var resultTable = new AggregationParametersInitializer().InitializeAggregation(allTables , dto).Aggregate();
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}