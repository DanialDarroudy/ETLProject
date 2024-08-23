using ETLProject.Deserialization;
using ETLProject.Transform.Condition.MainCondition;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Controllers;

[Route("[controller]/[action]")]
public class ConditionController : Controller
{
    [HttpPost]
    public IActionResult ApplyCondition([FromBody] ConditionDTO dto)
    {
        var manager = new DataConversionManager();
        var converters = manager.CreateConvertersFromSources(dto.Sources);

        var allTables = manager.AddConvertedTablesToList(converters, dto.Sources);
        
        var resultTable = new Condition(dto.Root).ApplyCondition(
            ConvertStringToObject.GetDataTable(allTables , dto.TableName));
        
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}