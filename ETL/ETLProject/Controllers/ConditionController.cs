using ETLProject.Controllers.Deserialization;
using ETLProject.Transform;
using ETLProject.Transform.Condition;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Controllers;

[Route("[controller]/[action]")]
public class ConditionController : Controller
{
    [HttpPost]
    public IActionResult ApplyCondition([FromBody] ConditionDTO dto)
    {
        var converters = DataConversionManager.CreateConvertersFromSources(dto.Sources);
        
        var allTables = DataConversionManager.AddConvertedTablesToList(converters , dto.Sources);
        
        var resultTable = new Condition(dto.Root).ApplyCondition(
            ConvertStringToObject.GetDataTable(allTables , dto.TableName));
        
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}