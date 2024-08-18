using ETLProject.Transform;
using ETLProject.Transform.Condition;
using ETLProject.Transform.Condition.Composite;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace ETLProject.Controllers;

public class ConditionController : Controller
{
    [HttpPost]
    public IActionResult Condition([FromBody] List<Tuple<string, string>> sources, [FromBody] IComponentCondition root ,
        string tableName)
    {
        var converters = DataConversionManager.CreateConvertersFromSources(sources);
        
        var allTables = DataConversionManager.AddConvertedTablesToList(converters , sources);
        
        var resultTable = new Condition(root).ApplyCondition(ConvertStringToObject.GetDataTable(allTables , tableName));
        
        return Ok(JsonConvert.SerializeObject(resultTable));
    }
}