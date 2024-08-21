using ETLProject.Transform.Condition.Composite;
using Newtonsoft.Json;

namespace ETLProject.Controllers.Deserialization;

public class ConditionDTO
{
    public List<string> Sources { get; set; } = null!;
    [JsonConverter(typeof(ComponentConditionConverter))]
    public IComponentCondition Root { get; set; } = null!;
    public string TableName { get; set; } = null!;
}