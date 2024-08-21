using System.Data;
using ETLProject.Controllers.Deserialization;
using Newtonsoft.Json;

namespace ETLProject.Transform.Condition.Composite;

[JsonConverter(typeof(ComponentConditionConverter))]
public interface IComponentCondition
{
    public List<DataRow> PerformFilter(DataTable table);
}