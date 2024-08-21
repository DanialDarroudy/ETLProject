using System.Text.Json;
using System.Text.Json.Serialization;
using ETLProject.Transform;
using ETLProject.Transform.Condition.Composite;

namespace ETLProject.Controllers.Deserialization;

using System;

public class ComponentConditionConverter : JsonConverter<IComponentCondition>
{
    public override IComponentCondition Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        var root = JsonDocument.ParseValue(ref reader).RootElement;
        var type = EnsureCheck.EnsurePropertyExist(root, "Type").GetString()!;
        switch (type)
        {
            case "CompositeCondition":
            {
                var strategyType = root.GetProperty("StrategyType").GetString()!;
                var children = DeserializeChildren(root, options);
                return new CompositeCondition(children, strategyType);
            }
            case "LeafCondition":
            {
                var condition = root.GetProperty("Condition").GetString()!;
                return new LeafCondition(condition);
            }
            default:
                throw new ArgumentException(EnsureCheck.NotExistProperty(type));
        }
    }

    private List<IComponentCondition> DeserializeChildren(JsonElement root, JsonSerializerOptions options)
    {
        var children = new List<IComponentCondition>();
        if (root.TryGetProperty("Children", out var childrenElement)
            && childrenElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var childElement in childrenElement.EnumerateArray())
            {
                children.Add(JsonSerializer.Deserialize<IComponentCondition>(childElement.GetRawText(),
                    options)!);
            }
        }
        return children;
    }


    public override void Write(Utf8JsonWriter writer, IComponentCondition value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}