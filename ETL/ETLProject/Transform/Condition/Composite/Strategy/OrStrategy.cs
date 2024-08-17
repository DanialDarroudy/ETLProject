using System.Data;

namespace ETLProject.Transform.Condition.Composite.Strategy;

public class OrStrategy : IOperatorStrategy
{
    public List<DataRow> Operate(List<List<DataRow>> dataRows)
    {
        var result = dataRows[0];
        for (var i = 1; i < dataRows.Count; i++)
        {
            result = result.Union(dataRows[i]).ToList();
        }

        return result;
    }
}