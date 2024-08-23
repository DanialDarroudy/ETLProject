using System.Data;

namespace ETLProject.Transform.Condition.OperatorStrategy;

public class AndStrategy : IOperatorStrategy
{
    public List<DataRow> Operate(List<List<DataRow>> dataRows)
    {
        var result = dataRows[0];
        for (var i = 1; i < dataRows.Count; i++)
        {
            result = result.Intersect(dataRows[i]).ToList();
        }
        return result;
    }
}