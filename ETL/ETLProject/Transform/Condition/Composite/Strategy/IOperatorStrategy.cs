using System.Data;

namespace ETLProject.Transform.Condition.Composite.Strategy;

public interface IOperatorStrategy
{
    public List<DataRow> Operate(List<List<DataRow>> dataRows);
}