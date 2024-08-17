using System.Data;

namespace ETLProject.Transform.Condition.Composite.OperatorStrategy;

public interface IOperatorStrategy
{
    public List<DataRow> Operate(List<List<DataRow>> dataRows);
}