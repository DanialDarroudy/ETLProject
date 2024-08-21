using System.Data;
using ETLProject.Controllers;
using ETLProject.Controllers.Deserialization;
using ETLProject.Transform.Aggregation.AggregateStrategy;

namespace ETLProject.Transform.Aggregation;

public class Aggregation(AggregationDto dto)
{
    private DataTable _table = null!;
    private List<DataColumn> _groupedBys = null!;
    private DataColumn _aggregated = null!;
    private IAggregateStrategy _strategy = null!;

    public DataTable Aggregate(DataTable table)
    {
        InitializeAggregationParameters(table);
        InitialCheck.CheckNull(_table);
        InitialCheck.CheckEmpty(_table);
        
        return GenerateAggregatedTable();
    }
    
    private DataTable GenerateAggregatedTable()
    {
        var resultTable = TableInitializer.InitializeTable(_groupedBys , _aggregated);
        var groupedRows = GroupByColumns();
        foreach (var (groupKey, rowsInGroup) in groupedRows)
        {
            var newRow = resultTable.NewRow();
            PopulateRowWithGroupValues(newRow, groupKey);
            newRow[_groupedBys.Count] = _strategy.DoSpecificAggregate(rowsInGroup , _aggregated);
            resultTable.Rows.Add(newRow);
        }
        resultTable.TableName = dto.TableName;
        return resultTable;
    }
    
    private Dictionary<string, List<DataRow>> GroupByColumns()
    {
        var groupedRowsDict = new Dictionary<string, List<DataRow>>();
        foreach (DataRow row in _table.Rows)
        {
            var groupKey = string.Join(",", _groupedBys.Select(column => row[column].ToString()));
            if (!groupedRowsDict.ContainsKey(groupKey))
            {
                groupedRowsDict[groupKey] = [];
            }
            groupedRowsDict[groupKey].Add(row);
        }
        return groupedRowsDict;
    }

    private void PopulateRowWithGroupValues(DataRow newRow, string groupKey)
    {
        var groupValues = groupKey.Split(',');

        for (var i = 0; i < _groupedBys.Count; i++)
        {
            newRow[i] = groupValues[i];
        }
    }
    
    private void InitializeAggregationParameters(DataTable table)
    {
        _table = table;
        _groupedBys = ConvertStringToObject.GetGroupedBysColumn(_table, dto.GroupedByColumnNames);
        _aggregated = ConvertStringToObject.GetAggregatedColumn(_table, dto.AggregatedColumnName);
        _strategy = ConvertStringToObject.GetAggregateStrategy(dto.StrategyType);
    }
}