﻿using System.Data;

namespace ETLProject.Transform.Aggregation.AggregateStrategy;

public class CountStrategy : IAggregateStrategy
{
    public decimal DoSpecificAggregate(List<DataRow> rowsInGroup , DataColumn aggregated)
    {
         return rowsInGroup.Count;
    }
}