namespace ETLProject.Transform.Condition;

public static class SplitCondition
{
    public static string GetColumnName(string condition)
    {
        var result = "";
        foreach (var character in condition)
        {
            if (character.Equals(' '))
            {
                break;
            }
            result += character;
        }
        return result;
    }
    public static string GetValue(string condition)
    {
        var result = "";
        var isReached = 0;
        foreach (var character in condition)
        {
            if (isReached == 2 && !character.Equals('\''))
            {
                result += character;
            }
            if (character.Equals(' '))
            {
                isReached += 1;
            }
        }

        return result;
    }
}