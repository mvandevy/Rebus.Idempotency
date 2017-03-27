using System;

namespace Rebus.Idempotency.MySql.Extensions
{
    internal static class DbDataReader
    {
        public static object ExtractValue(this global::System.Data.Common.DbDataReader reader, string columnName)
        {
            var o = reader[columnName];
            if (o is DBNull) return null;
            return o;
        }
    }
}
