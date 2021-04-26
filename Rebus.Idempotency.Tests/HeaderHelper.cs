using System;
using System.Collections.Generic;
using Rebus.Messages;

namespace Rebus.Idempotency.Tests
{
    internal static class HeaderHelper
    {
        public static Dictionary<string, string> ConstructHeadersWithMessageId()
        {
            var headers = new Dictionary<string, string>
            {
                { Headers.MessageId, Guid.NewGuid().ToString() }
            };
            return headers;
        }
    }
}
