using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.Idempotency
{
    public static class MessageDataFactory
    {
        public static MessageData BuildMessageData(string messageId, string inputQueueAddress, int? processingThreadId,
            DateTime? timeThreadIdAssigned)
        {
            var msgData = new MessageData()
            {
                MessageId = messageId,
                InputQueueAddress = inputQueueAddress,
                ProcessingThreadId = processingThreadId,
                TimeThreadIdAssigned = timeThreadIdAssigned
            };
            return msgData;
        }

    }
}
