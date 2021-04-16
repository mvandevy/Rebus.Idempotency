using System;

namespace Rebus.Idempotency
{
    public static class MessageDataFactory
    {
        public static MessageData BuildMessageData(
            string messageId, 
            int? deferCount,
            string inputQueueAddress, 
            int? processingThreadId,
            DateTime? timeThreadIdAssigned)
        {
            var msgData = new MessageData()
            {
                MessageId = new MessageId(messageId, deferCount),
                InputQueueAddress = inputQueueAddress,
                ProcessingThreadId = processingThreadId,
                TimeThreadIdAssigned = timeThreadIdAssigned
            };
            return msgData;
        }

    }
}
