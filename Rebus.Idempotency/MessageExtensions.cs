using Rebus.Bus;
using Rebus.Messages;

namespace Rebus.Idempotency
{
    public static class MessageExtensions
    {
        public static MessageId GetMessageIdWithDeferCount(this Message message)
        {
            var deferCount = message.Headers.TryGetValue(Headers.DeferCount, out var result)
                ? int.Parse(result)
                : 0;

            // if the message got defered, it needs a new ID in terms of idempotency.
            return new MessageId(message.GetMessageId(), deferCount);
        }

        public static MessageId GetMessageIdWithDeferCount(this TransportMessage message)
        {
            var deferCount = message.Headers.TryGetValue(Headers.DeferCount, out var result)
                ? int.Parse(result)
                : 0;

            // if the message got defered, it needs a new ID in terms of idempotency.
            return new MessageId(message.GetMessageId(), deferCount);
        }
    }
}