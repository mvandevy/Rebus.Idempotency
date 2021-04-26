using System;

namespace Rebus.Idempotency
{
    public class MessageId
    {
        public Guid OriginalMessageId;
        public int DeferCount = 0;

        public MessageId(Guid messageId, int? deferCount = 0)
        {
            OriginalMessageId = messageId;
            DeferCount = deferCount ?? 0;
        }

        public static implicit operator string(MessageId value)
        {
            return GetMessageIdString(value);
        }

        public static implicit operator MessageId(string value)
        {
            var split = value.Split('#');
            return new MessageId(Guid.Parse(split[0]), split.Length > 1 ? (int?)int.Parse(split[1]) : null);
        }

        private static string GetMessageIdString(MessageId value)
        {
            return value.OriginalMessageId + (value.DeferCount > 0 ? $"#{value.DeferCount}" : "");
        }

        public override bool Equals(object obj)
        {
            if ((obj == null) || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                MessageId m = (MessageId)obj;
                return (OriginalMessageId == m.OriginalMessageId) && (DeferCount == m.DeferCount);
            }
        }

        public override int GetHashCode()
        {
            return (OriginalMessageId, DeferCount).GetHashCode();
        }

        public override string ToString()
        {
            return $"{OriginalMessageId}" + (DeferCount > 0 ? $"#{DeferCount}" : "");
        }
    }
}