using System;
using System.Collections.Generic;
using Rebus.Messages;
using Rebus.Pipeline.Send;

namespace Rebus.Idempotency
{
    public class MessageData
    {
        public string MessageId { get; set; }
        public IdempotencyData IdempotencyData;
        public string InputQueueAddress { get; set; }
        public int? ProcessingThreadId { get; set; }
        public DateTime TimeThreadIdAssigned { get; set; }

        public MessageData()
        {
            IdempotencyData = new IdempotencyData();
        }

        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                int hash = (int)2166136261;
                // Suitable nullity checks etc, of course :)
                hash = (hash * 16777619) ^ MessageId.GetHashCode();
                hash = (hash * 16777619) ^ IdempotencyData.GetHashCode();
                hash = (hash * 16777619) ^ InputQueueAddress.GetHashCode();
                hash = (hash * 16777619) ^ ProcessingThreadId.GetValueOrDefault().GetHashCode();
                hash = (hash * 16777619) ^ TimeThreadIdAssigned.GetHashCode();
                return hash;
            }
        }

        public bool HasAlreadyHandled(string messageId)
        {
            return IdempotencyData.HasAlreadyHandled(messageId);
        }

        public IEnumerable<OutgoingMessage> GetOutgoingMessages()
        {
            return IdempotencyData.GetOutgoingMessages(MessageId);
        }

        public void MarkMessageAsHandled()
        {
            IdempotencyData.MarkMessageAsHandled(MessageId);
        }

        public void AddOutgoingMessage(string messageId, DestinationAddresses destinationAddresses, TransportMessage transportMessage)
        {
            IdempotencyData.AddOutgoingMessage(messageId, destinationAddresses, transportMessage);
        }
    }
}
