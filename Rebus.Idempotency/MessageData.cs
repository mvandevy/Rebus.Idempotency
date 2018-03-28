using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Rebus.Messages;
using Rebus.Pipeline.Send;

namespace Rebus.Idempotency
{
    public class MessageData
    {
        public string MessageId { get; set; }
        public string InputQueueAddress { get; set; }
        public int? ProcessingThreadId { get; set; }
        public DateTime? TimeThreadIdAssigned { get; set; }
        public IdempotencyData IdempotencyData { get; set; }

        public bool IsDuplicate { get; private set; }

        public MessageData()
        {
            IdempotencyData = new IdempotencyData();
        }

        public bool HasAlreadyHandled(string messageId)
        {
            var isAlreadyHandled = IdempotencyData.HasAlreadyHandled(messageId);           
            return isAlreadyHandled;
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

        public void MarkMessageAsDuplicate()
        {
            this.IsDuplicate = true;
        }

        public bool CanBeSaved()
        {
            return !this.IsDuplicate;
        }
    }
}
