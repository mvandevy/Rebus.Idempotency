using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.Idempotency.Tests
{
    internal class TransportCounter : ITransport
    {
        private readonly ITransport _innerTransport;
        private readonly ConcurrentDictionary<string, int> _transportMessagesSent;
        private readonly ConcurrentDictionary<string, int> _transportMessagesReceived;

        public TransportCounter(ITransport transport, ConcurrentDictionary<string, int> transportMessagesSent,
            ConcurrentDictionary<string, int> transportMessagesReceived)
        {
            _innerTransport = transport;
            _transportMessagesSent = transportMessagesSent;
            _transportMessagesReceived = transportMessagesReceived;
        }


        public void CreateQueue(string address)
        {
            _innerTransport.CreateQueue(address);
        }

        public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            var type = message.Headers[Headers.Type];
            if (_transportMessagesSent.TryGetValue(type, out int value))
            {
                _transportMessagesSent[type] = value + 1;
            }
            else
            {
                _transportMessagesSent.GetOrAdd(type, 1);
            }

            await _innerTransport.Send(destinationAddress, message, context);
        }

        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            var message = await _innerTransport.Receive(context, cancellationToken);
            if (message == null) return null;

            var type = message.Headers[Headers.Type];
            if (_transportMessagesReceived.TryGetValue(type, out int value))
            {
                _transportMessagesReceived[type] = value + 1;
            }
            else
            {
                _transportMessagesReceived.GetOrAdd(type, 1);
            }
            return message;
        }

        public string Address
        {
            get { return _innerTransport.Address; }
        }
    }
}
