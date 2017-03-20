﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Idempotency;
using Rebus.Idempotency.Persistence;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Transport;
using Rebus.Transport.InMem;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            BuiltinHandlerActivator _activator;
            IBus _bus;
            ConcurrentDictionary<string, MessageData> _persistentMessageData;

            _activator = new BuiltinHandlerActivator();

            _bus = Configure.With(_activator)
                .Logging(l => l.ColoredConsole())
                .Transport(t =>
                {
                    t.UseInMemoryTransport(new InMemNetwork(), "bimse");
                })
                .Options(o =>
                {
                    o.EnableIdempotentMessages(new InMemoryMessageStorage());
                    o.LogPipeline(true);
                })
                .Start();

            var handlersTriggered = new ConcurrentQueue<DateTime>();
            var receivedMessages = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyIdempotentHandler(b, handlersTriggered));
            _activator.Register(() => new OutgoingMessageCollector(receivedMessages));

            int total = 10;
            var msg = new MyMessage
                {
                    Id = 1,
                    Total = total,
                    SendOutgoingMessage = true
                };

//            var messagesToSend = new []
//            {
//                msg,
//                msg,
//                msg,
//                msg,
//                msg,
//                msg,
//                msg,
//                msg,
//                msg,
//                msg
//            };

            var messagesToSend = new[]
            {
                msg,
                msg
            };

            var headers = ConstructHeadersWithMessageId();

            foreach (var message in messagesToSend)
            {
                _bus.SendLocal(message, headers);
                Task.Delay(100);
            }

            Console.WriteLine("All messages processed - waiting for messages in outgoing message collector...");

            //Task.Delay(1000);

            Console.WriteLine($"The handler was triggered {handlersTriggered.ToArray().Length} times.");
            Console.WriteLine($"The number of outgoing messages received was {receivedMessages.ToArray().Length}");

            Console.ReadLine();
        }

        private static Dictionary<string, string> ConstructHeadersWithMessageId()
        {
            var headers = new Dictionary<string, string>();
            headers.Add(Headers.MessageId, Guid.NewGuid().ToString());
            return headers;
        }
    }

    class MyMessage
    {
        public string CorrelationId { get; set; }
        public int Id { get; set; }
        public int Total { get; set; }
        public bool SendOutgoingMessage { get; set; }
        public override string ToString()
        {
            return $"MyMessage {Id}/{Total}";
        }
    }

    class MyIdempotentHandler : IHandleMessages<MyMessage>
    {
        private readonly IBus _bus;
        private readonly ConcurrentQueue<DateTime> _sentMessages;

        public MyIdempotentHandler(IBus bus, ConcurrentQueue<DateTime> messages)
        {
            _bus = bus;
            _sentMessages = messages;
        }

        public async Task Handle(MyMessage message)
        {
            Console.WriteLine($"Handling MyMessage message with id {message.Id}");
            _sentMessages.Enqueue(DateTime.Now);

            if (message.SendOutgoingMessage)
            {
                Console.WriteLine($"Before sending Outgoing message with id {message.Id}");
                var headers = new Dictionary<string, string>();
                headers.Add(Headers.MessageId, Guid.NewGuid().ToString());
                await _bus.SendLocal(new OutgoingMessage { Id = message.Id }, headers);
                Console.WriteLine($"After sending Outgoing message with id {message.Id}");
            }
        }
    }

    class OutgoingMessage
    {
        public int Id { get; set; }
    }

    class OutgoingMessageCollector : IHandleMessages<OutgoingMessage>
    {
        readonly ConcurrentQueue<OutgoingMessage> _receivedMessages;

        public OutgoingMessageCollector(ConcurrentQueue<OutgoingMessage> receivedMessages)
        {
            _receivedMessages = receivedMessages;
        }

        public async Task Handle(OutgoingMessage message)
        {
            Console.WriteLine($"Handled outgoing message with id {message.Id}");
            _receivedMessages.Enqueue(message);
        }
    }
}