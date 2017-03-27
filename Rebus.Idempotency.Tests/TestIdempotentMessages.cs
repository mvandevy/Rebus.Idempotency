using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Handlers;
using Rebus.Idempotency.Persistence;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Transport;
using Rebus.Transport.InMem;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
namespace Rebus.Idempotency.Tests
{
    public class TestIdempotentMessages : UnitTestBase
    {
        const int MakeEveryFifthMessageFail = 5;
        private readonly BuiltinHandlerActivator _activator;
        private readonly IBus _bus;
        private readonly ConcurrentDictionary<string, int> _transportMessagesSent = new ConcurrentDictionary<string, int>();
        private readonly ConcurrentDictionary<string, int> _transportMessagesReceived = new ConcurrentDictionary<string, int>();

        public TestIdempotentMessages()
        {
            _activator = Using(new BuiltinHandlerActivator());

            _bus = Configure.With(_activator)
                .Logging(l => l.Console(LogLevel.Info))
                .Transport(t =>
                {
                    t.UseInMemoryTransport(new InMemNetwork(), "bimse");
                    t.Decorate(c =>
                    {
                        var transport = c.Get<ITransport>();
                        return new TransportCounter(transport, _transportMessagesSent, _transportMessagesReceived);
                    });
                })
                .Options(o =>
                {
                    o.EnableIdempotentMessages(new InMemoryMessageStorage());
                    o.LogPipeline(true);
                })
                .Start();
        }

        [Fact]
        public async Task MyMessageHandlerIsTriggered()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyMessageHandler(b, handlersTriggered));

            await _bus.SendLocal(new MyMessage());

            await Task.Delay(1000);

            Assert.Equal(1, handlersTriggered.Count);
        }

        [Fact]
        public async Task AllMessageHandlersAreTriggered()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyMessageHandler(b, handlersTriggered));
            _activator.Register((b, context) => new MyMessageHandler2(b, handlersTriggered));

            await _bus.SendLocal(new MyMessage());

            await Task.Delay(1000);

            Assert.Equal(2, handlersTriggered.Count);
        }

        [Fact]
        public async Task ResendOfOriginalMessageWithSingleHandlerDoesntResultInReprocessing()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyMessageHandler(b, handlersTriggered));

            var msgToSend = new MyMessage
            {
                Id = 1,
                Total = 2,
                SendOutgoingMessage = true
            };

            var headers = HeaderHelper.ConstructHeadersWithMessageId();
            await _bus.SendLocal(msgToSend, headers);
            await _bus.SendLocal(msgToSend, headers);

            await Task.Delay(1000);

            Assert.Equal(1, handlersTriggered.Count);
            Assert.Equal(2, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
        }

        [Fact]
        public async Task ResendOfOriginalMessageWithMultipleHandlersDoesntResultInReprocessing()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyMessageHandler(b, handlersTriggered));
            _activator.Register((b, context) => new MyMessageHandler2(b, handlersTriggered));

            var msgToSend = new MyMessage
            {
                Id = 1,
                Total = 2,
                SendOutgoingMessage = true
            };

            var headers = HeaderHelper.ConstructHeadersWithMessageId();
            await _bus.SendLocal(msgToSend, headers);
            await _bus.SendLocal(msgToSend, headers);

            await Task.Delay(1000);

            Assert.Equal(2, handlersTriggered.Count);
            Assert.Equal(2, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
        }

        [Theory]
        [InlineData(10)]
        public async Task OutgoingMessagesAreAllRetriggeredWithSingleHandler(int total)
        {
            if (total < MakeEveryFifthMessageFail)
            {
                Assert.True(false, "Fail factor must be less than or equal to total!");
            }

            var myMessageHandlersTriggered = new ConcurrentQueue<DateTime>();
            var outgoingMessageHandlersTriggered = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyMessageHandler(b, myMessageHandlersTriggered));
            _activator.Register(() => new OutgoingMessageCollector(outgoingMessageHandlersTriggered));

            var messagesToSend = Enumerable
                .Range(0, total)
                .Select(id => new MyMessage
                {
                    Id = 1,
                    Total = total,
                    SendOutgoingMessage = true
                })
                .ToList();
            var headers = HeaderHelper.ConstructHeadersWithMessageId();

            await Task.WhenAll(messagesToSend.Select(message => _bus.SendLocal(message, headers)));

            Console.WriteLine("All messages processed - waiting for messages in outgoing message collector...");

            await Task.Delay(2000);

            Assert.Equal(1, myMessageHandlersTriggered.Count);
            Assert.Equal(total, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesReceived[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesSent[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesSent[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(1, outgoingMessageHandlersTriggered.Count);
        }

        [Theory]
        [InlineData(10)]
        public async Task OutgoingMessagesAreAllRetriggeredWithMultipleHandlers(int total)
        {
            if (total < MakeEveryFifthMessageFail)
            {
                Assert.True(false, "Fail factor must be less than or equal to total!");
            }

            var myMessageHandlersTriggered = new ConcurrentQueue<DateTime>();
            var outgoingMessageHandlersTriggered = new ConcurrentQueue<OutgoingMessage>();
            var outgoingMessage2HandlersTriggered = new ConcurrentQueue<OutgoingMessage2>();

            _activator.Register((b, context) => new MyMessageHandler(b, myMessageHandlersTriggered));
            _activator.Register((b, context) => new MyMessageHandler2(b, myMessageHandlersTriggered));
            _activator.Register(() => new OutgoingMessageCollector(outgoingMessageHandlersTriggered));
            _activator.Register(() => new OutgoingMessage2Collector(outgoingMessage2HandlersTriggered));

            var messagesToSend = Enumerable
                .Range(0, total)
                .Select(id => new MyMessage
                {
                    Id = 1,
                    Total = total,
                    SendOutgoingMessage = true
                })
                .ToList();
            var headers = HeaderHelper.ConstructHeadersWithMessageId();

            await Task.WhenAll(messagesToSend.Select(message => _bus.SendLocal(message, headers)));

            Console.WriteLine("All messages processed - waiting for messages in outgoing message collector...");

            await Task.Delay(2000);

            Assert.Equal(2, myMessageHandlersTriggered.Count);
            Assert.Equal(total, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesReceived[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesReceived[typeof(OutgoingMessage2).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesSent[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesSent[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(total, _transportMessagesSent[typeof(OutgoingMessage2).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(1, outgoingMessageHandlersTriggered.Count);
            Assert.Equal(1, outgoingMessage2HandlersTriggered.Count);
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

        class MyMessageHandler : IHandleMessages<MyMessage>
        {
            private readonly IBus _bus;
            private readonly ConcurrentQueue<DateTime> _sentMessages;

            public MyMessageHandler(IBus bus, ConcurrentQueue<DateTime> messages)
            {
                _bus = bus;
                _sentMessages = messages;
            }

            public async Task Handle(MyMessage message)
            {
                _sentMessages.Enqueue(DateTime.Now);

                if (message.SendOutgoingMessage)
                {
                    await _bus.SendLocal(new OutgoingMessage { Id = message.Id });
                }
            }
        }

        class MyMessageHandler2 : IHandleMessages<MyMessage>
        {
            private readonly IBus _bus;
            private readonly ConcurrentQueue<DateTime> _messagesSentOn;

            public MyMessageHandler2(IBus bus, ConcurrentQueue<DateTime> messagesSentOn)
            {
                _bus = bus;
                _messagesSentOn = messagesSentOn;
            }

            public async Task Handle(MyMessage message)
            {
                _messagesSentOn.Enqueue(DateTime.Now);

                if (message.SendOutgoingMessage)
                {
                    await _bus.SendLocal(new OutgoingMessage2 {Id = message.Id});
                }
            }
        }

        class OutgoingMessage
        {
            public int Id { get; set; }
        }

        class OutgoingMessage2
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

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
            public async Task Handle(OutgoingMessage message)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
            {
                _receivedMessages.Enqueue(message);
            }
        }

        class OutgoingMessage2Collector : IHandleMessages<OutgoingMessage2>
        {
            readonly ConcurrentQueue<OutgoingMessage2> _receivedMessages;

            public OutgoingMessage2Collector(ConcurrentQueue<OutgoingMessage2> receivedMessages)
            {
                _receivedMessages = receivedMessages;
            }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
            public async Task Handle(OutgoingMessage2 message)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
            {
                _receivedMessages.Enqueue(message);
            }
        }
    }
}