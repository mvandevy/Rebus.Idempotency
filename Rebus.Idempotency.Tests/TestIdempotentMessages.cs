using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Tests.Contracts;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Idempotency.Persistence;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Tests.Contracts.Extensions;
using Rebus.Transport;
using Rebus.Transport.InMem;

namespace Rebus.Idempotency.Tests
{
    [TestFixture]
    public class TestIdempotentMessages : FixtureBase
    {
        const int MakeEveryFifthMessageFail = 5;
        BuiltinHandlerActivator _activator;
        IBus _bus;
        ConcurrentDictionary<string, MessageData> _persistentMessageData;

        protected override void SetUp()
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
                        return new IntroducerOfTransportInstability(transport, MakeEveryFifthMessageFail);
                    });
                })
                .Options(o =>
                {
                    o.EnableIdempotentMessages(new InMemoryMessageStorage());
                    o.LogPipeline(true);
                })
                .Start();
        }

        [Test]
        public async Task HandlerIsTriggered()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyIdempotentHandler(b, handlersTriggered));

            await _bus.SendLocal(new MyMessage());

            await Task.Delay(1000);

            Assert.That(handlersTriggered.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task ResendOfOriginalMessageDoesntResultInReprocessing()
        {
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyIdempotentHandler(b, handlersTriggered));

            var msgToSend = new MyMessage
            {
                Id = 1,
                Total = 2,
                SendOutgoingMessage = true
            };

            var headers = ConstructHeadersWithMessageId();
            await _bus.SendLocal(msgToSend, headers);
            await _bus.SendLocal(msgToSend, headers);

            await Task.Delay(1000);

            Assert.That(handlersTriggered.Count, Is.EqualTo(1));
        }

        [TestCase(10)]
        public async Task SlowReceiveOfDuplicateMessagesTriggersAllOutgoingMessages(int total)
        {
            if (total < MakeEveryFifthMessageFail)
            {
                Assert.Fail("Fail factor must be less than or equal to total!");
            }

            var handlersTriggered = new ConcurrentQueue<DateTime>();
            var receivedMessages = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyIdempotentHandler(b, handlersTriggered));
            _activator.Register(() => new OutgoingMessageCollector(receivedMessages));

            var messagesToSend = Enumerable
                .Range(0, total)
                .Select(id => new MyMessage
                {
                    Id = 1,
                    Total = total,
                    SendOutgoingMessage = true
                })
                .ToList();
            var headers = ConstructHeadersWithMessageId();

            messagesToSend.ForEach(async message =>
            {
                await _bus.SendLocal(message, headers);
                await Task.Delay(100);
            });

            Console.WriteLine("All messages processed - waiting for messages in outgoing message collector...");

            await Task.Delay(2000);

            Assert.That(handlersTriggered.Count, Is.EqualTo(1), "The handler should only have been triggered once.");
            Assert.That(receivedMessages.Count, Is.EqualTo(total), "Not all outgoing messages where received.");
        }

        [TestCase(10)]
        public async Task OutgoingMessagesAreAllRetriggered(int total)
        {
            if (total < MakeEveryFifthMessageFail)
            {
                Assert.Fail("Fail factor must be less than or equal to total!");
            }

            var handlersTriggered = new ConcurrentQueue<DateTime>();
            var receivedMessages = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyIdempotentHandler(b, handlersTriggered));
            _activator.Register(() => new OutgoingMessageCollector(receivedMessages));

            var messagesToSend = Enumerable
                .Range(0, total)
                .Select(id => new MyMessage
                {
                    Id = 1,
                    Total = total,
                    SendOutgoingMessage = true
                })
                .ToList();
            var headers = ConstructHeadersWithMessageId();

            await Task.WhenAll(messagesToSend.Select(message => _bus.SendLocal(message, headers)));

            Console.WriteLine("All messages processed - waiting for messages in outgoing message collector...");

            await Task.Delay(2000);

            Assert.That(handlersTriggered.Count, Is.EqualTo(1), "The handler should only have been triggered once.");
            Assert.That(receivedMessages.Count, Is.EqualTo(total), "Not all outgoing messages where received.");
        }

        private Dictionary<string, string> ConstructHeadersWithMessageId()
        {
            var headers = new Dictionary<string, string>();
            headers.Add(Headers.MessageId, Guid.NewGuid().ToString());
            return headers;
        }

        class IntroducerOfTransportInstability : ITransport
        {
            readonly ITransport _innerTransport;
            readonly int _failFactor;
            int _failCounter;

            public IntroducerOfTransportInstability(ITransport innerTransport, int failFactor)
            {
                _innerTransport = innerTransport;
                _failFactor = failFactor;
            }

            public void CreateQueue(string address)
            {
                _innerTransport.CreateQueue(address);
            }

            public async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
            {
                await _innerTransport.Send(destinationAddress, message, context);
            }

            public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
            {
                var transportMessage = await _innerTransport.Receive(context, cancellationToken);
                if (transportMessage == null) return null;

                var shouldFailThisTime = Interlocked.Increment(ref _failCounter) % _failFactor == 0;

                if (shouldFailThisTime)
                {
                    context.OnCommitted(async () =>
                    {
                        throw new Exception("oh noes!!!!!");
                    });
                }

                return transportMessage;
            }

            public string Address
            {
                get { return _innerTransport.Address; }
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
                _sentMessages.Enqueue(DateTime.Now);

                if (message.SendOutgoingMessage)
                {
                    await _bus.SendLocal(new OutgoingMessage { Id = message.Id });
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
                _receivedMessages.Enqueue(message);
            }
        }
    }
}
