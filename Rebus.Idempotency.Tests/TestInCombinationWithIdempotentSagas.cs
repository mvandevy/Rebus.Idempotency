using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Handlers;
using Rebus.Idempotency.Persistence;
using Rebus.Logging;
using Rebus.Sagas;
using Rebus.Sagas.Idempotent;
using Rebus.Transport;
using Rebus.Transport.InMem;
using Xunit;

namespace Rebus.Idempotency.Tests
{
    public class TestInCombinationWithIdempotentSagas : UnitTestBase
    {
        private readonly BuiltinHandlerActivator _activator;
        private readonly IBus _bus;
        private readonly ConcurrentDictionary<string, int> _transportMessagesSent = new ConcurrentDictionary<string, int>();
        private readonly ConcurrentDictionary<string, int> _transportMessagesReceived = new ConcurrentDictionary<string, int>();

        public TestInCombinationWithIdempotentSagas()
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
                    o.EnableIdempotentSagas();
                })
                .Start();
        }

        [Fact]
        public async Task MyMessageSagaIsTriggered()
        {
            var allMessagesReceived = new ManualResetEvent(false);
            var handlersTriggered = new ConcurrentQueue<DateTime>();
            _activator.Register((b, context) => new MyMessageSaga(allMessagesReceived, b, handlersTriggered));

            await _bus.SendLocal(new MyMessage());

            await Task.Delay(1000);

            Assert.Equal(1, handlersTriggered.Count);
        }

        [Fact]
        public async Task BothSagaAndMessageHandlerAreTriggered()
        {
            var allMessagesReceived = new ManualResetEvent(false);
            var sagaHandlersTriggered = new ConcurrentQueue<DateTime>();
            var plainHandlersTriggered = new ConcurrentQueue<DateTime>();

            _activator.Register((b, context) => new MyMessageSaga(allMessagesReceived, b, sagaHandlersTriggered));
            _activator.Register((b, context) => new MyMessageHandler(b, plainHandlersTriggered));

            await _bus.SendLocal(new MyMessage());

            await Task.Delay(1000);

            Assert.Equal(1, sagaHandlersTriggered.Count);
            Assert.Equal(1, plainHandlersTriggered.Count);
        }


        [Fact]
        public async Task ResendOfOriginalMessageDoesntResultInReprocessing()
        {
            var allMessagesReceived = new ManualResetEvent(false);
            var myMessageHandlersTriggered = new ConcurrentQueue<DateTime>();
            var outgoingMessageHandlersTriggered = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyMessageSaga(allMessagesReceived, b, myMessageHandlersTriggered));
            _activator.Register((b, context) => new OutgoingMessageCollector(outgoingMessageHandlersTriggered));

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

            Assert.Equal(1, myMessageHandlersTriggered.Count);
            Assert.Equal(2, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(2, _transportMessagesReceived[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(2, _transportMessagesSent[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(2, _transportMessagesSent[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(1, outgoingMessageHandlersTriggered.Count);
        }

        [Fact]
        public async Task ResendOfOriginalMessageIsProcessedByBothSagaAndPlainHandler()
        {
            var allMessagesReceived = new ManualResetEvent(false);
            var sagaHandlersTriggered = new ConcurrentQueue<DateTime>();
            var plainHandlersTriggered = new ConcurrentQueue<DateTime>();
            var outgoingMessageHandlersTriggered = new ConcurrentQueue<OutgoingMessage>();

            _activator.Register((b, context) => new MyMessageSaga(allMessagesReceived, b, sagaHandlersTriggered));
            _activator.Register((b, context) => new MyMessageHandler(b, plainHandlersTriggered));
            _activator.Register((b, context) => new OutgoingMessageCollector(outgoingMessageHandlersTriggered));

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

            Assert.Equal(1, sagaHandlersTriggered.Count);
            Assert.Equal(1, plainHandlersTriggered.Count);
            Assert.Equal(2, _transportMessagesReceived[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(4, _transportMessagesReceived[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(2, _transportMessagesSent[typeof(MyMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(4, _transportMessagesSent[typeof(OutgoingMessage).GetSimpleAssemblyQualifiedName()]);
            Assert.Equal(2, outgoingMessageHandlersTriggered.Count);
        }

        #region Inner classes

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

        class MyMessageSaga : IdempotentSaga<MyMessageSagaData>, IAmInitiatedBy<MyMessage>
        {
            readonly ManualResetEvent _allMessagesReceived;
            private readonly ConcurrentQueue<DateTime> _sentMessages;
            readonly IBus _bus;

            public MyMessageSaga(ManualResetEvent allMessagesReceived, IBus bus, ConcurrentQueue<DateTime> sentMessages)
            {
                _allMessagesReceived = allMessagesReceived;
                _bus = bus;
                _sentMessages = sentMessages;
            }

            protected override void CorrelateMessages(ICorrelationConfig<MyMessageSagaData> config)
            {
                config.Correlate<MyMessage>(m => m.CorrelationId, d => d.CorrelationId);
            }

            public async Task Handle(MyMessage message)
            {
                _sentMessages.Enqueue(DateTime.Now);
                Data.CorrelationId = message.CorrelationId;

                if (!Data.CountPerId.ContainsKey(message.Id))
                {
                    Data.CountPerId[message.Id] = 0;
                }

                Data.CountPerId[message.Id]++;

                if (message.SendOutgoingMessage)
                {
                    await _bus.SendLocal(new OutgoingMessage { Id = message.Id });
                }

                if (Data.CountPerId.Count == message.Total)
                {
                    _allMessagesReceived.Set();
                }
            }
        }

        class MyMessageSagaData : IdempotentSagaData
        {
            public MyMessageSagaData()
            {
                CountPerId = new Dictionary<int, int>();
            }

            public string CorrelationId { get; set; }

            public Dictionary<int, int> CountPerId { get; }
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

        #endregion
    }
}
