using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Moq;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.Transport;
using Rebus.Transport.InMem;
using Xunit;


namespace Rebus.Idempotency.Tests
{
    // public class TestIncorrectMessages : UnitTestBase
    // {
    //     const int MakeEveryFifthMessageFail = 5;
    //     private readonly BuiltinHandlerActivator _activator;
    //     private readonly IBus _bus;
    //     private readonly ConcurrentDictionary<string, int> _transportMessagesSent = new ConcurrentDictionary<string, int>();
    //     private readonly ConcurrentDictionary<string, int> _transportMessagesReceived = new ConcurrentDictionary<string, int>();
    //     private readonly MockRepository _mockRepository;
    //     private readonly Mock<IMessageStorage> _storage;
    //     public TestIncorrectMessages()
    //     {
    //         _activator = Using(new BuiltinHandlerActivator());
    //         _mockRepository = new MockRepository(MockBehavior.Strict);
    //         _storage = _mockRepository.Create<IMessageStorage>();
    //         _bus = Configure.With(_activator)
    //             .Logging(l => l.Console(LogLevel.Info))
    //             .Transport(t =>
    //             {
    //                 t.UseInMemoryTransport(new InMemNetwork(), "bimse");
    //                 t.Decorate(c =>
    //                 {
    //                     var transport = c.Get<ITransport>();
    //                     return new TransportCounter(transport, _transportMessagesSent, _transportMessagesReceived);
    //                 });
    //             })
    //             .Options(o =>
    //             {
    //                 o.EnableIdempotentMessages(_storage.Object);
    //                 o.LogPipeline(true);
    //             })
    //             .Start();
    //     }

    //     [Fact]
    //     public async Task When_AnExceptionIsThrownInTheMessageHandler_Then_TheMessageIsNotMarkedAsHandled()
    //     {
    //         var handlersTriggered = new ConcurrentQueue<DateTime>();
    //         _activator.Register((b, context) => new MyErrorMessageHandler(b, handlersTriggered));
    //         var myMessage = new TestIdempotentMessages.MyMessage();
    //         var messageData = new MessageData();
            
    //         _storage.Setup(s => s.Find(It.IsAny<string>())).Returns(Task.FromResult(messageData));
    //         _storage.Setup(s => s.IsProcessing(It.IsAny<string>())).Returns(Task.FromResult(false));
    //         _storage.Setup(s => s.InsertOrUpdate(messageData)).Returns(Task.CompletedTask).Verifiable();            
            
    //         await _bus.SendLocal(myMessage);
    //         await _bus.SendLocal(myMessage);

    //         await Task.Delay(5000);

    //         Assert.Equal(0, handlersTriggered.Count);
    //         _storage.VerifyAll();
            
    //     }

    //     [Fact]
    //     public async Task Given_MessageIsBeingProcessed_When_AnExceptionIsThrownInTheMessageHandler_Then_TheMessageIsNotMarkedAsHandled()
    //     {
    //         var handlersTriggered = new ConcurrentQueue<DateTime>();
    //         _activator.Register((b, context) => new MyErrorMessageHandler(b, handlersTriggered));
    //         var myMessage = new TestIdempotentMessages.MyMessage();
    //         var messageData = new MessageData();
            
    //         _storage.Setup(s => s.Find(It.IsAny<string>())).Returns(Task.FromResult(messageData));
    //         _storage.Setup(s => s.IsProcessing(It.IsAny<string>())).Returns(Task.FromResult(false));
    //         _storage.Setup(s => s.InsertOrUpdate(messageData)).Returns(Task.CompletedTask);
            
    //         await _bus.SendLocal(myMessage);
    //         await _bus.SendLocal(myMessage);

    //         await Task.Delay(5000);

    //         Assert.Equal(1, handlersTriggered.Count);
            
    //     }

    //     class MyErrorMessageHandler : IHandleMessages<TestIdempotentMessages.MyMessage>
    //     {
    //         private readonly IBus _bus;
    //         private readonly ConcurrentQueue<DateTime> _messagesSentOn;

    //         public MyErrorMessageHandler(IBus bus, ConcurrentQueue<DateTime> messagesSentOn)
    //         {
    //             _bus = bus;
    //             _messagesSentOn = messagesSentOn;
    //         }

    //         public async Task Handle(TestIdempotentMessages.MyMessage message)
    //         {
    //             throw new InvalidOperationException("we cannot process the message");
    //         }
    //     }
    // }
}