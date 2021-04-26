
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Messages;
using Xunit;

namespace Rebus.Idempotency.MySql.Tests
{
    public class TestIdempotentMessageStorage : UnitTestBase
    {
        private readonly MySqlMessageStorage _messageStorage;
        private readonly string _tableName = "idempotent_messages";

        public TestIdempotentMessageStorage()
        {
            MySqlTestHelper.DropTableIfExists(_tableName).Wait();
            var consoleLoggerFactory = new ConsoleLoggerFactory(false);
            var connectionHelper = new MySqlConnectionHelper(MySqlTestHelper.ConnectionString);
            _messageStorage = new MySqlMessageStorage(connectionHelper, _tableName, consoleLoggerFactory);
            _messageStorage.EnsureTablesAreCreated().Wait();
            Using(_messageStorage);
        }

        [Fact]
        public async Task TestBasicStoreAndRetrieve()
        {
            // Arrange
            var msgId = Guid.NewGuid().ToString();

            var msgData = new MessageData
            {
                MessageId = msgId,
                InputQueueAddress = "testQueueName"
            };

            // Act
            await _messageStorage.InsertOrUpdate(msgData);
            var retrievedMsgData = await _messageStorage.Find(msgId);

            // Assert
            Assert.Equal(msgData.MessageId, retrievedMsgData.MessageId);
            Assert.Equal(msgData.InputQueueAddress, retrievedMsgData.InputQueueAddress);
            Assert.Equal(null, retrievedMsgData.ProcessingThreadId);
            Assert.Equal(null, retrievedMsgData.TimeThreadIdAssigned);
            Assert.True(retrievedMsgData.IdempotencyData != null);
        }

        [Fact]
        public async Task TestFullBlownStoreAndRetrieve()
        {
            // Arrange
            var msgId = Guid.NewGuid().ToString();

            var idempotencyData = new IdempotencyData();
            idempotencyData.MarkMessageAsHandled(Guid.NewGuid().ToString());
            idempotencyData.AddOutgoingMessage(
                Guid.NewGuid().ToString(),
                new[] { "test_destination" },
                new TransportMessage(new Dictionary<string, string>() { { "rbs2-msg-id", msgId.ToString() } }, Encoding.ASCII.GetBytes("test_body")));

            var msgData = new MessageData
            {
                MessageId = msgId,
                InputQueueAddress = "testQueueName",
                ProcessingThreadId = 123,
                TimeThreadIdAssigned = new DateTime(2017, 3, 27, 1, 23, 45),
                IdempotencyData = idempotencyData
            };

            // Act
            await _messageStorage.InsertOrUpdate(msgData);
            var retrievedMsgData = await _messageStorage.Find(msgId);

            // Assert
            Assert.Equal(msgData.MessageId, retrievedMsgData.MessageId);
            Assert.Equal(msgData.InputQueueAddress, retrievedMsgData.InputQueueAddress);
            Assert.Equal(msgData.ProcessingThreadId, retrievedMsgData.ProcessingThreadId);
            Assert.Equal(msgData.TimeThreadIdAssigned, retrievedMsgData.TimeThreadIdAssigned);
            Assert.Equal(msgData.IdempotencyData.HandledMessageIds, retrievedMsgData.IdempotencyData.HandledMessageIds);
            Assert.Equal(msgData.IdempotencyData.OutgoingMessages[0].MessageId, retrievedMsgData.IdempotencyData.OutgoingMessages[0].MessageId);
        }

        [Fact]
        public async Task TestFullBlownStoreAndRetrieveDeferredMessage()
        {
            // Arrange
            var msgId = new MessageId(Guid.NewGuid(), 2);

            var idempotencyData = new IdempotencyData();
            idempotencyData.MarkMessageAsHandled(Guid.NewGuid().ToString());
            idempotencyData.AddOutgoingMessage(
                Guid.NewGuid().ToString(),
                new[] { "test_destination" },
                new TransportMessage(new Dictionary<string, string>() { 
                    { "rbs2-msg-id", msgId.OriginalMessageId.ToString() },
                    { "rbs2-defer-count", msgId.DeferCount.ToString() } }, 
                    Encoding.ASCII.GetBytes("test_body")));

            var msgData = new MessageData
            {
                MessageId = msgId,
                InputQueueAddress = "testQueueName",
                ProcessingThreadId = 123,
                TimeThreadIdAssigned = new DateTime(2017, 3, 27, 1, 23, 45),
                IdempotencyData = idempotencyData
            };

            // Act
            await _messageStorage.InsertOrUpdate(msgData);
            var retrievedMsgData = await _messageStorage.Find(msgId);

            // Assert
            Assert.Equal(msgData.MessageId.OriginalMessageId, retrievedMsgData.MessageId.OriginalMessageId);
            Assert.Equal(msgData.MessageId.DeferCount, retrievedMsgData.MessageId.DeferCount);
            Assert.Equal(msgData.InputQueueAddress, retrievedMsgData.InputQueueAddress);
            Assert.Equal(msgData.ProcessingThreadId, retrievedMsgData.ProcessingThreadId);
            Assert.Equal(msgData.TimeThreadIdAssigned, retrievedMsgData.TimeThreadIdAssigned);
            Assert.Equal(msgData.IdempotencyData.HandledMessageIds, retrievedMsgData.IdempotencyData.HandledMessageIds);
            Assert.Equal(msgData.IdempotencyData.OutgoingMessages[0].MessageId, retrievedMsgData.IdempotencyData.OutgoingMessages[0].MessageId);
        }

        [Fact]
        public async Task TestIdempotentMessageIsMarkedAsIsProcessingWhenItHasProcessingThreadId()
        {
            // Arrange
            var msgId = Guid.NewGuid().ToString();

            var msgData = new MessageData
            {
                MessageId = msgId,
                InputQueueAddress = "testQueueName",
                ProcessingThreadId = 123,
                TimeThreadIdAssigned = new DateTime(2017, 3, 27, 1, 23, 45)
            };

            // Act
            await _messageStorage.InsertOrUpdate(msgData);
            var isProcessing = await _messageStorage.IsProcessing(msgId);

            // Assert
            Assert.True(isProcessing);
        }

        [Fact]
        public async Task TestIdempotentMessageIsNotMarkedAsIsProcessingWhenItHasNoProcessingThreadId()
        {
            // Arrange
            var msgId = Guid.NewGuid().ToString();

            var msgData = new MessageData
            {
                MessageId = msgId,
                InputQueueAddress = "testQueueName",
                TimeThreadIdAssigned = new DateTime(2017, 3, 27, 1, 23, 45)
            };

            // Act
            await _messageStorage.InsertOrUpdate(msgData);
            var isProcessing = await _messageStorage.IsProcessing(msgId);

            // Assert
            Assert.False(isProcessing);
        }

        [Fact]
        public async Task TestVerify()
        {
            await _messageStorage.Verify();
        }
    }
}
