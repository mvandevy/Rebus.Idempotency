using System;
using Xunit;

namespace Rebus.Idempotency.Tests
{
    public class MessageIdTests
    {
        [Fact]
        public void Given_OriginalMessage_When_CreateMessageId_Then_MessageIdIsGuid()
        {
            var id = Guid.NewGuid();

            var messageId = new MessageId(id.ToString());

            Assert.Equal(id.ToString(), messageId);
        }

        [Fact]
        public void Given_DeferredMessage_When_CreateMessageId_Then_MessageIdIsGuidWithDeferCount()
        {
            var id = Guid.NewGuid();

            var messageId = new MessageId(id.ToString(), 5);

            Assert.Equal($"{id}#5", messageId);
        }

        [Fact]
        public void Given_MessageIdIsGuid_When_DeconstructMessageId_Then_IdIsGuid()
        {
            var id = Guid.NewGuid();
            var messageId = $"{id}";

            Assert.Equal(messageId, new MessageId(id.ToString()));
            Assert.Equal(id, Guid.Parse(((MessageId)messageId).OriginalMessageId));
            Assert.Equal(0, ((MessageId)messageId).DeferCount);
        }

        [Fact]
        public void Given_MessageIdIsGuidWithDeferCount_When_DeconstructMessageId_Then_PropertiesAreFilled()
        {
            var id = Guid.NewGuid();
            var messageId = $"{id}#5";

            Assert.Equal(messageId, new MessageId(id.ToString(), 5));
            Assert.Equal(id.ToString(), ((MessageId)messageId).OriginalMessageId);
            Assert.Equal(5, ((MessageId)messageId).DeferCount);
        }
    }
}