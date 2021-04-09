using System;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;
using Rebus.Transport;

namespace Rebus.Idempotency
{

    /// <summary>
    /// Outgoing pipeline step that stores the sent message in the current message data
    /// </summary>
    [StepDocumentation("Collects the outgoing messages which need to be resent once a message is processed twice.")]
    public class IdempotentMessageOutgoingStep : IOutgoingStep
    {
        private readonly ILog _log;

        public IdempotentMessageOutgoingStep(IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _log = rebusLoggerFactory.GetLogger<IdempotentMessageOutgoingStep>();
        }

        public async Task Process(OutgoingStepContext context, Func<Task> next)
        {
            var transactionContext = context.Load<ITransactionContext>();

            if (transactionContext.Items.TryGetValue(Keys.MessageData, out object temp))
            {
                var msgData = (MessageData)temp;

                if (msgData != null)
                {
                    var transportMessage = context.Load<TransportMessage>();
                    var destinationAddresses = context.Load<DestinationAddresses>();
                    var incomingStepContext = transactionContext.Items.GetOrThrow<IncomingStepContext>(StepContext.StepContextKey);
                    var messageId = incomingStepContext.Load<Message>().GetMessageIdWithDeferCount();

                    _log.Info($"Adding outgoing message with ID {transportMessage.Headers[Headers.MessageId]} for message with ID {msgData.MessageId} onto the message data.");
                    msgData.AddOutgoingMessage(messageId, destinationAddresses, transportMessage);
                }
            }

            await next();
        }
    }
}
