using Rebus.Config;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Pipeline.Send;
using Rebus.Transport;

namespace Rebus.Idempotency
{
    public static class IdempotentMessageConfigurationExtensions
    {
        public static void EnableIdempotentMessages(this OptionsConfigurer configurer, IMessageStorage messageStorage) 
        {   
            messageStorage.Verify().Wait();
            configurer.Decorate<IPipeline>(c =>
            {
                var transport = c.Get<ITransport>();
                var pipeline = c.Get<IPipeline>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();

                var incomingStep = new IdempotentMessageIncomingStep(transport, messageStorage, rebusLoggerFactory);
                var loadMessageDataStep = new LoadMessageDataStep(messageStorage, rebusLoggerFactory);

                var outgoingStep = new IdempotentMessageOutgoingStep(rebusLoggerFactory);

                var injector = new PipelineStepInjector(pipeline)
                    .OnReceive(loadMessageDataStep, PipelineRelativePosition.Before, typeof(DispatchIncomingMessageStep))
                    .OnReceive(incomingStep, PipelineRelativePosition.Before, typeof(DispatchIncomingMessageStep))
                    .OnSend(outgoingStep, PipelineRelativePosition.After, typeof(SendOutgoingMessageStep));

                return injector;
            });
        }
    }
}
