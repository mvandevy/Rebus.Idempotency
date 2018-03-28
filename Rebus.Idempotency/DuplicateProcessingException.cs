using System;

namespace Rebus.Idempotency
{
    internal class DuplicateProcessingException : Exception
    {
        public DuplicateProcessingException()
        {
        }

        public DuplicateProcessingException(string message) : base(message)
        {
        }

        public DuplicateProcessingException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}