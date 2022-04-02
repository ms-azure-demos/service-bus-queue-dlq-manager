using System;
using Microsoft.ServiceBus.Messaging;

namespace DeadLetterProcessor
{
    class OldDK
    {
        internal static void DequeueMessage(string connectionString, string queueName, int count)
        {
            var client = QueueClient.CreateFromConnectionString(connectionString, queueName);
            BrokeredMessage originalMessage;
            var queue = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);

            int processed = 0;
            do
            {
                originalMessage = queue.Receive();
                Console.WriteLine($"Message received from queue. SequenceNo:{originalMessage.SequenceNumber}");
                originalMessage.Complete();
                processed++;
            } while (originalMessage != null && processed < count);
        }
    }
}
