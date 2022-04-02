using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

using Microsoft.Azure.ServiceBus.Core;
namespace DeadLetterProcessor
{
    class MicrosoftAzureSDK
    {
        internal async static Task ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            Message message;
            var dlqReceiver = new MessageReceiver(connectionString,
               EntityNameHelper.FormatDeadLetterPath(queueName),
               Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock);
            dlqReceiver.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;

            var resubmitSender = new MessageSender(connectionString, queueName);
            resubmitSender.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;

            int processed = 0;
            do
            {
                // here, we create a receiver on the Deadletter queue
                message = await dlqReceiver.ReceiveAsync();
                if (message != null)
                {
                    Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - Received Message. Sequence :{message.MessageId}");
                    // first, we create a clone of the picked up message
                    // that we can resubmit. 
                    var resubmitMessage = message.Clone();
                    // if the cloned message has an "error" we know the main loop
                    // can't handle, let's fix the message
                    if (resubmitMessage != null)
                    {
                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - Resubmitting: MessageId : {message.MessageId},SequenceNumber : {message.SystemProperties.SequenceNumber}, Label = {message.Label}");
                            Console.ResetColor();
                        }
                        // set the label to "Scientist"
                        // and re-enqueue the cloned message
                        await resubmitSender.SendAsync(resubmitMessage);
                    }
                    // finally complete the original message and remove it from the DLQ
                    await dlqReceiver.CompleteAsync(message.SystemProperties.LockToken);
                    Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - REsubmitted the deadletter message back to queue. MessageId: {message.MessageId}");
                    processed++;
                }
                else
                {
                    Console.WriteLine("Not dequeued");
                }
            } while (message != null && processed < count);
        }
        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            var receiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock);
            Message originalMessage = null;
            int processed = 0;
            do
            {
                originalMessage = await receiver.ReceiveAsync();
                Console.WriteLine($"Message received from queue. SequenceNo:{originalMessage.MessageId}");
                await receiver.CompleteAsync(originalMessage.SystemProperties.LockToken);
                processed++;
            } while (originalMessage != null && processed < count);
        }
        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            var dlqReceiver = new MessageReceiver(connectionString, queueName, ReceiveMode.PeekLock);

            var originalMessage = await dlqReceiver.ReceiveAsync();
            throw new Exception("Forceful failure");
        }
    }
}