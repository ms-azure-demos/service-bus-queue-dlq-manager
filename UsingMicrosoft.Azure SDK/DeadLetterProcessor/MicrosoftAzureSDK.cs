using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

using Microsoft.Azure.ServiceBus.Core;
namespace DeadLetterProcessor
{
    class MicrosoftAzureSDK
    {
        internal static void ReSubmitDeadLetterMessageUsingMicrosoftAzureSDK(string connectionString, string queueName, int count)
        {
            int processed = 0;
            Message message;
            do
            {
                // here, we create a receiver on the Deadletter queue
                var dlqReceiver = new Microsoft.Azure.ServiceBus.Core.MessageReceiver(connectionString,
                EntityNameHelper.FormatDeadLetterPath(queueName),
                Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock);
                dlqReceiver.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;

                var resubmitSender = new MessageSender(connectionString, queueName);
                resubmitSender.ServiceBusConnection.TransportType = TransportType.AmqpWebSockets;
                // close the receiver and factory when the CancellationToken fires 
                //cancellationToken.Register(
                //    async () =>
                //    {
                //        await dlqReceiver.CloseAsync();
                //        doneReceiving.SetResult(true);
                //    });
                // register the RegisterMessageHandler callback
                message = dlqReceiver.ReceiveAsync().Result;
                if (message != null)
                {
                    Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - Received Message. Sequecen :{message.MessageId}");
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
                        resubmitSender.SendAsync(resubmitMessage).Wait();
                    }
                    // finally complete the original message and remove it from the DLQ
                    dlqReceiver.CompleteAsync(message.SystemProperties.LockToken).Wait();
                    Console.WriteLine($"{nameof(MicrosoftAzureSDK)} - REsubmitted the deadletter message back to queue. MessageId: {message.MessageId}");
                    processed++;
                }
                else
                {
                    Console.WriteLine("Not dequeued");
                }
            } while (message != null && processed < count);
        } 

        private static Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine($"Exceptio {e.Exception}");
            return Task.Delay(1);
        }
    }
}
