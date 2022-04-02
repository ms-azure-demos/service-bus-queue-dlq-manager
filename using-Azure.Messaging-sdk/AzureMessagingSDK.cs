using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace DeadLetterProcessor
{
    class AzureMessagingSDK
    {
        internal async static Task ReSubmitDeadLetterMessages(string connectionString, string queueName, int count)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);
            
            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            var message = await receiver.ReceiveMessageAsync();
            //message.clone
            // the sender used to publish messages to the topic
            ServiceBusSender sender = client.CreateSender(queueName);
        }

        internal async static Task DequeueMessage(string connectionString, string queueName, int count)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);

            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            var message = await receiver.ReceiveMessageAsync();
            Console.WriteLine($"DequeueMessage - received messsage. SequenceNo:{message.SequenceNumber}");
            await receiver.CompleteMessageAsync(message);
            Console.WriteLine($"DequeueMessage - completed messsage. SequenceNo:{message.SequenceNumber}");

        }
        internal async static Task SimulateFailure(string connectionString, string queueName)
        {
            ServiceBusClient client = GetServiceBusClient(connectionString);

            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            var message = await receiver.ReceiveMessageAsync();
            Console.WriteLine($"DequeueMessage - received messsage. SequenceNo:{message.SequenceNumber}");

            throw new Exception("Forceful failure");
        }
        private static ServiceBusClient GetServiceBusClient(string connectionString)
        {
            ServiceBusClientOptions clientOption = new ServiceBusClientOptions();
            clientOption.TransportType = ServiceBusTransportType.AmqpWebSockets;
            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client = new ServiceBusClient(connectionString, clientOption);
            Console.WriteLine("ServiceBusClient created ...");
            return client;
        }
    }
}
