using System;
using Azure.Messaging.ServiceBus;

namespace DeadLetterProcessor
{
    class AzureADK
    {
        private static void ReSubmitDeadLetterMessageUsingAzureSDK(string connectionString, string queueName, int count)
        {
            ServiceBusClientOptions clientOption = new ServiceBusClientOptions();
            //clientOption.TransportType = ServiceBusTransportType.AmqpWebSockets;
            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client = new ServiceBusClient(connectionString, clientOption);
            Console.WriteLine("ServiceBusClient initiated ...");
            ServiceBusReceiver receiver = client.CreateReceiver(queueName,
                new ServiceBusReceiverOptions() { ReceiveMode = ServiceBusReceiveMode.PeekLock });
            var message = receiver.ReceiveMessageAsync().Result;
            //message.clone
            // the sender used to publish messages to the topic
            ServiceBusSender sender = client.CreateSender(queueName);
        }
    }
}
