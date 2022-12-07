using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

namespace nsgFunc
{
    public partial class Util
    {
        private static Lazy<EventHubProducerClient> LazyEventHubConnection = new Lazy<EventHubProducerClient>(() =>
        {
            string EventHubConnectionString = GetEnvironmentVariable("eventHubConnection");
            string EventHubName = GetEnvironmentVariable("eventHubName");

            //var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            //{
            //    EntityPath = EventHubName
            //};
            //var eventHubClient = EventHubProducerClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            var eventHubClient = new EventHubProducerClient(EventHubConnectionString, EventHubName);

            return eventHubClient;
        });

        public static async Task<int> obEventHub(string newClientContent, ILogger log)
        {
            int bytesSent = 0;
            var eventHubClient = LazyEventHubConnection.Value;

            foreach (var bundleOfMessages in bundleMessageListsJson(newClientContent, log))
            {
                try
                {
                    using EventDataBatch eventBatch = await eventHubClient.CreateBatchAsync();
                    var eventData = new EventData(Encoding.UTF8.GetBytes(bundleOfMessages));
                    if (!eventBatch.TryAdd(eventData))
                    {
                        log.LogError($"The event at {bundleOfMessages} could not be added.");
                    }

                    //await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(bundleOfMessages)));
                    //bytesSent += bundleOfMessages.Length;

                    await eventHubClient.SendAsync(eventBatch);
                    bytesSent += bundleOfMessages.Length;
                }
                finally
                {            
                }
            }
            //await eventHubClient.CloseAsync();
            return bytesSent;
        }

        static System.Collections.Generic.IEnumerable<string> bundleMessageListsJson(string newClientContent, ILogger log)
        {
            foreach (var messageList in denormalizedRecords(newClientContent, null, log))
            {
                var outgoingRecords = new OutgoingRecords();
                outgoingRecords.records = messageList;

                var outgoingJson = JsonConvert.SerializeObject(outgoingRecords, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

                yield return outgoingJson;
            }
        }
    }
}
