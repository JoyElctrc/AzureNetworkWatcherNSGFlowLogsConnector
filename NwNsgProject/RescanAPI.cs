using Azure.Data.Tables;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace nsgFunc
{
    public static class RescanAPI
    {
        // https://<APP_NAME>.azurewebsites.net/api/rescan/2/17/8
        //
        [FunctionName("RescanAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "rescan/resourceId=/SUBSCRIPTIONS/{subId}/RESOURCEGROUPS/{resourceGroup}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{nsgName}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/macAddress={mac}/PT1H.json")]
//            [Table("checkpoints", Connection = "AzureWebJobsStorage")] CloudTable checkpointToReset,
            HttpRequest req,
            Binder checkpointsBinder,
            Binder nsgDataBlobBinder,
            string subId, string resourceGroup, string nsgName, string blobYear, string blobMonth, string blobDay, string blobHour, string blobMinute, string mac,
            ILogger log)
        {
            string nsgSourceDataAccount = Util.GetEnvironmentVariable("nsgSourceDataAccount");
            if (nsgSourceDataAccount.Length == 0)
            {
                log.LogError("Value for nsgSourceDataAccount is required.");
                throw new System.ArgumentNullException("nsgSourceDataAccount", "Please provide setting.");
            }

            string AzureWebJobsStorage = Util.GetEnvironmentVariable("AzureWebJobsStorage");
            if (AzureWebJobsStorage.Length == 0)
            {
                log.LogError("Value for AzureWebJobsStorage is required.");
                throw new System.ArgumentNullException("AzureWebJobsStorage", "Please provide setting.");
            }

            string blobContainerName = Util.GetEnvironmentVariable("blobContainerName");
            if (blobContainerName.Length == 0)
            {
                log.LogError("Value for blobContainerName is required.");
                throw new System.ArgumentNullException("blobContainerName", "Please provide setting.");
            }

            var blobName = $"resourceId=/SUBSCRIPTIONS/{subId}/RESOURCEGROUPS/{resourceGroup}/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/{nsgName}/y={blobYear}/m={blobMonth}/d={blobDay}/h={blobHour}/m={blobMinute}/macAddress={mac}/PT1H.json";
            var blobDetails = new BlobDetails(subId, resourceGroup, nsgName, blobYear, blobMonth, blobDay, blobHour, blobMinute, mac);

            var tableAttributes = new Attribute[]
            {
                new TableAttribute("checkpoints"),
                new StorageAccountAttribute("AzureWebJobsStorage")
            };

            try
            {
                TableClient CheckpointTable = await checkpointsBinder.BindAsync<TableClient>(tableAttributes);

                //Azure.NullableResponse<nsgFunc.Checkpoint> checkpointtest = CheckpointTable.GetEntityIfExistsAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey()).Result;

                Checkpoint c = CheckpointTable.GetEntityAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey()).Result;

                //TableOperation getOperation = TableOperation.Retrieve<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
                //TableResult result = await CheckpointTable.ExecuteAsync(getOperation);
                //Checkpoint c = (Checkpoint)result.Result;
                
                c.CheckpointIndex = 1;

                CheckpointTable.UpsertEntityAsync(c).Wait();

                //TableOperation putOperation = TableOperation.InsertOrReplace(c);
                //await CheckpointTable.ExecuteAsync(putOperation);
            }
            catch (Exception ex)
            {
                log.LogError(string.Format("Error binding checkpoints table: {0}", ex.Message));
                throw ex;
            }

            var attributes = new Attribute[]
            {
                new BlobAttribute(string.Format("{0}/{1}", blobContainerName, blobName)),
                new StorageAccountAttribute(nsgSourceDataAccount)
            };

            try
            {
                BlockBlobClient blob = await nsgDataBlobBinder.BindAsync<BlockBlobClient>(attributes);

                BlobProperties props = await blob.GetPropertiesAsync();

                //await blob.FetchAttributesAsync();

                var metadata = props.Metadata;
                if (metadata.ContainsKey("rescan"))
                {
                    int numberRescans = Convert.ToInt32(metadata["rescan"]);
                    metadata["rescan"] = (numberRescans + 1).ToString();
                }
                else
                {
                    metadata.Add("rescan", "1");
                }

                await blob.SetMetadataAsync(metadata);
                //await blob.SetMetadataAsync();
            }
            catch (Exception ex)
            {
                log.LogError(string.Format("Error binding blob input: {0}", ex.Message));
                throw ex;
            }

            return (ActionResult)new OkObjectResult($"NSG flow logs for {blobName} were requested.");

        }
    }
}
