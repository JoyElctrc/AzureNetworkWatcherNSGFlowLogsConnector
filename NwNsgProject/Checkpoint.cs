using Azure.Data.Tables;
using System;

namespace nsgFunc
{
    public class Checkpoint : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public Azure.ETag ETag { get; set; }

        public int CheckpointIndex { get; set; }  // index of the last processed block list item

        public Checkpoint()
        {
        }

        public Checkpoint(string partitionKey, string rowKey, string blockName, long offset, int index)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            CheckpointIndex = index;
        }

        public static Checkpoint GetCheckpoint(BlobDetails blobDetails, TableClient checkpointTable)
        {

            //TableOperation operation = TableOperation.Retrieve<Checkpoint>(
            //    blobDetails.GetPartitionKey(), blobDetails.GetRowKey());
            //TableResult result = checkpointTable.ExecuteAsync(operation).Result;

            //Checkpoint checkpoint = (Checkpoint)result.Result;

            Checkpoint checkpoint = null;

            Azure.NullableResponse<nsgFunc.Checkpoint> checkpointtest = checkpointTable.GetEntityIfExistsAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey()).Result;
            
            if (checkpointtest.HasValue)
            {
                checkpoint = checkpointTable.GetEntityAsync<Checkpoint>(blobDetails.GetPartitionKey(), blobDetails.GetRowKey()).Result;
            }
            if (checkpoint == null)
            {
                checkpoint = new Checkpoint(blobDetails.GetPartitionKey(), blobDetails.GetRowKey(), "", 0, 1);
            }
            if (checkpoint.CheckpointIndex == 0)
            {
                checkpoint.CheckpointIndex = 1;
            }

            return checkpoint;
        }

        public void PutCheckpoint(TableClient checkpointTable, int index)
        {
            CheckpointIndex = index;

            //TableOperation operation = TableOperation.InsertOrReplace(this);
            //checkpointTable.ExecuteAsync(operation).Wait();

            checkpointTable.UpsertEntityAsync(this).Wait();
        }
    }
}
