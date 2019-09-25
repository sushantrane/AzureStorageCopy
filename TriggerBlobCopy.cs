using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using AzPremiumCopyDemo.Models;

namespace AzPremiumCopyDemo
{
    public static class TriggerBlobCopy
    {
        [FunctionName("TriggerBlobCopy")]
        public static async Task Run([QueueTrigger("copyblobs")]EventMetadata eventMetadata, ILogger log)
        {
            log.LogInformation($"C# Queue trigger function processed: {eventMetadata.id}");


            //Get references to the containers
            string[] subjectSplit = eventMetadata.subject.Split('/');
            int containerPosition = Array.IndexOf(subjectSplit, "containers");
            string container = subjectSplit[containerPosition + 1];
            log.LogInformation($"Container is: {container}");


            //Get reference to the blob name
            int blobPosition = Array.IndexOf(subjectSplit, "blobs");
            string fileName = subjectSplit[blobPosition + 1];
            log.LogInformation($"Blob name is: {fileName}");


            // Set up source storage account connection
            CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("SourceStorageAccountConnection", EnvironmentVariableTarget.Process));
            CloudBlobClient sourceStorageClient = sourceAccount.CreateCloudBlobClient();

            var sourceContainer = sourceStorageClient.GetContainerReference(container);

            // Set up destination storage account creation
            CloudStorageAccount destinationAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("DestinationStorageAccountConnection", EnvironmentVariableTarget.Process));

            CloudBlobClient destinationStorageClient = destinationAccount.CreateCloudBlobClient();
            // Create the replica container if it doesn't exist
            var destinationContainer = destinationStorageClient.GetContainerReference(container);

            await destinationContainer.CreateIfNotExistsAsync();

            if (eventMetadata.eventType == "Microsoft.Storage.BlobCreated")
            {
                //A new blob was created, replicate the blob to another storage account
                log.LogInformation($"EventType: {eventMetadata.eventType}");

                CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(fileName);
                string sourceBlobUriString = GetBlobSasUri(sourceContainer, fileName, null);
                Uri sourceBlobUri = new Uri(sourceBlobUriString);
                CloudBlockBlob sourceBlob = new CloudBlockBlob(sourceBlobUri);
                // if (await sourceBlob.ExistsAsync())
                // {
                // Create the replica
                log.LogInformation($"Copying {sourceBlob.Uri.ToString()} to {destinationBlob.Uri.ToString()}");

                await destinationBlob.StartCopyAsync(sourceBlob);
                ICloudBlob destBlobRef = await destinationContainer.GetBlobReferenceFromServerAsync(destinationBlob.Name);
                while (destBlobRef.CopyState.Status == CopyStatus.Pending)
                {
                    log.LogInformation($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                    await Task.Delay(500);
                    destBlobRef = await destinationContainer.GetBlobReferenceFromServerAsync(sourceBlob.Name);
                }
                log.LogInformation($"Blob: {fileName} Complete");
                log.LogInformation($"Copied blob {sourceBlob.Uri.ToString()} to {destinationBlob.Uri.ToString()}");

                // }
                // else
                // {
                //     log.LogInformation("Source blob does not exist, no copy made");
                //     log.LogInformation($"Blob {sourceBlob.Uri.ToString()} was not copied as it did not exist at run time");

                // }

            }
            else if (eventMetadata.eventType == "Microsoft.Storage.BlobDeleted")
            {
                //Blob was deleted, delete the replica if it exists
                log.LogInformation($"EventType: {eventMetadata.eventType}");
                CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(fileName);
                await destinationBlob.DeleteIfExistsAsync();
                log.LogInformation($"Deleted blob {destinationBlob.Uri.ToString()}");
            }
        }
        private static string GetBlobSasUri(CloudBlobContainer container, string blobName, string policyName = null)
        {
            string sasBlobToken;

            // Get a reference to a blob within the container.
            // Note that the blob may not exist yet, but a SAS can still be created for it.
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

            if (policyName == null)
            {
                // Create a new access policy and define its constraints.
                // Note that the SharedAccessBlobPolicy class is used both to define the parameters of an ad-hoc SAS, and
                // to construct a shared access policy that is saved to the container's shared access policies.
                SharedAccessBlobPolicy adHocSAS = new SharedAccessBlobPolicy()
                {
                    // When the start time for the SAS is omitted, the start time is assumed to be the time when the storage service receives the request.
                    // Omitting the start time for a SAS that is effective immediately helps to avoid clock skew.
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(1),
                    Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Create
                };

                // Generate the shared access signature on the blob, setting the constraints directly on the signature.
                sasBlobToken = blob.GetSharedAccessSignature(adHocSAS);
            }
            else
            {
                // Generate the shared access signature on the blob. In this case, all of the constraints for the
                // shared access signature are specified on the container's stored access policy.
                sasBlobToken = blob.GetSharedAccessSignature(null, policyName);
            }

            // Return the URI string for the container, including the SAS token.
            return blob.Uri + sasBlobToken;
        }
    }

}
