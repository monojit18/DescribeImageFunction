using System;
using System.Text;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using DescribeImageFunction.Models;

namespace DescribeImageFunction
{
    public static class DescribeImageFunction
    {

        private static async Task<string> DescribeImageAsync(byte[] imageBytesArray)
        {

            using (var httpClient = new HttpClient())
            {

                var url = Environment.GetEnvironmentVariable("COMPUTER_VISION_URL");
                var suffix = Environment.GetEnvironmentVariable("COMPUTER_VISION_URL_SUFFIX");
                var key = Environment.GetEnvironmentVariable("Ocp-Apim-Subscription-Key");


                httpClient.BaseAddress = new Uri(url);
                httpClient.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", key);

                using (var byteArrayContent = new ByteArrayContent(imageBytesArray, 0,
                                                                  imageBytesArray.Length))
                {

                    byteArrayContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                    var httpResponse = await httpClient.PostAsync(suffix, byteArrayContent);
                    var responseString = await httpResponse.Content.ReadAsStringAsync();
                    if (string.IsNullOrEmpty(responseString) == true)
                        return null;

                    var analyzedModel = JsonConvert.DeserializeObject<AnalyzedModel>(responseString);
                    var captions = analyzedModel?.Description.Captions;
                    var describedString = captions?[0].Text;
                    Console.WriteLine(describedString);
                    return describedString;


                }

            }

        }


        [FunctionName("Describer")]
        public static async Task Run([BlobTrigger("easyblob/{blobName}.{blobExtension}",
                                        Connection = "AzureWebJobsStorage")]
                                        Stream imageBlob,
                                        string blobName,
                                        string blobExtension,
                                        [EventHub("workshopeventhub",
                                        Connection = "OCR_EVENTHUB_CONNECTION")]
                                        IAsyncCollector<EventData> imageEventCollector,
                                        [Table("easytable")] CloudTable cloudTable,
                                        ILogger log)
        {


            byte[] buffer = new byte[imageBlob.Length];
            byte[] imageBytesArray = null;
            using (MemoryStream ms = new MemoryStream())
            {
                int read;
                while ((read = imageBlob.Read(buffer, 0, buffer.Length)) > 0)               
                    ms.Write(buffer, 0, read);

                imageBytesArray = ms.ToArray();

            }

            var describedString = await DescribeImageAsync(imageBytesArray);
            var eventData = new EventData(Encoding.UTF8.GetBytes(describedString));
            await imageEventCollector.AddAsync(eventData);

            var item = new Item()
            {

                PartitionKey = "Describe",
                RowKey = Guid.NewGuid().ToString(),
                BlobName = $"{blobName}.{blobExtension}",
                DescribedText = describedString


            };

            var operation = TableOperation.Insert(item);
            await cloudTable.ExecuteAsync(operation);


        }
    }

    public class Item : TableEntity
    {
        public string BlobName      { get; set; }
        public string DescribedText  { get; set; }
    }
}
