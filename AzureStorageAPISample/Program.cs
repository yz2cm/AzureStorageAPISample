using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Web;

namespace AzureStorageAPISample
{
    class Program
    {
        static async Task<HttpResponseMessage> listBlobsAzureStorageAsync(HttpClient client, string uri, string storageAccount, string containerName, string accessKey)
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("r"));
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-version", "2017-07-29");

            var headers = client.DefaultRequestHeaders.Select(x => new KeyValuePair<string, string>(x.Key, x.Value.FirstOrDefault() ?? string.Empty)).ToList();
            string query = new Uri(uri).GetComponents(UriComponents.Query, UriFormat.UriEscaped);
            string stringToSign = buildStringToSign("GET", headers, storageAccount, containerName, string.Empty, query);
            Console.WriteLine("******** StringToSign ********");
            Console.WriteLine(stringToSign);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string signature = buildSignature(stringToSign, accessKey);
            Console.WriteLine("******** Signature ********");
            Console.WriteLine(signature);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string authValue = buildAuthorizationHeaderValue(storageAccount, signature);
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", authValue);

            Console.WriteLine("**** Request header ****");
            foreach (var header in client.DefaultRequestHeaders)
            {
                Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
            }
            Console.WriteLine("*************************");

            return await client.GetAsync(uri);

        }
        static async Task<HttpResponseMessage> deleteBlobAzureStorageAsync(HttpClient client, string uri, string storageAccount, string containerName, string accessKey, string blobObjectName)
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("r"));
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-version", "2017-07-29");

            var headers = client.DefaultRequestHeaders.Select(x => new KeyValuePair<string, string>(x.Key, x.Value.FirstOrDefault() ?? string.Empty)).ToList();
            string stringToSign = buildStringToSign("DELETE", headers, storageAccount, containerName, blobObjectName, string.Empty);
            Console.WriteLine("******** StringToSign ********");
            Console.WriteLine(stringToSign);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string signature = buildSignature(stringToSign, accessKey);
            Console.WriteLine("******** Signature ********");
            Console.WriteLine(signature);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string authValue = buildAuthorizationHeaderValue(storageAccount, signature);
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", authValue);

            Console.WriteLine("**** Request header ****");
            foreach (var header in client.DefaultRequestHeaders)
            {
                Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
            }
            Console.WriteLine("*************************");

            return await client.DeleteAsync(new Uri(uri));
        }
        static async Task<HttpResponseMessage> getBlobAzureStorageAsync(HttpClient client, string uri, string storageAccount, string containerName, string accessKey, string blobObjectName)
        {
            client.DefaultRequestHeaders.Clear();
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-date", DateTime.UtcNow.ToString("r"));
            client.DefaultRequestHeaders.TryAddWithoutValidation("x-ms-version", "2017-07-29");

            var headers = client.DefaultRequestHeaders.Select(x => new KeyValuePair<string, string>(x.Key, x.Value.FirstOrDefault() ?? string.Empty)).ToList();
            string stringToSign = buildStringToSign("GET", headers, storageAccount, containerName, blobObjectName, string.Empty);
            Console.WriteLine("******** StringToSign ********");
            Console.WriteLine(stringToSign);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string signature = buildSignature(stringToSign, accessKey);
            Console.WriteLine("******** Signature ********");
            Console.WriteLine(signature);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string authValue = buildAuthorizationHeaderValue(storageAccount, signature);
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", authValue);

            Console.WriteLine("**** Request header ****");
            foreach (var header in client.DefaultRequestHeaders)
            {
                Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
            }
            Console.WriteLine("*************************");

            return await client.GetAsync(new Uri(uri));
        }
        static async Task<HttpResponseMessage> putBlobAzureStorageAsync(HttpClient client, string uri, string storageAccount, string containerName, string accessKey, string blobObjectName, byte[] blobContent)
        {
            client.DefaultRequestHeaders.Clear();

            HttpContent httpContent = new ByteArrayContent(blobContent);
            DateTime xMsDate_dateTime = DateTime.UtcNow;
            httpContent.Headers.TryAddWithoutValidation("x-ms-date", xMsDate_dateTime.ToString("r"));
            httpContent.Headers.TryAddWithoutValidation("x-ms-version", "2017-07-29");
            httpContent.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            httpContent.Headers.ContentLength = blobContent.Length;

            var headers = httpContent.Headers.Select(x => new KeyValuePair<string, string>(x.Key, x.Value.FirstOrDefault() ?? string.Empty)).ToList();

            string stringToSign = buildStringToSign("PUT", headers, storageAccount, containerName, blobObjectName, string.Empty);
            Console.WriteLine("******** StringToSign ********");
            Console.WriteLine(stringToSign);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string signature = buildSignature(stringToSign, accessKey);
            Console.WriteLine("******** Signature ********");
            Console.WriteLine(signature);
            Console.WriteLine("***************************");
            Console.WriteLine();

            string authValue = buildAuthorizationHeaderValue(storageAccount, signature);
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", authValue);

            Console.WriteLine("**** Request header ****");
            foreach (var header in httpContent.Headers)
            {
                Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
            }
            Console.WriteLine("*************************");

            return await client.PutAsync(uri, httpContent);
        }
        static void printHttpResponse(HttpResponseMessage response)
        {
            Console.WriteLine();
            Console.WriteLine("**** Response header ****");
            foreach (var header in response.Content.Headers)
            {
                Console.WriteLine($"{header.Key}: {string.Join(",", header.Value)}");
            }
            Console.WriteLine("*************************");
            Console.WriteLine($"[DEBUG] Status code:   {response.StatusCode.ToString()}");
            Console.WriteLine($"[DEBUG] Reason phrase: {response.ReasonPhrase}");
            Console.WriteLine();

            Console.WriteLine("******** Response Content ********");
            if(response.Content.Headers.ContentLength > 0)
            {
                var responseMessage = response.Content.ReadAsStringAsync().Result;
                Console.WriteLine(responseMessage);
            }
            else
            {
                Console.WriteLine("(None)");
            }
            Console.WriteLine("**********************************");

        }
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("AzureStorageAPISample <storage-account> <container-name>");
                return;
            }
            string storageAccount = args[0];
            string containerName = args[1];
            string accessKey = Environment.GetEnvironmentVariable("AZURE_STORAGE_ACCESS_KEY");

            string blobObjectName = "sample.txt";
            var blobObjectContent = Encoding.UTF8.GetBytes("hoge");

            string containerUri = @"http://" + storageAccount + ".blob.core.windows.net/" + containerName;

            using (var handler = new HttpClientHandler())
            {
                handler.UseProxy = false;
                using (var client = new HttpClient(handler))
                {
                    client.MaxResponseContentBufferSize = int.MaxValue;
                    // ============================================================
                    //  Put Blob
                    // ============================================================
                    using (var response = putBlobAzureStorageAsync(client, containerUri + "/" + blobObjectName, storageAccount, containerName, accessKey, blobObjectName, blobObjectContent).Result)
                    {
                        printHttpResponse(response);

                        string actualMD5Value = computeMD5InBase64String(blobObjectContent);
                        var contentMD5Value = response.Content.Headers.GetValues("Content-MD5").FirstOrDefault() ?? string.Empty;

                        if (actualMD5Value == contentMD5Value)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"MD5 hash matched. (actual:{actualMD5Value} Content-MD5:{contentMD5Value})");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"MD5 hash unmatched. (request:{actualMD5Value} response:{contentMD5Value})");
                            Console.ResetColor();
                        }
                    }

                    // ============================================================
                    //  Get Blob
                    // ============================================================
                    using (var response = getBlobAzureStorageAsync(client, containerUri + "/" + blobObjectName, storageAccount, containerName, accessKey, blobObjectName).Result)
                    {
                        printHttpResponse(response);
                        var contentMD5Value = response.Content.Headers.GetValues("Content-MD5").FirstOrDefault() ?? string.Empty;
                        var blobContent = response.Content.ReadAsByteArrayAsync().Result;
                        string actualMD5Value = computeMD5InBase64String(blobContent);

                        if (actualMD5Value == contentMD5Value)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"MD5 hash matched. (actual:{actualMD5Value} Content-MD5:{contentMD5Value})");
                            Console.ResetColor();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"MD5 hash unmatched. (request:{actualMD5Value} response:{contentMD5Value})");
                            Console.ResetColor();
                        }
                    }
                    // ============================================================
                    //  List Blobs
                    // ============================================================
                    using (var response = listBlobsAzureStorageAsync(client, containerUri + "?restype=container&comp=list", storageAccount, containerName, accessKey).Result)
                    {
                        printHttpResponse(response);
                    }

                    // ============================================================
                    //  Delete Blob
                    // ============================================================
                    using (var response = deleteBlobAzureStorageAsync(client, containerUri + "/" + blobObjectName, storageAccount, containerName, accessKey, blobObjectName).Result)
                    {
                        printHttpResponse(response);
                    }
                }
            }

            Console.ReadKey();
        }

        static string buildAuthorizationHeaderValue(string storageAccount, string signature)
        {
            return $"SharedKey {storageAccount}:{signature}";
        }
        static string buildStringToSign(string verb, IReadOnlyList<KeyValuePair<string, string>> headers, string storageAccount, string containerName, string blobObjectName, string querySection)
        {
            var stringToSign = new StringBuilder();
            stringToSign.Append(verb).Append("\n"); // VERB
            stringToSign.Append("\n");    // Content-Encoding
            stringToSign.Append("\n");    // Content-Language
            stringToSign.Append((headers.FirstOrDefault(x => x.Key.ToLower() == "content-length").Value ?? string.Empty) + ("\n")); // Content-Length
            stringToSign.Append("\n");    // Content-MD5
            stringToSign.Append("\n");    // Content-Type
            stringToSign.Append((headers.FirstOrDefault(x => x.Key.ToLower() == "date").Value ?? string.Empty) + ("\n"));    // Date
            stringToSign.Append("\n");    // If-Modified-Since
            stringToSign.Append("\n");    // If-Match
            stringToSign.Append("\n");    // If-Non-Match
            stringToSign.Append("\n");    // If-Unmodified-Since
            stringToSign.Append("\n");    // Range

            // canonicalized headers
            foreach(var header in headers.Where(x => x.Key.StartsWith("x-ms-")).OrderBy(x => x.Key.ToLower()))
            {
                stringToSign.Append(header.Key.ToLower());
                stringToSign.Append(":");
                stringToSign.Append(header.Value ?? string.Empty);
                stringToSign.Append("\n");
            }
            // canonicalized resource
            stringToSign.AppendFormat("/{0}/{1}", storageAccount, containerName);
            if(blobObjectName.Length > 0)
            {
                stringToSign.AppendFormat("/{0}", blobObjectName);
            }

            if(querySection.Length > 0)
            {
                var kvs = new List<KeyValuePair<string, string>>();
                foreach (var query in querySection.Split('&').OrderBy(x => x))
                {
                    var splitted = query.Split('=');
                    string key = splitted[0].ToLower();
                    string value = splitted[1];
                    kvs.Add(new KeyValuePair<string, string>(key, value));
                }

                kvs = kvs.OrderBy(x => x.Key).ToList();
                foreach(var kv in kvs)
                {
                    stringToSign.AppendFormat("\n{0}:{1}", kv.Key, kv.Value);
                }
            }

            return stringToSign.ToString();
        }
        static string buildSignature(string stringToSign, string accessKey)
        {
            var stringToSign_bytes =  Encoding.UTF8.GetBytes(stringToSign.ToString());
            var accessKey_bytes = Convert.FromBase64String(accessKey);
            var stringToSign_hashed = HMAC_SHA256(stringToSign_bytes, accessKey_bytes);
            var stringToSign_base64 = Convert.ToBase64String(stringToSign_hashed);

            return stringToSign_base64.ToString();
        }
        static string computeMD5InBase64String(byte[] content)
        {
            using (var md5 = MD5.Create())
            {
                var md5Value = md5.ComputeHash(content);
                return Convert.ToBase64String(md5Value);
            }
        }
        static byte[] HMAC_SHA256(byte[] data, byte[] key)
        {
            using (var kha = KeyedHashAlgorithm.Create("HmacSHA256"))
            {
                kha.Key = key;
                return kha.ComputeHash(data);
            }
        }
    }
}
