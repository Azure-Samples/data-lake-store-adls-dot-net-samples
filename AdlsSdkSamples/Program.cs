using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;

namespace AdlsSdkSamples
{
    class Program
    {
        private static string clientId = "FILL-IN-HERE";            // Also called application id in portal
        private static string clientSecret = "FILL-IN-HERE";
        private static string domain = "FILL-IN-HERE";              // Also called tenant Id
        private static string clientAccountPath = "FILL-IN-HERE";
        private static string UserId = "FILL-IN-HERE";
        private static string Psswd = "FILL-IN-HERE";
        private static string localFileTransferPath = @"C:\Data"; // Used for bulk transfer
        private static string remoteFileTransferPath = @"/Data";  // Used for bulk transfer
        public static void Main(string[] args)
        {
            try
            {
                // Acquire token and create client using user id and password
                ServiceClientCredentials clientCreds1 = UserTokenProvider.LoginSilentAsync(clientId, domain, UserId, Psswd).GetAwaiter().GetResult();
                AdlsClient client1 = AdlsClient.CreateClient(clientAccountPath, clientCreds1);

                // Acquire token and create client using client secret and client id
                var creds = new ClientCredential(clientId, clientSecret);
                ServiceClientCredentials clientCreds2 = ApplicationTokenProvider.LoginSilentAsync(domain, creds).GetAwaiter().GetResult();
                AdlsClient client2 = AdlsClient.CreateClient(clientAccountPath, clientCreds2);

                // Perform write with flush and read with seek
                PerformWriteFlushReadSeek(client2);

                // Concatenate two files
                PerformConcat(client2);

                // Get Content summary using async operations
                GetContentSummaryAsync(client2).GetAwaiter().GetResult();

                // Bulk upload and download
                RunFileTransfer(client2);
                // Illustrate token refresh
                TestTokenRefresh(client2);
            }
            catch (AdlsException e)
            {
                PrintAdlsException(e);
            }
            Console.WriteLine("Done. Press ENTER to continue ...");
            Console.ReadLine();
        }
        private static void PerformWriteFlushReadSeek(AdlsClient client)
        {
            // Illustrate stream operations of Adls

            string fileName = "/Test/dir1/testFilename1.txt";

            // Create a file. It creates the parent directories /Test/dir1
            using (var createStream = client.CreateFile(fileName, IfExists.Overwrite))
            {
                // Write byte-array to stream
                byte[] writeData = Encoding.UTF8.GetBytes("This is the first line.\n");
                createStream.Write(writeData, 0, writeData.Length);
                // Flush the data in buffer to server
                createStream.Flush();
                writeData = Encoding.UTF8.GetBytes("This is the second line.\n");
                createStream.Write(writeData, 0, writeData.Length);
            } // Closing the stream flushes remaining data

            // Append to the file
            using (var appendStream = client.GetAppendStream(fileName))
            {
                byte[] writeData = Encoding.UTF8.GetBytes("This is the third line.\n");
                appendStream.Write(writeData, 0, writeData.Length);
                writeData = Encoding.UTF8.GetBytes("This is the fourth line.\n");
                appendStream.Write(writeData, 0, writeData.Length);
            }

            // Read the file
            using (var readStream = client.GetReadStream(fileName))
            {
                byte[] readData = new byte[1024];

                // Seek to offset 24
                readStream.Seek(24, SeekOrigin.Current);

                // Read 40 bytes at this offset
                int readBytes = readStream.Read(readData, 0, 40);
                Console.WriteLine("Read output of 40 bytes from offset 25: " + Encoding.UTF8.GetString(readData, 0, readBytes));

            }
        }
        private static void PerformConcat(AdlsClient client)
        {
            // Create two files
            string file1 = "/dir1/testConcat1.txt";
            string file2 = "/dir1/testConcat2.txt";
            string destFile = "/dir1/testConcat.txt";
            using (var createStream = new StreamWriter(client.CreateFile(file1, IfExists.Overwrite)))
            {
                createStream.WriteLine("This is the first file.");
            }
            using (var createStream = new StreamWriter(client.CreateFile(file2, IfExists.Overwrite)))
            {
                createStream.WriteLine("This is the second file.");
            }

            // Concatenate the files
            List<string> list = new List<string> { file1, file2 };
            client.ConcatenateFiles(destFile, list);

            // Print the resultant combined file
            using (var readStream = new StreamReader(client.GetReadStream(destFile)))
            {
                string line;
                while ((line = readStream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
        }
        private static void TestTokenRefresh(AdlsClient client)
        {
            string path = "/Test/TokenRefresh.txt";

            // Create file
            using (var ostreamWriter = new StreamWriter(client.CreateFile(path, IfExists.Overwrite, "")))
            {
                ostreamWriter.WriteLine("This is first line.");
            }

            // Wait 65 mins
            Console.WriteLine("Wait for 65 minutes to illustrate token refresh");
            Thread.Sleep(65 * 60 * 1000);

            // Read file - this still works, because token is internally refreshed
            using (var readStream = new StreamReader(client.GetReadStream(path)))
            {
                string line;
                while ((line = readStream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }

        }

        private static async Task GetContentSummaryAsync(AdlsClient client)
        {
            await client.DeleteRecursiveAsync("/a");

            Console.WriteLine("Build a sample hierarchical directory tree");
            await CreateDirRecursiveAsync(client, "/a", 3, 3, 1);

            Console.WriteLine("Retrieve the content summary");
            ContentSummary summary = client.GetContentSummary("/a");
            Console.WriteLine($"Directory Count: {summary.DirectoryCount}");
            Console.WriteLine($"File Count: {summary.FileCount}");
            Console.WriteLine($"Total Size: {summary.SpaceConsumed}");
        }

        // Create a sample hierarchical directory tree using async operations
        private static async Task CreateDirRecursiveAsync(AdlsClient client, string path, int recursLevel, int noDirEntries, int noFileEntries)
        {
            await client.CreateDirectoryAsync(path);
            string[] str = path.Split('/');
            char nextLevel = str[str.Length - 1][0];
            nextLevel++;
            for (int i = 0; i < noFileEntries; i++)
            {
                using (var ostream = new StreamWriter(await client.CreateFileAsync(path + "/" + nextLevel + i + "File.txt", IfExists.Overwrite, "")))
                {
                    await ostream.WriteLineAsync("This is first line.");
                }
            }
            if (recursLevel == 0)
            {
                return;
            }

            string newPath = path + "/";
            for (int i = 0; i < noDirEntries; i++)
            {
                await CreateDirRecursiveAsync(client, newPath + nextLevel + i, recursLevel - 1, noDirEntries, noFileEntries);
            }
        }

        // Bulk upload and download
        private static void RunFileTransfer(AdlsClient client)
        {
            Directory.CreateDirectory(localFileTransferPath);
            var fileName = localFileTransferPath + @"\testUploadFile.txt";
            Console.WriteLine("Creating the test file to upload:");
            using (var stream = new StreamWriter(new FileStream(fileName, FileMode.Create, FileAccess.ReadWrite)))
            {
                stream.WriteLine("Hello I am the first line of upload.");
                stream.WriteLine("Hello I am the second line of upload.");
            }
            var destFile = remoteFileTransferPath + "/testremoteUploadFile.txt";
            Console.WriteLine("Upload of the file:");
            client.BulkUpload(fileName, destFile, 1); // Source and destination could also be directories
            using (var readStream = new StreamReader(client.GetReadStream(destFile)))
            {
                string line;
                while ((line = readStream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
            var localDestFile = localFileTransferPath + @"\testlocalDownloadFile.txt";
            Console.WriteLine("Download of the uploaded file:");
            client.BulkDownload(destFile, localDestFile, 1); // Source and destination could also be directories
            using (var stream = new StreamReader(File.OpenRead(localDestFile)))
            {
                string line;
                while ((line = stream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
            Directory.Delete(localFileTransferPath,true);
            client.DeleteRecursive(remoteFileTransferPath);
        }

        private static void PrintAdlsException(AdlsException exp)
        {
            Console.WriteLine("ADLException");
            Console.WriteLine($"   Http Status: {exp.HttpStatus}");
            Console.WriteLine($"   Http Message: {exp.HttpMessage}");
            Console.WriteLine($"   Remote Exception Name: {exp.RemoteExceptionName}");
            Console.WriteLine($"   Server Trace Id: {exp.TraceId}");
            Console.WriteLine($"   Exception Message: {exp.Message}");
            Console.WriteLine($"   Exception Stack Trace: {exp.StackTrace}");
            Console.WriteLine();
        }
    }
}
