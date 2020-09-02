using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace AdlsSdkSamples
{
    class Program
    {
        private static string clientId = "FILL-IN-HERE";            // Also called application id in portal
        private static string clientSecret = "FILL-IN-HERE";
        private static string domain = "FILL-IN-HERE";              // Also called tenant Id
        private static string serviceUri = "FILL-IN-HERE";
        private static string localFileTransferPath = @"C:\Data"; // Used for bulk transfer
        private static string remoteFileTransferPath = @"/Data";  // Used for bulk transfer
        public static void Main(string[] args)
        {
            // Create Client Secret Credential
            var creds = new ClientSecretCredential(domain, clientId, clientSecret);

            // Create data lake file service client object
            DataLakeServiceClient serviceClient = new DataLakeServiceClient(new Uri(serviceUri), creds);
            var name = "sample-filesystem" + Guid.NewGuid().ToString("n").Substring(0, 8);
            // Create data lake file system client object
            DataLakeFileSystemClient filesystemclient = serviceClient.GetFileSystemClient(name);

            filesystemclient.CreateIfNotExists();
            try { 
                // Perform write with flush and read with seek
                PerformWriteFlushReadSeek(filesystemclient);

                // Upload and download
                RunFileTransfer(filesystemclient);
                // Change Acl and get acl properties
                SetAclAndGetFileProperties(filesystemclient);
                // Illustrate token refresh
                TestTokenRefresh(filesystemclient);
            }
            finally
            {
                filesystemclient.Delete();
            }
            Console.WriteLine("Done. Press ENTER to continue ...");
            Console.ReadLine();
        }
        private static void PerformWriteFlushReadSeek(DataLakeFileSystemClient client)
        {
            long length;
            string fileName = "/Test/dir1/testFilename1.txt";

            DataLakeFileClient file = client.GetFileClient(fileName);

            // Create a file. It creates the parent directories /Test/dir1
            using (var stream = new MemoryStream())
            {
                // Write byte-array to stream
                byte[] writeData = Encoding.UTF8.GetBytes("This is the first line.\n");
                stream.Write(writeData, 0, writeData.Length);
                // Flush the data in buffer to server
                stream.Flush();
                writeData = Encoding.UTF8.GetBytes("This is the second line.\n");
                stream.Write(writeData, 0, writeData.Length);
                length = stream.Length;
                stream.Seek(0, SeekOrigin.Begin);
                file.Upload(stream, true);
            }// Closing the stream flushes remaining data

            // Append to the file
            using (var stream = new MemoryStream())
            {
                byte[] writeData = Encoding.UTF8.GetBytes("This is the third line.\n");
                stream.Write(writeData, 0, writeData.Length);
                writeData = Encoding.UTF8.GetBytes("This is the fourth line.\n");
                stream.Write(writeData, 0, writeData.Length);
                stream.Seek(0, SeekOrigin.Begin);
                file.Append(stream, length);
                file.Flush(length + stream.Length);
            }

            // Read the file
            Response<FileDownloadInfo> fileContents = file.Read();
            using (var readStream =fileContents.Value.Content)
            {
                byte[] readData = new byte[1024];
           
                // Read 40 bytes at this offset
                int readBytes = readStream.Read(readData, 25, 40);
                Console.WriteLine("Read output of 40 bytes from offset 25: " + Encoding.UTF8.GetString(readData, 0, readBytes));
            }
        }
       
        private static void TestTokenRefresh(DataLakeFileSystemClient client)
        {
            string path = "/Test/TokenRefresh.txt";
            DataLakeFileClient file = client.GetFileClient(path);
            // Create file
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("This is the first file.")))
            {
                file.Upload(stream, true);
            }

            // Wait 65 mins
            Console.WriteLine("Wait for 65 minutes to illustrate token refresh");
            Thread.Sleep(65 * 60 * 1000);

            // Read file - this still works, because token is internally refreshed
            Response<FileDownloadInfo> fileContents = file.Read();
            using (var readStream = new StreamReader(fileContents.Value.Content))
            {
                string line;
                while ((line = readStream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }

        }

        // Create a sample hierarchical directory tree using async operations
        private static async Task CreateDirRecursiveAsync(DataLakeFileSystemClient client, string path, int recursLevel, int noDirEntries, int noFileEntries)
        {
            await client.CreateDirectoryAsync(path);
            byte[] writeData = Encoding.UTF8.GetBytes("This is the first line.\n");
            string[] str = path.Split('/');
            char nextLevel = str[str.Length - 1][0];
            nextLevel++;
            for (int i = 0; i < noFileEntries; i++)
            {
                DataLakeFileClient file = client.GetFileClient(path + "/" + nextLevel + i + "File.txt");

                using (var stream = new MemoryStream(writeData))
                {
                    file.Upload(stream, true);
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
        private static void RunFileTransfer(DataLakeFileSystemClient client)
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
            DataLakeFileClient file = client.GetFileClient(destFile);
            Console.WriteLine("Upload of the file:");
            file.Upload(fileName); // Source and destination could also be directories
            Response<FileDownloadInfo> fileContents = file.Read();
            using (var readStream = new StreamReader(fileContents.Value.Content))
            {
                string line;
                while ((line = readStream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
            var localDestFile = localFileTransferPath + @"\testlocalDownloadFile.txt";
            Console.WriteLine("Download of the uploaded file:");
            using (FileStream stream = File.OpenWrite(localDestFile))
            {
                fileContents.Value.Content.CopyTo(stream);
            }
            using (var stream = new StreamReader(File.OpenRead(localDestFile)))
            {
                string line;
                while ((line = stream.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
            Directory.Delete(localFileTransferPath, true);
            client.DeleteDirectory(remoteFileTransferPath);
        }

        private static void SetAclAndGetFileProperties(DataLakeFileSystemClient client)
        {
            DataLakeFileClient fileClient = client.GetFileClient("sample.txt");
            fileClient.Create();

            // Set Access Control List
            IList<PathAccessControlItem> accessControlList
                = PathAccessControlExtensions.ParseAccessControlList("user::rwx,group::r--,mask::rwx,other::---");
            fileClient.SetAccessControlList(accessControlList);
            PathAccessControl accessControlResponse = fileClient.GetAccessControl();
            Console.WriteLine($"User: {accessControlResponse.Owner}");
            Console.WriteLine($"Group: {accessControlResponse.Group}");
            Console.WriteLine($"Permissions: {accessControlResponse.Permissions}");
        }
    }
}

