﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
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
            string fileName = "/Test/dir1/testFilename1.txt";

            DataLakeFileClient file = client.GetFileClient(fileName);

            // Create the file
            Stream stream = BinaryData.FromString("This is the first line.\nThis is the second line.\n").ToStream();
            long length = stream.Length;
            file.Upload(stream, true);

            // Append to the file
            stream = BinaryData.FromString("This is the third line.\nThis is the fourth line.\n").ToStream();
            file.Append(stream, length);
            file.Flush(length + stream.Length);

            // Read the file
            using (var readStream = file.OpenRead())
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
            file.Upload(
                 BinaryData.FromString("This is the first file.").ToStream(), overwrite: true);

            // Wait 1 mins
            Console.WriteLine("The token is refreshing...");
            Thread.Sleep(60 * 1000);

            // Read file - this still works, because token is internally refreshed
            using (var readStream = new StreamReader(file.OpenRead()))
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
            MemoryStream fileContentDown = new MemoryStream();
            fileContents.Value.Content.CopyTo(fileContentDown);
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
                fileContentDown.CopyTo(stream);
            }
            fileContentDown.Close();
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

