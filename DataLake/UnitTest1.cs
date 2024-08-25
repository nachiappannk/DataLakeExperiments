using Azure.Storage.Files.DataLake;
using Azure.Storage;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage.Blobs;
using Azure.Core;
using Azure.Storage.Blobs.Models;
using System;

namespace DataLake
{
    public class Tests
    {
        private const string accountName = "nachistorage";
        private const string accountKey = "";//TBD
        private const string containerName = "nachi";
        private const string localFilePath = @"C:\Users\nachi\Downloads\Resume_Nachiappan.docx";

        [Test]
        public async Task CreateARemoteFileUsingBlobSdk()
        {
            var containerClient = GetBlobContainerClient();

            List<string> remoteFileNames = new List<string>()
            {
                GetRemoteFileName(localFilePath),
                GetRemoteFileName(localFilePath, 0, "tempDirectory"),
                GetRemoteFileName(localFilePath, 2, "tempDirectory"),
                GetRemoteFileName(localFilePath, 0, "main"),
                GetRemoteFileName(localFilePath, 2, "main"),
            };

            var accessTiers = new List<AccessTier?>()
            {
                AccessTier.Hot,
                AccessTier.Cold,
                AccessTier.Cool,
                AccessTier.Archive,
                null,
            };

            for (int i = 0; i < remoteFileNames.Count; i++)
            {
                var remoteFileName = remoteFileNames[i];
                var accessTier = accessTiers[i];
                BlobClient blobClient = containerClient.GetBlobClient(remoteFileName);
                using FileStream localFileStream = File.OpenRead(localFilePath);

                if (accessTier == null)
                {
                    await blobClient.UploadAsync(localFileStream);
                }
                else
                {
                    var blobUploadOptions = new BlobUploadOptions
                    {
                        AccessTier = accessTier,
                    };
                    await blobClient.UploadAsync(localFileStream, blobUploadOptions);
                }

                localFileStream.Close();
                Console.WriteLine("File created");
            }
        }


        [Test]
        public async Task DeleteARemoteFileUsingBlobSdk()
        {
            var containerClient = GetBlobContainerClient();

            List<string> remoteFileNames = new List<string>()
            {
                GetRemoteFileName(localFilePath),
                GetRemoteFileName(localFilePath, 0, "tempDirectory"),
                GetRemoteFileName(localFilePath, 0, "main"),
            };


            for (int i = 0; i < remoteFileNames.Count; i++)
            {
                var remoteFileName = remoteFileNames[i];
                BlobClient blobClient = containerClient.GetBlobClient(remoteFileName);
                await blobClient.DeleteAsync();
            }
        }

        [Test]
        public async Task CreateRemoteFileUsingDataLake()
        {
            List<string> remoteFileNames = new List<string>()
            {
                GetRemoteFileName(localFilePath),
                GetRemoteFileName(localFilePath, 0, "tempDirectory1"),
                GetRemoteFileName(localFilePath, 2, "tempDirectory1"),
                GetRemoteFileName(localFilePath, 0, "main1"),
                GetRemoteFileName(localFilePath, 2, "main1"),
            };
            for (int i = 0; i < remoteFileNames.Count; i++)
            {
                var fileSystemClient = DataLakeFileSystemClient();
                var fileClient = fileSystemClient.GetFileClient(remoteFileNames[i]);
                using FileStream fileStream = File.OpenRead(localFilePath);
                await fileClient.CreateAsync();
                await fileClient.AppendAsync(fileStream, 0);
                await fileClient.FlushAsync(fileStream.Length);
            }
        }

        [Test]
        public async Task DeleteRemoteFileUsingDataLake()
        {
            List<string> remoteFileNames = new List<string>()
            {
                GetRemoteFileName(localFilePath),
                GetRemoteFileName(localFilePath, 0, "tempDirectory1"),
                GetRemoteFileName(localFilePath, 0, "main1"),
            };
            for (int i = 0; i < remoteFileNames.Count; i++)
            {
                var fileSystemClient = DataLakeFileSystemClient();
                var fileClient = fileSystemClient.GetFileClient(remoteFileNames[i]);
                await fileClient.DeleteIfExistsAsync();
            }
        }

        [TestCase("main")]
        [TestCase("")]
        //[TestCase("notthere")] will give error as expected
        public async Task ListingFilesUsingDataLake(string directoryName)
        {
            var fileSystemClient = DataLakeFileSystemClient();
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directoryName);
            await foreach (var item in directoryClient.GetPathsAsync(recursive: true))
            {
                Console.WriteLine($"{(item.IsDirectory ?? false ? "Directory" : "File")}: {item.Name}");
            }
        }

        [Test]
        public async Task DeleteDirectoryUsingDataLakeClient()
        {
            var fileSystemClient = DataLakeFileSystemClient();
            await fileSystemClient.DeleteDirectoryAsync("main");
        }

        [Test]
        public async Task RenameDirectoryUsingDataLakeClient()
        {
            var fileSystemClient = DataLakeFileSystemClient();
            var directoryClient = fileSystemClient.GetDirectoryClient("main");
            await directoryClient.RenameAsync("main1234");
        }

        [Test]
        public async Task CreateDirectoryUsingDataLakeClientAtRoot()
        {
            var fileSystemClient = DataLakeFileSystemClient();
            await fileSystemClient.CreateDirectoryAsync("mainNew");
        }


        [Test]
        public async Task CreateDirectoryUsingDataLakeClient()
        {
            var fileSystemClient = DataLakeFileSystemClient();
            var directoryClient = fileSystemClient.GetDirectoryClient("main");
            await directoryClient.CreateSubDirectoryAsync("MainNew");
        }

        [Test]
        public async Task MoveAccessTierUsingBlobClient()
        {
            var containerClient = GetBlobContainerClient();
            var blobClient = containerClient.GetBlobClient(GetRemoteFileName(localFilePath));
            await blobClient.SetAccessTierAsync(AccessTier.Cold);
        }

        private static DataLakeFileSystemClient DataLakeFileSystemClient()
        {
            string serviceUri = $"https://{accountName}.dfs.core.windows.net";
            DataLakeServiceClient serviceClient = new DataLakeServiceClient(new Uri(serviceUri), new StorageSharedKeyCredential(accountName, accountKey));
            DataLakeFileSystemClient fileSystemClient = serviceClient.GetFileSystemClient(containerName);
            return fileSystemClient;
        }

        static async Task UploadFileAsync(DataLakeFileClient fileClient, string localFilePath)
        {
            using FileStream fileStream = File.OpenRead(localFilePath);
            await fileClient.CreateAsync();
            await fileClient.AppendAsync(fileStream, 0);
            await fileClient.FlushAsync(fileStream.Length);
            Console.WriteLine("File uploaded successfully.");
        }

        static async Task ListFilesInDirectoryAsync(DataLakeFileSystemClient fileSystemClient, string directoryPath)
        {
            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
            await foreach (var item in directoryClient.GetPathsAsync(recursive: true))
            {
                Console.WriteLine($"{(item.IsDirectory ?? false ? "Directory" : "File")}: {item.Name}");
            }
        }

        private static BlobContainerClient GetBlobContainerClient()
        {
            var sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
            string blobUri = "https://" + accountName + ".blob.core.windows.net";
            var blobServiceClient = new BlobServiceClient(new Uri(blobUri), sharedKeyCredential);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            return containerClient;
        }

        private string GetRemoteFileName(string file, int depth = 0, params string[] folders)
        {
            var fileName = Path.GetFileName(file);

            var remoteFileName = string.Empty;

            foreach (var folder in folders)
            {
                remoteFileName = remoteFileName + folder + "/";
            }
            for (int i = 0; i < depth; i++)
            {
                var random = GetRandomString();
                remoteFileName = remoteFileName + random + "/";
            }

            return remoteFileName + fileName;

        }

        private string GetRandomString()
        {
            int length = 10;
            Random random = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

    }   
}