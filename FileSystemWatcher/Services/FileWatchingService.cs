using CsFileSystemWatcher.Models;
using System;
using System.IO;
using System.Text.Json;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace CsFileSystemWatcher.Services
{
    class FileWatchingService
    {
        private readonly KafkaService _producer;
        

        public FileWatchingService(KafkaService producer)
        {
            _producer = producer;

        }
        public void Watch()
        {
            using var watcher = new FileSystemWatcher(@".\..\alert-simulator\alerts");

            

            watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.Security
                                 | NotifyFilters.Size;

            watcher.Changed += OnChanged;
            watcher.Created += OnCreated;
            watcher.Deleted += OnDeleted;
            watcher.Renamed += OnRenamed;
            watcher.Error += OnError;

            watcher.Filter = "";
            watcher.IncludeSubdirectories = true;
            watcher.EnableRaisingEvents = true;

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }

        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType != WatcherChangeTypes.Changed)
            {
                return;
            }
            Console.WriteLine($"Changed: {e.FullPath}");
        }

        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            if (e.FullPath.Split('\\').Last() == "alert.ready")
            {
                var filePath = e.FullPath.Split('\\');
                filePath[filePath.Length-1] = "alert.json";
                

                var mesegeString = File.ReadAllText(string.Join("\\", filePath));
                try
                {
                    var rawMesege = JsonSerializer.Deserialize<RawMesege>(mesegeString);
                    if (rawMesege == null)
                    {
                        _producer.Log("ERROR", $"failed to Serialise mesege");
                        return;
                    }

                    _producer.Produce("raw-data", rawMesege);
                    _producer.Log("INFO", $"sent mesege {rawMesege.AlertId} to topic raw-data");
                }
                catch (Exception ex)
                {
                    _producer.Log("WORNING", $"failed to send mesege");
                }
            }

            string value = $"Created: {e.FullPath}";
            
        }

        private static void OnDeleted(object sender, FileSystemEventArgs e) =>
            Console.WriteLine($"Deleted: {e.FullPath}");

        private static void OnRenamed(object sender, RenamedEventArgs e)
        {
            Console.WriteLine($"Renamed:");
            Console.WriteLine($"    Old: {e.OldFullPath}");
            Console.WriteLine($"    New: {e.FullPath}");
        }

        private static void OnError(object sender, ErrorEventArgs e) =>
            PrintException(e.GetException());

        private static void PrintException(Exception? ex)
        {
            if (ex != null)
            {
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine("Stacktrace:");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine();
                PrintException(ex.InnerException);
            }
        }
    }
}