using System;
using System.IO;

namespace DatabaseUpdater
{
    public class CommandLogger : ICommandLogger
    {
        private readonly object _syncObj = new object();
        private readonly object _syncObjConsole = new object();

        public FileInfo LogFile { get; set; }
        public bool Verbose { get; set; }

        public void IfException(Action action, string text, bool throwForward = true)
        {
            try
            {
                action();
            }
            catch
            {
                LogLine(text);
                if (throwForward)
                    throw;
            }
        }

        public void InfoLine(string text) => Console.WriteLine(text);

        public void NewLine() => Console.WriteLine();

        public void LogLine(string text)
        {
            if (Verbose)
            {
                lock (_syncObjConsole)
                {
                    var colorStore = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.DarkCyan;
                    Console.WriteLine(text);
                    Console.ForegroundColor = colorStore;
                }
                
            }
            Log(text);
        }

        public void LogException(Exception exception)
        {
            lock (_syncObjConsole)
            {
                var colorStore = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.WriteLine(Verbose ? exception.ToString() : exception.Message);
                Console.ForegroundColor = colorStore;
            }
            Log(exception.ToString());
        }

        private void Log(string text)
        {
            lock (_syncObj)
            {
                if (LogFile != null)
                {
                    try
                    {
                        File.AppendAllText(LogFile.FullName, text);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }
        }

        public void Dispose()
        {
        }
    }
}