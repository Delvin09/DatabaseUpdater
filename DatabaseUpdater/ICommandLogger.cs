using System;
using System.IO;

namespace DatabaseUpdater
{
    public interface ICommandLogger : IDisposable
    {
        void InfoLine(string text);
        void NewLine();
        void LogException(Exception exception);
        void LogLine(string text);
        FileInfo LogFile { get; set; }
        bool Verbose { get; set; }

        void IfException(Action action, string text, bool throwForward = false);
    }
}