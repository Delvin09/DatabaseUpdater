using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DatabaseUpdater
{
    public class CommandProcessor : IDisposable
    {
        private struct OptionInfo
        {
            public Action<string> ProcessOption { get; set; }
            public bool IsRequired { get; set; }
        }

        private readonly Dictionary<SortedSet<string>, OptionInfo> AllOptions = new Dictionary<SortedSet<string>, OptionInfo>();

        private readonly IArguments _arguments;
        private readonly ICommandLogger _logger;

        private bool _isHelpNeeded;

        private SqlConnectionStringBuilder _connectionString;
        private string[] _databases;
        private IList<FileInfo> _files;
        private bool _isParallel = true;

        public CommandProcessor()
        {
            var helpSet = new SortedSet<string> { "help", "h", "?" };
            AllOptions[helpSet] = new OptionInfo { IsRequired = false, ProcessOption = s => _isHelpNeeded = true };

            var connectionStringSet = new SortedSet<string> { "connectionString", "cs" };
            AllOptions[connectionStringSet] = new OptionInfo { IsRequired = true, ProcessOption = s => _logger.IfException(() => _connectionString = new SqlConnectionStringBuilder(s), "Invalid connection string") };

            var databaseSet = new SortedSet<string> { "database", "db" };
            AllOptions[databaseSet] = new OptionInfo { IsRequired = true, ProcessOption = s => _databases = s.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries) };

            var scriptSet = new SortedSet<string> { "script", "s" };
            AllOptions[scriptSet] = new OptionInfo { IsRequired = false, ProcessOption = s => _logger.IfException(() => _files = s.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries).Select(script => new FileInfo(script)).ToList(), "Invalid file") };

            var logSet = new SortedSet<string> { "log", "l" };
            AllOptions[logSet] = new OptionInfo { IsRequired = false, ProcessOption = s => _logger.IfException(() => _logger.LogFile = new FileInfo(s), "Invalid log file") };

            var verboseSet = new SortedSet<string> { "verbose", "v" };
            AllOptions[verboseSet] = new OptionInfo { IsRequired = false, ProcessOption = s => _logger.Verbose = true };

            var parallelSet = new SortedSet<string> { "parallel", "p" };
            AllOptions[parallelSet] = new OptionInfo { IsRequired = false, ProcessOption = s => _logger.IfException(() => _isParallel = bool.Parse(s), "Invalid input.") };
        }

        public CommandProcessor(IArguments arguments, ICommandLogger logger) : this()
        {
            _arguments = arguments;
            _logger = logger;
        }

        private KeyValuePair<SortedSet<string>, OptionInfo> FindSet(string key)
        {
            return AllOptions.FirstOrDefault(o => o.Key.Contains(key, StringComparer.OrdinalIgnoreCase));
        }

        private void ValidateArguments()
        {
            _logger.LogLine("Start argument validation");
            var requiredOptions = AllOptions.Where(e => e.Value.IsRequired).ToList();
            var foundedSet = new HashSet<KeyValuePair<SortedSet<string>, OptionInfo>>();
            var foudedArguments = new HashSet<KeyValuePair<string, string>>();
            foreach (var pair in requiredOptions)
            {
                if (foundedSet.Contains(pair))
                {
                    _logger.LogLine("Duplicate arguments!");
                    throw new InvalidOperationException();
                }

                var argument = _arguments.FirstOrDefault(a => pair.Key.Contains(a.Key, StringComparer.OrdinalIgnoreCase));
                if (argument.Value != null)
                {
                    pair.Value.ProcessOption(argument.Value);
                    foundedSet.Add(pair);
                    foudedArguments.Add(argument);
                }
            }

            if (requiredOptions.Count != foundedSet.Count)
            {
                throw new ArgumentException("Not all required options is set.");
            }

            foreach (var argument in _arguments.Except(foudedArguments))
            {
                var set = FindSet(argument.Key);
                if (foundedSet.Contains(set))
                {
                    _logger.LogLine("Duplicate arguments!");
                    throw new InvalidOperationException();
                }

                set.Value.ProcessOption(argument.Value);
                foundedSet.Add(set);
            }

            _connectionString.MultipleActiveResultSets = _isParallel;
        }

        public void GetHelp()
        {
            _logger.InfoLine("-help, -h, -? ----> This command. Shows help.");
            _logger.NewLine();
            _logger.InfoLine("-connectionString, -cs ----> connection string. [REQUIRED]");
            _logger.NewLine();
            _logger.InfoLine("-database, -db ----> database for process. [REQUIRED]");
            _logger.NewLine();
            _logger.InfoLine("-script, -s ----> execution scripts. if doesn't defined than just echo.");
            _logger.NewLine();
            _logger.InfoLine("-log, -l ----> log into file");
            _logger.NewLine();
            _logger.InfoLine("-verbose, -v ----> log info into screen");
            _logger.NewLine();
            _logger.InfoLine("-parallel, -p ----> parallel true/false. default - true");
            _logger.NewLine();
        }

        public void Process()
        {
            ValidateArguments();
            if (_isHelpNeeded)
            {
                GetHelp();
                return;
            }

            ProcessInternal();
        }

        private void ProcessInternal()
        {
            _logger.LogLine("Start processing with arguments: " + _arguments.ToString());
            var filesText = new Dictionary<string, IEnumerable<string>>();
            try
            {
                _logger.LogLine("Create connection...");
                using (var connection = new SqlConnection(_connectionString.ToString()))
                {
                    connection.Open();
                    _logger.LogLine("Connection opened.");

                    foreach (var database in GetDatabases(connection))
                        ProcessDatabase(database, connection, filesText);
                }
            }
            catch (Exception ex)
            {
                _logger.LogException(ex);
            }
        }

        private void ProcessDatabase(string database, SqlConnection connection, Dictionary<string, IEnumerable<string>> filesText)
        {
            _logger.LogLine($"Process database: {database}");
            connection.ChangeDatabase(database);

            using (var trans = connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                try
                {
                    if (_files == null)
                        ExecuteBatch("PRINT 1", connection, trans, null, database).Wait();
                    else
                    {
                        var bag = new ConcurrentBag<Task>();
                        foreach (var fileInfo in _files)
                        foreach (var batch in FillFileTextCollection(filesText, fileInfo))
                            bag.Add(ExecuteBatch(batch, connection, trans, fileInfo, database));
                        Task.WaitAll(bag.ToArray());
                    }

                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    _logger.LogException(ex);
                }
            }
        }

        private Task ExecuteBatch(string batch, SqlConnection connection, SqlTransaction trans, FileInfo fileInfo,
            string database)
        {
            _logger.LogLine("Process batch: ");
            _logger.LogLine(batch);
            using (var cmd = new SqlCommand(batch, connection, trans))
            {
                var sw = Stopwatch.StartNew();
                if (_isParallel)
                    return cmd.ExecuteNonQueryAsync().ContinueWith(i =>
                    {
                        sw.Stop();
                        _logger.LogLine(
                            $"Executed batch from: {fileInfo?.FullName} on database: {database}, affected rows {i.Result}, time: {sw.ElapsedMilliseconds}");
                    });
                else
                {
                    var i = cmd.ExecuteNonQuery();
                    sw.Stop();
                    _logger.LogLine(
                        $"Executed batch from: {fileInfo?.FullName} on database: {database}, affected rows {i}, time: {sw.ElapsedMilliseconds}");
                }

                return Task.FromResult(0);
            }
        }

        private IEnumerable<string> FillFileTextCollection(Dictionary<string, IEnumerable<string>> filesText, FileInfo fileInfo)
        {
            if (!filesText.TryGetValue(fileInfo.FullName, out var cmdText))
            {
                _logger.LogLine($"Getting text from file: {fileInfo.FullName}");
                cmdText = filesText[fileInfo.FullName]
                    = Regex.Split(File.ReadAllText(fileInfo.FullName), @"^\s*GO\s*$",
                            RegexOptions.Multiline | RegexOptions.IgnoreCase)
                        .Where(l => l.Trim() != string.Empty);
            }

            return cmdText;
        }

        private IEnumerable<string> GetDatabases(SqlConnection connection)
        {
            _logger.LogLine("Start receive databases");
            var list = new List<string>();
            try
            {
                using (var cmd = new SqlCommand("SELECT name from sys.databases", connection))
                {
                    using (IDataReader dr = cmd.ExecuteReader())
                    {
                        try
                        {
                            while (dr.Read())
                            {
                                list.Add(dr[0].ToString());
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogException(ex);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogException(ex);
            }

            return list.Where(d => _databases.Any(pattern => pattern.Equals(d, StringComparison.OrdinalIgnoreCase) || Regex.IsMatch(d, pattern)));
        }

        public void Dispose()
        {
        }
    }
}