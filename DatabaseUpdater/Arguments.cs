using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace DatabaseUpdater
{
    public interface IArguments : IEnumerable<KeyValuePair<string, string>>
    {
        string this[string key] { get; }
    }

    public class Arguments : IArguments
    {
        private readonly char[] _keys = { '-', '/', '\\' };
        private readonly Dictionary<string, string> _dict = new Dictionary<string, string>();

        public Arguments(string[] args = null)
        {
            if (args == null) return;

            var prevKey = string.Empty;
            foreach (var item in args)
            {
                if (_keys.Contains(item.First()))
                {
                    prevKey = item.Substring(1);
                    _dict[prevKey] = string.Empty;
                }
                else
                {
                    _dict[prevKey] = item;
                }
            }
        }

        public string this[string key] => _dict[key];

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}