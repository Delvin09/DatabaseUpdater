using System;
using System.Net.Configuration;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseUpdater
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = new CommandLogger();
            logger.LogLine("Start program.");
            foreach (var s in args)
                logger.LogLine(s);
            
            var processor = new CommandProcessor(new Arguments(args), logger);
            try
            {
                processor.Process();
            }
            catch (Exception ex)
            {
                logger.LogException(ex);
                throw;
            }
            finally
            {
                processor.Dispose();
                logger.Dispose();
            }
        }
    }
}
