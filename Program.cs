using FatumCore;
using PhlozCore;

namespace PhlozServerDebug
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        //[STAThread]
        static void Main()
        {
            try
            {
                var MSP = new MasterStreamProcessor(new fatumconfig());
                MSP.Start();
            }
            catch (System.Threading.ThreadAbortException)
            {

            }
        }
    }
}
