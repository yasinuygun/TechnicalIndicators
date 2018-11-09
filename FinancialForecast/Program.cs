using System;
using System.Linq;
using System.Windows.Forms;
using TechnicalIndicators.indicators;

namespace TechnicalIndicators
{
    class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string[] args)
        {
            /* Database connection */
            MongoDBService.InitiateService("mongodb://localhost:27017", "financialData", "data");

            /* Run application */
            RunFormApplication();
        }

        private static void RunFormApplication()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new form.View());
        }
    }
}
