using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechnicalIndicators
{
    [Serializable()]
    class IndicatorException : Exception
    {
        public static readonly string DATA_NOT_ENOUGH_MESSAGE = "Veritabanındaki veri miktarı istenen indikatörün koşullarını sağlamıyor.";

        public IndicatorException() : base() { }

        public IndicatorException(string message) : base(message) { }
    }
}
