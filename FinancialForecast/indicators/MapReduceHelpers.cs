using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace TechnicalIndicators.indicators
{
    class MapReduceHelpers
    {
        public static BsonJavaScript IsDatesEqual = new BsonJavaScript(@"
            function(date1, date2) {
	            return date1.getFullYear() == date2.getFullYear() && date1.getMonth() == date2.getMonth() && date1.getDate() == date2.getDate();
            }
        ");

        public static BsonJavaScript BinarySearch = new BsonJavaScript(@"
            function binarySearch(dates, date) {
                var low = 0;
	            var high = dates.length;
	            var mid = (low + high) >> 1;
	            while (!(equalityFunction(date, dates[mid])) && low < high) {
		            if (dates[mid] < date)
			            high = mid - 1;
		            else
			            low = mid + 1;
		            mid = (low + high) >> 1;
	            }
	            if (equalityFunction(date, dates[mid])) return mid;
	            else return -1;
            }
        ");
    }
}
