using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TechnicalIndicators.indicators
{
    class WilliamsR
    {
        public static double[] Wsr(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (numberOfData <= 0)
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");

            var data = IndicatorService.GetData(code, targetDate, new string[] { "Kapanis", "Dusuk", "Yuksek" }, period + numberOfData - 1);
            if (data.Count < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            return calculateWsr(period, numberOfData, data);
        }

        private static double[] calculateWsr(int period, int numberOfData, List<BsonDocument> data)
        {
            // indices used in this method is from the "data" result set: 0 -> Dusuk, 1 -> Yuksek, 2 -> Kapanis
            double lowestLow, highestHigh, currentClose, prevLow, prevHigh, oldestLow, oldestHigh;

            if (data.Count < numberOfData + period - 1)
                numberOfData = data.Count - period + 1;
            double[] williamsR = new double[numberOfData];

            var fastKElements = data.Skip(0).Take(period);
            lowestLow = fastKElements.Min(p => p.GetElement(0).Value.ToDouble());
            highestHigh = fastKElements.Max(p => p.GetElement(1).Value.ToDouble());
            currentClose = fastKElements.FirstOrDefault().GetElement(2).Value.ToDouble();
            williamsR[0] = -100 * (highestHigh - currentClose) / (highestHigh - lowestLow);

            for (int i = 1; i < numberOfData; i++)
            {
                var currentElement = data.ElementAt(i);
                currentClose = currentElement.GetElement(2).Value.ToDouble();

                var oldestElement = data.ElementAt(i + period - 1);

                var prevElement = data.ElementAt(i - 1);
                prevLow = prevElement.GetElement(0).Value.ToDouble();
                prevHigh = prevElement.GetElement(1).Value.ToDouble();

                if (prevLow == lowestLow || prevHigh == highestHigh)
                {
                    fastKElements = data.Skip(i).Take(period);

                    if (prevLow == lowestLow)
                    {
                        lowestLow = fastKElements.Min(p => p.GetElement(0).Value.ToDouble());
                    }
                    else
                    {
                        oldestLow = oldestElement.GetElement(0).Value.ToDouble();
                        lowestLow = oldestLow < lowestLow ? oldestLow : lowestLow;
                    }
                    if (prevHigh == highestHigh)
                    {
                        highestHigh = fastKElements.Max(p => p.GetElement(1).Value.ToDouble());
                    }
                    else
                    {
                        oldestHigh = oldestElement.GetElement(1).Value.ToDouble();
                        highestHigh = oldestHigh > highestHigh ? oldestHigh : highestHigh;
                    }
                }
                else
                {
                    oldestLow = oldestElement.GetElement(0).Value.ToDouble();
                    oldestHigh = oldestElement.GetElement(1).Value.ToDouble();
                    lowestLow = oldestLow < lowestLow ? oldestLow : lowestLow;
                    highestHigh = oldestHigh > highestHigh ? oldestHigh : highestHigh;
                }

                williamsR[i] = -100 * (highestHigh - currentClose) / (highestHigh - lowestLow);
            }

            return williamsR;
        }

        public static double[] WsrMR(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (numberOfData <= 0)
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");

            int dataCount = IndicatorService.DataCount(code, targetDate);
            if (dataCount < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            if (dataCount < numberOfData + period - 1)
                numberOfData = dataCount - period + 1;

            var limit = period + numberOfData - 1;
            var filter = new BsonDocument { { "Kod", code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(targetDate)) } };
            var sort = new BsonDocument("Tarih", -1);
            var projection = new BsonDocument { { "_id", 0 }, { "Tarih", 1 } };

            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
		                return;
	                else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
		                return;
	
	                var dateIndex = binarySearch(dates, this.Tarih);

	                if (dateIndex == -1)
		                return;
	
	                for (var i = 0; i < period && dateIndex >= 0; i++, dateIndex--) {
		                if (dates[dateIndex] > startingDate || equalityFunction(dates[dateIndex], startingDate))
			                emit(dates[dateIndex], {close: this.Kapanis, low: this.Dusuk, high: this.Yuksek});
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
                    var result = {close: values[0].close, low: values[0].low, high: values[0].high};
                    for (var i = 1; i < values.length; i++) {
                        result.low = values[i].low < result.low ? values[i].low : result.low;
                        result.high = values[i].high > result.high ? values[i].high : result.high;
                    }
                    return result;
                }
            ");

            BsonJavaScript finalizer = new BsonJavaScript(@"
                function(key, reducedValue) {
                    reducedValue = -100 * (reducedValue.high - reducedValue.close) / (reducedValue.high - reducedValue.low);
                    return reducedValue;
                }
            ");

            var startingDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, numberOfData - 1).GetElement(0).Value.ToLocalTime();
            var lastDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            var limitDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, limit - 1).GetElement(0).Value.ToLocalTime();
            var dates = IndicatorService.GetData(code, targetDate, "Tarih", limit).Select(p => p.GetElement(0).Value.ToLocalTime()).ToArray();
            var scope = new BsonDocument { { "startingDate", startingDate }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "period", period }, { "numberOfData", numberOfData }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual }, { "dates", new BsonArray(dates) } };

            var options = new MapReduceOptions<BsonDocument, BsonDocument>()
            {
                Filter = filter,
                Sort = sort,
                Scope = scope,
                OutputOptions = MapReduceOutputOptions.Replace("wsrOut", "financialData", false),
                Finalize = finalizer
            };

            double[] wsr = new double[numberOfData];
            var resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);

            for (int i = 0, j = resultSet.Count-1; i < numberOfData; i++, j--)
            {
                wsr[i] = resultSet.ElementAt(j).GetElement(1).Value.ToDouble();
            }

            return wsr;
        }
    }
}
