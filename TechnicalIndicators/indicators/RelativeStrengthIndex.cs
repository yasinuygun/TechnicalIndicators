using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TechnicalIndicators.indicators
{
    class RelativeStrengthIndex
    {
        public static double[] Rsi(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (numberOfData <= 0)
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");

            var data = IndicatorService.GetData(code, targetDate, "Kapanis", period + numberOfData - 1);

            if (data.Count < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            return calculateRSI(period, numberOfData, data);
        }

        private static double[] calculateRSI(int period, int numberOfData, List<BsonDocument> data)
        {
            double[] rsi;

            if (data.Count < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            if (data.Count < numberOfData + period - 1)
                numberOfData = data.Count - period + 1;

            int limit = numberOfData + period - 1;

            rsi = new double[numberOfData];

            double sumOfGains = 0, sumOfLoss = 0;
            int i = 0;
            for (; i < period - 1; i++)
            {
                double diff = data.ElementAt(i).GetElement(0).Value.ToDouble() - data.ElementAt(i + 1).GetElement(0).Value.ToDouble();
                if (diff > 0)
                    sumOfGains += diff;
                else
                    sumOfLoss -= diff;
            }

            i = 0;
            if (sumOfLoss == 0)
                rsi[i++] = 100;
            else
                rsi[i++] = 100 - (100 / (1 + (sumOfGains / sumOfLoss)));

            for (; i < numberOfData; i++)
            {
                double diffLast = data.ElementAt(period + i - 2).GetElement(0).Value.ToDouble() - data.ElementAt(period + i - 1).GetElement(0).Value.ToDouble();
                double diffFirst = data.ElementAt(i - 1).GetElement(0).Value.ToDouble() - data.ElementAt(i).GetElement(0).Value.ToDouble();

                if (diffLast > 0)
                    sumOfGains += diffLast;
                else
                    sumOfLoss -= diffLast;

                if (diffFirst > 0)
                    sumOfGains -= diffFirst;
                else
                    sumOfLoss += diffFirst;

                if (sumOfLoss == 0)
                    rsi[i] = 100;
                else
                    rsi[i] = 100 - (100 / (1 + (sumOfGains / sumOfLoss)));
            }

            return rsi;
        }

        public static double[] RsiMR(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
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

            int limit = numberOfData + period - 1;

            var filter = new BsonDocument { { "Kod", code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(targetDate)) } };
            var sort = new BsonDocument("Tarih", -1);

            var projection = new BsonDocument { { "_id", 0 }, { "Tarih", 1 } };
            var startingDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, numberOfData - 1).GetElement(0).Value.ToLocalTime();
            var lastDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            var limitDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, limit - 1).GetElement(0).Value.ToLocalTime();
            var dates = IndicatorService.GetData(code, targetDate, "Tarih", limit).Select(p => p.GetElement(0).Value.ToLocalTime()).ToArray();

            MapReduceOptions<BsonDocument, BsonDocument> options = new MapReduceOptions<BsonDocument, BsonDocument>
            {
                Filter = new BsonDocument { { "Kod", code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(targetDate)) } },
                Sort = new BsonDocument("Tarih", -1),
                Scope = new BsonDocument { { "startingDate", startingDate }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "period", period }, { "numberOfData", numberOfData }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual }, { "dates", new BsonArray(dates) } },
                OutputOptions = MapReduceOutputOptions.Replace("rsiOut", "financialData", false)
            };

            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
                        return;
                    else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
                        return;

                    var dateIndex = binarySearch(dates, this.Tarih);
                    if (dateIndex == -1)
    	                return;

                    emit(dates[dateIndex], {date: this.Tarih, close: this.Kapanis});
                    if (dateIndex != 0)
    	                emit(dates[dateIndex-1], {date: this.Tarih, close: this.Kapanis});
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key,values){
                    if (values[1].date > values[0].date)
    	                return {date: key, close: values[1].close - values[0].close};
                    else
    	                return {date: key, close: values[0].close - values[1].close};
                }
            ");

            List<BsonDocument> resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);

            double sumLoss = 0, sumGain = 0;

            int i, j;
            for (i = resultSet.Count - 1, j = 1; j < period; i--, j++)
            {
                var diff = resultSet.ElementAt(i).GetElement(1).Value.ToBsonDocument().GetElement(1).Value.ToDouble();
                if (diff > 0)
                    sumGain += diff;
                else
                    sumLoss -= diff;
            }

            double[] result = new double[numberOfData]; // result returns the calculated rsi value.
            j = 0;

            if (sumLoss < 0)
                result[j++] = 100;
            else
                result[j++] = 100 - (100 / (1 + (sumGain / sumLoss)));

            for (; j < numberOfData; j++)
            {
                var diffLast = resultSet.ElementAt(i - j + 1).GetElement(1).Value.ToBsonDocument().GetElement(1).Value.ToDouble();
                var diffFirst = resultSet.ElementAt(i - j + period).GetElement(1).Value.ToBsonDocument().GetElement(1).Value.ToDouble();
                if (diffLast > 0)
                    sumGain += diffLast;
                else
                    sumLoss -= diffLast;

                if (diffFirst > 0)
                    sumGain -= diffFirst;
                else
                    sumLoss += diffFirst;

                if (sumLoss == 0)
                    result[j] = 100;
                else
                    result[j] = 100 - (100 / (1 + (sumGain / sumLoss)));
            }

            return result;
        }
    }
}
