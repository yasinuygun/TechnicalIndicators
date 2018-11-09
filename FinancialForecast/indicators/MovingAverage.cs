using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TechnicalIndicators.indicators
{
    class MovingAverage
    {
        public static double[] Simple(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (numberOfData <= 0)
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");

            List<BsonDocument> data = IndicatorService.GetData(code, targetDate, "Kapanis", numberOfData + period - 1);
            if (data.Count < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            double[] avg = calculateSMA(period, numberOfData, data);
            return avg;
        }

        private static double[] calculateSMA(int period, int numberOfData, List<BsonDocument> data)
        {
            if (data.Count < numberOfData + period - 1)
                numberOfData = data.Count - period + 1;

            double[] avg;
            avg = new double[numberOfData];
            double sumOfDatas = 0;
            for (int i = 0, j = period; j >= 1; i++, j--)
            {
                sumOfDatas += data.ElementAt(i).GetElement(0).Value.ToDouble();
            }
            avg[0] = sumOfDatas / period;

            for (int i = 1; i < numberOfData; i++)
            {
                sumOfDatas += data.ElementAt(period + i - 1).GetElement(0).Value.ToDouble();
                sumOfDatas -= data.ElementAt(i - 1).GetElement(0).Value.ToDouble();
                avg[i] = sumOfDatas / period;
            }

            return avg;
        }

        public static double[] Weighted(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (numberOfData <= 0)
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");

            List<BsonDocument> data = IndicatorService.GetData(code, targetDate, "Kapanis", numberOfData + period - 1);
            if (data.Count < period)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            double[] avg = calculateWMA(period, numberOfData, data);
            return avg;
        }

        private static double[] calculateWMA(int period, int numberOfData, List<BsonDocument> data)
        {
            if (data.Count < numberOfData + period - 1)
                numberOfData = data.Count - period + 1;

            double[] avg;
            avg = new double[numberOfData];
            int denominator = (period * (period + 1)) >> 1;
            double numerator = 0;
            double sumOfDatas = 0;

            for (int i = numberOfData - 1, j = period; j >= 1; i++, j--)
            {
                numerator += j * data.ElementAt(i).GetElement(0).Value.ToDouble();
                sumOfDatas += data.ElementAt(i).GetElement(0).Value.ToDouble();
            }
            avg[numberOfData - 1] = numerator / denominator;

            for (int i = numberOfData - 2; i >= 0; i--)
            {
                numerator = numerator + period * data.ElementAt(i).GetElement(0).Value.ToDouble() - sumOfDatas;
                sumOfDatas = sumOfDatas + data.ElementAt(i).GetElement(0).Value.ToDouble() - data.ElementAt(i + period).GetElement(0).Value.ToDouble();
                avg[i] = numerator / denominator;
            }

            return avg;
        }

        public static double[] Exponential(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
        {
            if (period <= 0)
                throw new IndicatorException("Period must be positive.");

            List<BsonDocument> data = IndicatorService.GetData(code, targetDate, "Kapanis", numberOfData);
            if (data.Count < period)
                period = data.Count;

            double[] avg = calculateEMA(period, numberOfData, data);

            return avg;
        }

        public static double[] calculateEMA(int period, int numberOfData, List<BsonDocument> data)
        {
            if (data.Count < numberOfData)
                numberOfData = data.Count;
            double[] avg = new double[numberOfData];
            double coeff = 2.0 / (1 + period);
            int i = numberOfData - 1;
            avg[i] = data.ElementAt(i--).GetElement(0).Value.ToDouble();
            for (; i >= 0; i--)
            {
                avg[i] = coeff * (data.ElementAt(i).GetElement(0).Value.ToDouble() - avg[i + 1]) + avg[i + 1];
            }

            return avg;
        }

        public static double[] calculateEMA(int period, int numberOfData, double[] data)
        {
            if (data.Length < numberOfData)
                numberOfData = data.Length;

            double[] avg = new double[numberOfData];
            double coeff = 2.0 / (1 + period);
            int i = numberOfData - 1;
            avg[i] = data[i--];
            for (; i >= 0; i--)
            {
                avg[i] = coeff * (data[i] - avg[i + 1]) + avg[i + 1];
            }

            return avg;
        }

        /* ------------------------------------------- */
        /* MapReduce implementations of the indicators */
        /* ------------------------------------------- */

        public static double[] SimpleMR(string code, DateTime targetDate, int period = 14, int numberOfData = 30)
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
                Filter = filter,
                Sort = sort,
                Scope = new BsonDocument { { "startingDate", startingDate }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "period", period }, { "numberOfData", numberOfData }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual }, { "dates", new BsonArray(dates) } },
                OutputOptions = MapReduceOutputOptions.Replace("smaOut", "financialData", false)
            };

            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
		                return;
	                else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
		                return;
	
	                var dateIndex;
	
	                dateIndex = binarySearch(dates, this.Tarih);
	                if (dateIndex == -1)
		                return;
	
	                for (var i = 0; i < period && dateIndex >= 0; i++, dateIndex--) {
		                if (dates[dateIndex] > startingDate || equalityFunction(dates[dateIndex], startingDate))
			                emit(dates[dateIndex], this.Kapanis);
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
	                return Array.sum(values);
                }
            ");

            BsonJavaScript finalizer = new BsonJavaScript(@"
                function(key, reducedValue) {
	                return reducedValue / period;
                }
            ");

            options.Finalize = finalizer;

            double[] avg = new double[numberOfData];

            List<BsonDocument> resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);
            for (int i = 0, j = numberOfData - 1; i < numberOfData; i++, j--)
            {
                avg[i] = resultSet.ElementAt(j).GetElement(1).Value.ToDouble();
            }

            return avg;
        }

        public static double[] WeightedMR(string code, DateTime targetDate, int period = 14, int numberOfData = 30)
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
            double denominator = (period * (period + 1)) >> 1;

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
                OutputOptions = MapReduceOutputOptions.Replace("wmaOut", "financialData", false),
                Scope = new BsonDocument { { "startingDate", startingDate }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "period", period }, { "denominator", denominator }, { "numberOfData", numberOfData }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual }, { "dates", new BsonArray(dates) } }
            };

            double[] avg;
            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
		                return;
	                else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
		                return;
	
	                var dateIndex;
	
	                dateIndex = binarySearch(dates, this.Tarih);
	                if (dateIndex == -1)
		                return;
	
	                var factor = period;

	                for (var i = 0; i < period && dateIndex >= 0; i++, dateIndex--) {
		                if (dates[dateIndex] > startingDate || equalityFunction(dates[dateIndex], startingDate))
			                emit(dates[dateIndex], this.Kapanis * factor);
		                factor--;
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
	                return Array.sum(values);
                }
            ");

            BsonJavaScript finalizer = new BsonJavaScript(@"
                function(key, reducedValue) {
	                return reducedValue / denominator;
                }
            ");

            options.Finalize = finalizer;

            List<BsonDocument> resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);

            avg = new double[numberOfData];

            for (int i = 0, j = numberOfData - 1; i < numberOfData; i++, j--)
            {
                avg[i] = resultSet.ElementAt(j).GetElement(1).Value.ToDouble();
            }

            return avg;
        }

        public static double[] ExponentialMR(string code, DateTime targetDate, int period = 14, int numberOfData = 30)
        {
            if (period <= 0)
                throw new IndicatorException("Period must be positive.");

            int dataCount = IndicatorService.DataCount(code, targetDate);
            if (dataCount < numberOfData)
                numberOfData = dataCount;

            double alpha = 2.0 / (1 + period);
            double beta = 1 - alpha;
            double minimumCoefficient = 0.000001;
            int epoch = (int)Math.Ceiling(Math.Log(minimumCoefficient / alpha, Math.Floor(beta * 1000) / 1000));

            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
		                return;
	                else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
		                return;

	                var dateIndex = binarySearch(dates, this.Tarih);
	                if (dateIndex == -1)
		                return;
	
	                var value;
	                if (dateIndex == numberOfData-1) {
		                value = this.Kapanis;
		                for (var i = dateIndex, j = 0; j < epoch && i >= 0; i--, j++) {
			                emit(dates[i], value);
			                value = value * beta;
		                }
	                } else {
		                value = this.Kapanis * alpha;
		                for (var i = dateIndex, j = 0; j < epoch && i >= 0; i--, j++) {
			                emit(dates[i], value);
			                value = value * beta;
		                }
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key,values){
                    return Array.sum(values);
                }
            ");

            var filter = new BsonDocument { { "Kod", code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(targetDate)) } };
            var sort = new BsonDocument("Tarih", -1);
            var mongoService = MongoDBService.GetService();

            var projection = new BsonDocument { { "_id", 0 }, { "Tarih", 1 } };
            var lastDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            var limitDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, numberOfData - 1).GetElement(0).Value.ToLocalTime();
            var dates = IndicatorService.GetData(code, targetDate, "Tarih", numberOfData).Select(p => p.GetElement(0).Value.ToLocalTime()).ToArray();
            var scope = new BsonDocument { { "numberOfData", numberOfData }, { "epoch", epoch }, { "alpha", alpha }, { "beta", beta }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "dates", new BsonArray(dates) }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual } };

            MapReduceOptions<BsonDocument, BsonDocument> options = new MapReduceOptions<BsonDocument, BsonDocument>
            {
                Filter = filter,
                Sort = sort,
                Scope = scope,
                OutputOptions = MapReduceOutputOptions.Replace("emaOut", "financialData", false)
            };

            double[] values = MongoDBService.GetService().MapReduceMany(mapper, reducer, options).Select(p => p.GetElement("value").Value.ToDouble()).ToArray();
            Array.Reverse(values);
            return values;
        }
    }
}
