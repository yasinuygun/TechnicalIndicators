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

        public static double[] Exponential(string code, DateTime targetDate, int period = 14)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");

            List<BsonDocument> data = IndicatorService.GetData(code, targetDate, "Kapanis", period);
            if (data.Count < period)
                period = data.Count;

            double[] avg = calculateEMA(period, data);

            return avg;
        }

        public static double[] calculateEMA(int period, List<BsonDocument> data)
        {
            double[] avg = new double[period];
            double coeff = 2.0 / (1 + period);
            int i = period - 1;
            avg[i] = data.ElementAt(i--).GetElement(0).Value.ToDouble();
            for (; i >= 0; i--)
            {
                avg[i] = coeff * (data.ElementAt(i).GetElement(0).Value.ToDouble() - avg[i + 1]) + avg[i + 1];
            }

            return avg;
        }

        public static double[] calculateEMA(int period, double[] data)
        {
            if (data.Length < period)
                period = data.Length;

            double[] avg = new double[period];
            double coeff = 2.0 / (1 + period);
            int i = period - 1;
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

        public static double[] SimpleMR(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
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
	                if (this.Tarih < limitDate || this.Tarih > lastDate)
		                return;
	
	                var dateIndex = binarySearch(dates, this.Tarih);
	
	                if (dateIndex < period) {
		                emit(dates[0], {close: this.Kapanis, date: this.Tarih});
		                emit(dates[dateIndex + 1], {close: this.Kapanis, date: this.Tarih});
	                } else if (dateIndex < numberOfData - period) {
		                emit(dates[dateIndex - period + 1], {close: this.Kapanis, date: this.Tarih});
		                emit(dates[dateIndex + 1], {close: this.Kapanis, date: this.Tarih});
	                } else {
		                emit(dates[dateIndex - period + 1], {close: this.Kapanis, date: this.Tarih});
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
	                if (equalityFunction(key, dates[0])) {
		                var result = {close: values[0].close, date: key};
		                for (var i = 1; i < values.length; i++) {
			                result.close += values[i].close;
		                }
		                return result;
	                } else {
		                var result = {close: 0, date: key};
		                if (values[1].date < values[0].date) {
			                result.close = values[1].close - values[0].close;
		                } else {
			                result.close = values[0].close - values[1].close;
		                }
		                return result;
	                }
                }
            ");

            List<BsonDocument> resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);

            double[] avg = new double[numberOfData];
            double sumOfDatas = resultSet.ElementAt(resultSet.Count - 1).GetElement(1).Value.ToBsonDocument().GetElement(0).Value.ToDouble();
            avg[0] = sumOfDatas / period;
            for (int i = 1, j = resultSet.Count - 2; i < numberOfData; i++, j--)
            {
                sumOfDatas += resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(0).Value.ToDouble();
                avg[i] = sumOfDatas / period;
            }

            return avg;
        }

        public static double[] WeightedMR(string code, DateTime targetDate, int period = 14, int numberOfData = 1)
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

            BsonJavaScript mapper = new BsonJavaScript(@"
                function() {
	                if (!equalityFunction(this.Tarih, limitDate) && this.Tarih < limitDate)
		                return;
	                else if (!equalityFunction(this.Tarih, lastDate) && this.Tarih > lastDate)
		                return;
	
	                var dateIndex = binarySearch(dates, this.Tarih);
	
	                if (dateIndex < period) {
		                emit(dates[0], {sumOfWeightedCloses: this.Kapanis * (period - dateIndex), sumOfCloses: this.Kapanis, close: this.Kapanis, date: this.Tarih});
		                emit(dates[dateIndex + 1], {sumOfWeightedCloses: this.Kapanis, sumOfCloses: this.Kapanis, close: this.Kapanis, date: this.Tarih});
	                } else if (dateIndex < numberOfData - period) {
		                emit(dates[dateIndex - period + 1], {sumOfWeightedCloses: this.Kapanis, sumOfCloses: this.Kapanis, close: this.Kapanis, date: this.Tarih});
		                emit(dates[dateIndex + 1], {sumOfWeightedCloses: this.Kapanis, sumOfCloses: this.Kapanis, close: this.Kapanis, date: this.Tarih});
	                } else {
		                emit(dates[dateIndex - period + 1], {sumOfWeightedCloses: this.Kapanis, sumOfCloses: this.Kapanis, close: this.Kapanis, date: this.Tarih});
	                }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
	                if (equalityFunction(key, dates[0])) {
		                var result = {sumOfWeightedCloses: values[0].sumOfWeightedCloses, sumOfCloses: values[0].sumOfCloses, close: 0, date: key};
		                for (var i = 1; i < values.length; i++) {
			                result.sumOfWeightedCloses += values[i].sumOfWeightedCloses;
			                result.sumOfCloses += values[i].sumOfCloses;
		                }
		                return result;
	                } else {
		                var result = {sumOfWeightedCloses: 0, sumOfCloses: 0, close: 0, date: key};
		                if (values[1].date < values[0].date) {
			                result.sumOfWeightedCloses = values[1].sumOfWeightedCloses - period * values[0].sumOfWeightedCloses;
			                result.sumOfCloses = values[1].sumOfCloses - values[0].sumOfCloses;
			                result.close = values[1].close;
		                } else {
			                result.sumOfWeightedCloses = values[0].sumOfWeightedCloses - period * values[1].sumOfWeightedCloses;
			                result.sumOfCloses = values[0].sumOfCloses - values[1].sumOfCloses;
			                result.close = values[0].close;
		                }
		                return result;
	                }
                }
            ");

            List<BsonDocument> resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);
            double[] avg = new double[numberOfData];

            int j = resultSet.Count - 1;
            double sumOfWeightedCloses = resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(0).Value.ToDouble();
            double sumOfCloses = resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(1).Value.ToDouble();
            j--;

            avg[0] = sumOfWeightedCloses / denominator;

            for (int i = 1; i < numberOfData; i++, j--)
            {
                sumOfCloses += resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(1).Value.ToDouble();
                sumOfWeightedCloses += resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(0).Value.ToDouble() + sumOfCloses - resultSet.ElementAt(j).GetElement(1).Value.ToBsonDocument().GetElement(2).Value.ToDouble();
                avg[i] = sumOfWeightedCloses / denominator;
            }

            return avg;
        }

        public static double[] ExponentialMR(string code, DateTime targetDate, int period = 14)
        {
            if (period <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");

            int dataCount = IndicatorService.DataCount(code, targetDate);
            if (dataCount < period)
                period = dataCount;

            double coeff = 2.0 / (1 + period);

            BsonJavaScript mapper = new BsonJavaScript(@"
                function(){
                    if (this.Tarih < limitDate || this.Tarih > lastDate)
		                return;
		            emit(this.Tarih, this.Kapanis);
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key,values){
                    return null;
                }
            ");

            BsonJavaScript finalizer = new BsonJavaScript(@"
                function(key, reducedValue){
                    return reducedValue * coeff;                    
                }
            ");

            var filter = new BsonDocument { { "Kod", code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(targetDate)) } };
            var sort = new BsonDocument("Tarih", -1);
            var mongoService = MongoDBService.GetService();

            var projection = new BsonDocument { { "_id", 0 }, { "Tarih", 1 } };
            var lastDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            var limitDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, period -2).GetElement(0).Value.ToLocalTime();
            var scope = new BsonDocument { { "lastDate", lastDate }, { "limitDate", limitDate }, { "coeff", coeff } };

            MapReduceOptions<BsonDocument, BsonDocument> options = new MapReduceOptions<BsonDocument, BsonDocument>
            {
                Filter = filter,
                Sort = sort,
                Finalize = finalizer,
                Scope = scope,
                OutputOptions = MapReduceOutputOptions.Replace("emaOut", "financialData", false)
            };

            List<BsonDocument> data = IndicatorService.GetData(code, targetDate, "Kapanis", period);

            int i = period - 1;
            double[] ema = new double[i + 1];
            ema[i] = data.ElementAt(i--).GetElement(0).Value.ToDouble();

            double[] values = MongoDBService.GetService().MapReduceMany(mapper, reducer, options).Select(p => p.GetElement("value").Value.ToDouble()).ToArray();

            for (int j = 0; i >= 0; i--, j++)
            {
                ema[i] = values[j] + (1 - coeff) * ema[i + 1];
            }
            return ema;
        }
    }
}
