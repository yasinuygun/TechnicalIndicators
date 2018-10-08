using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TechnicalIndicators.indicators
{
    class Stochastics
    {
        private readonly string Code;
        private readonly int FastKPeriod;
        private readonly int FastDPeriod;
        private readonly int SlowDPeriod;
        private readonly DateTime TargetDate;
        private int NumberOfData;
        public double[] FastK { get; set; }
        public double[] FastD { get; set; }
        public double[] SlowK { get { return FastD; } }
        public double[] SlowD { get; set; }

        public Stochastics(string code, DateTime targetDate, int fastKPeriod = 14, int fastDPeriod = 3, int slowDPeriod = 3, int numberOfData = 1, bool mapReduce = false)
        {
            if (fastKPeriod <= 0)
            {
                throw new IndicatorException("Fast K periyodu pozitif sayı olmalıdır.");
            }
            else if (fastDPeriod <= 0)
            {
                throw new IndicatorException("Fast D periyodu pozitif sayı olmalıdır.");
            }
            else if (slowDPeriod <= 0)
            {
                throw new IndicatorException("Slow D periyodu pozitif sayı olmalıdır.");
            }
            else if (numberOfData <= 0)
            {
                throw new IndicatorException("Gösterilecek veri sayısı pozitif sayı olmalıdır.");
            }
            else if (numberOfData < fastKPeriod)
            {
                throw new IndicatorException("Gösterilecek veri sayısı Fast K periyodundan küçük olamaz.");
            }

            Code = code;
            TargetDate = targetDate;
            FastKPeriod = fastKPeriod;
            FastDPeriod = fastDPeriod;
            SlowDPeriod = slowDPeriod;
            NumberOfData = numberOfData;
            if (!mapReduce)
                CalculateStochastics();
            else
                CalculateStochasticsMR();
        }

        private void CalculateStochastics()
        {
            var data = IndicatorService.GetData(Code, TargetDate, new string[] { "Kapanis", "Dusuk", "Yuksek" }, FastKPeriod + NumberOfData - 1);
            if (data.Count < FastKPeriod)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            CalculateFastK(data);
            CalculateFastD();
            CalculateSlowD();
        }

        private void CalculateFastK(List<BsonDocument> data)
        {
            // indices used in this method is from the "data" result set: 0 -> Dusuk, 1 -> Yuksek, 2 -> Kapanis
            double lowestLow, highestHigh, currentClose, prevLow, prevHigh, oldestLow, oldestHigh;
            if (data.Count < NumberOfData + FastKPeriod - 1)
                NumberOfData = data.Count - FastKPeriod + 1;

            FastK = new double[NumberOfData];

            var fastKElements = data.Skip(0).Take(FastKPeriod);
            lowestLow = fastKElements.Min(p => p.GetElement(0).Value.ToDouble());
            highestHigh = fastKElements.Max(p => p.GetElement(1).Value.ToDouble());
            currentClose = fastKElements.FirstOrDefault().GetElement(2).Value.ToDouble();
            FastK[0] = 100 * (currentClose - lowestLow) / (highestHigh - lowestLow);

            for (int i = 1; i < NumberOfData; i++)
            {
                var currentElement = data.ElementAt(i);
                currentClose = currentElement.GetElement(2).Value.ToDouble();

                var oldestElement = data.ElementAt(i + FastKPeriod - 1);

                var prevElement = data.ElementAt(i - 1);
                prevLow = prevElement.GetElement(0).Value.ToDouble();
                prevHigh = prevElement.GetElement(1).Value.ToDouble();

                if (prevLow == lowestLow || prevHigh == highestHigh)
                {
                    fastKElements = data.Skip(i).Take(FastKPeriod);

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

                FastK[i] = 100 * (currentClose - lowestLow) / (highestHigh - lowestLow);
            }
        }

        private void CalculateFastD()
        {
            FastD = CalculateD(FastK, FastDPeriod);
        }

        private void CalculateSlowD()
        {
            SlowD = CalculateD(FastD, SlowDPeriod);
        }

        private double[] CalculateD(double[] src, int period)
        {
            double[] dest = null;
            if (src.Length >= period)
            {
                int len = src.Length - period + 1;
                dest = new double[len];

                double sumOfDatas = 0;
                for (int i = 0; i < period; i++)
                {
                    sumOfDatas += src[i];
                }
                dest[0] = sumOfDatas / period;

                for (int i = 1, j = i + period - 1; i < len; i++, j++)
                {
                    sumOfDatas -= src[i - 1];
                    sumOfDatas += src[j];
                    dest[i] = sumOfDatas / period;
                }
            }
            else
            {
                dest = new double[1];
                dest[0] = src.Average();
            }
            return dest;
        }

        private void CalculateStochasticsMR()
        {
            int dataCount = IndicatorService.DataCount(Code, TargetDate);
            if (dataCount < FastKPeriod)
                throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);

            CalculateFastK_MR(dataCount);
            CalculateFastD();
            CalculateSlowD();
        }

        private void CalculateFastK_MR(int dataCount)
        {
            if (dataCount < NumberOfData + FastKPeriod - 1)
                NumberOfData = dataCount - FastKPeriod + 1;

            var limit = FastKPeriod + NumberOfData - 1;
            var filter = new BsonDocument { { "Kod", Code }, { "Tarih", new BsonDocument("$lte", new BsonDateTime(TargetDate)) } };
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
                    reducedValue = 100 * (reducedValue.close - reducedValue.low) / (reducedValue.high - reducedValue.low);
                    return reducedValue;
                }
            ");

            var startingDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, NumberOfData - 1).GetElement(0).Value.ToLocalTime();
            var lastDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            var limitDate = MongoDBService.GetService().FindOneSortProjectSkip(filter, sort, projection, limit - 1).GetElement(0).Value.ToLocalTime();
            var dates = IndicatorService.GetData(Code, TargetDate, "Tarih", limit).Select(p => p.GetElement(0).Value.ToLocalTime()).ToArray();
            var scope = new BsonDocument { { "startingDate", startingDate }, { "lastDate", lastDate }, { "limitDate", limitDate }, { "period", FastKPeriod }, { "numberOfData", NumberOfData }, { "binarySearch", MapReduceHelpers.BinarySearch }, { "equalityFunction", MapReduceHelpers.IsDatesEqual }, { "dates", new BsonArray(dates) } };

            var options = new MapReduceOptions<BsonDocument, BsonDocument>()
            {
                Filter = filter,
                Sort = sort,
                Scope = scope,
                OutputOptions = MapReduceOutputOptions.Replace("stochasticsOut", "financialData", false),
                Finalize = finalizer
            };

            FastK = new double[NumberOfData];
            var resultSet = MongoDBService.GetService().MapReduceMany(mapper, reducer, options);

            for (int i = 0, j = resultSet.Count -1; i < NumberOfData; i++, j--)
            {
                FastK[i] = resultSet.ElementAt(j).GetElement(1).Value.ToDouble();
            }
        }
    }
}
