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

                    for (var i = 0; i < period && dateIndex >= 0; i++, dateIndex--) {
                        if (dates[dateIndex] > startingDate || equalityFunction(dates[dateIndex], startingDate))
                            emit(dates[dateIndex], {date: this.Tarih, headClose : this.Kapanis, tailClose : this.Kapanis, gain: 0 , loss: 0});
                    }
                }
            ");

            BsonJavaScript reducer = new BsonJavaScript(@"
                function(key, values) {
	                values.sort(function(a, b) {
		                return a.date < b.date;
	                });

                    sumGain = values[0].gain; 
                    sumLoss = values[0].loss;
                    var diff;
    
                    for(var i = 0; i < values.length - 1; i++) {
                        sumGain += values[i+1].gain;   
                        sumLoss += values[i+1].loss;
                        diff = values[i].tailClose - values[i+1].headClose;
                        if(diff > 0) 
                            sumGain += diff;
                        else 
                            sumLoss -= diff;
                    }

                    return {date: values[0].date, headClose: values[0].headClose, tailClose: values[values.length-1].tailClose, gain: sumGain, loss: sumLoss};
                }
            ");

            BsonJavaScript finalizer = new BsonJavaScript(@"
                function(key, reducedValue) {
                    if (reducedValue.loss == 0) return 100;
                    reducedValue = 100 - 100 / (1 + (reducedValue.gain / reducedValue.loss));
                    return reducedValue;
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
    }
}
