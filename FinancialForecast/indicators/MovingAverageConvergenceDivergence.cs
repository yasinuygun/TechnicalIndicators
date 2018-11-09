using System;
using System.Collections.Generic;
using MongoDB.Bson;

namespace TechnicalIndicators.indicators
{
    class MovingAverageConvergenceDivergence
    {
        private readonly string Code;
        private int FirstPeriod { get; set; }
        private int SecondPeriod { get; set; }
        private int TriggerPeriod { get; set; }
        private int NumberOfData { get; set; }
        private readonly DateTime TargetDate;

        public double[] EmaFirst { get; set; }
        public double[] EmaSecond { get; set; }

        public double[] MacdLine { get; set; }
        public double[] TriggerLine { get; set; }

        public MovingAverageConvergenceDivergence(string code, DateTime targetDate, int firstPeriod = 12, int secondPeriod = 26, int triggerPeriod = 9, int numberOfData = 1, bool mapReduce = false)
        {
            if (firstPeriod <= 0)
                throw new IndicatorException("Period must be positive.");
            else if (secondPeriod <= 0)
                throw new IndicatorException("Period must be positive.");
            else if (secondPeriod <= firstPeriod)
                throw new IndicatorException("Second period cannot be less than first period.");
            else if (firstPeriod <= triggerPeriod)
                throw new IndicatorException("First period cannot be less than trigger period.");
            else if (triggerPeriod <= 0)
                throw new IndicatorException("Period must be positive.");

            Code = code;
            TargetDate = targetDate;
            FirstPeriod = firstPeriod;
            SecondPeriod = secondPeriod;
            TriggerPeriod = triggerPeriod;
            NumberOfData = numberOfData;
            if (!mapReduce)
                CalculateMovingAverageConvergenceDivergence();
            else
                CalculateMovingAverageConvergenceDivergenceMR();
        }

        private void CalculateMovingAverageConvergenceDivergence()
        {
            var data = IndicatorService.GetData(Code, TargetDate, "Kapanis", NumberOfData);

            calculateMACD(data);
            calculateTrigger();
        }

        private void calculateMACD(List<BsonDocument> data)
        {
            EmaFirst = MovingAverage.calculateEMA(FirstPeriod, NumberOfData, data);
            EmaSecond = MovingAverage.calculateEMA(SecondPeriod, NumberOfData, data);

            MacdLine = new double[EmaFirst.Length];

            for (int i = 0; i < EmaFirst.Length; i++)
            {
                MacdLine[i] = EmaFirst[i] - EmaSecond[i];
            }
        }

        private void calculateTrigger()
        {
            TriggerLine = MovingAverage.calculateEMA(TriggerPeriod, NumberOfData, MacdLine);
        }

        private void CalculateMovingAverageConvergenceDivergenceMR()
        {
            EmaFirst = MovingAverage.ExponentialMR(Code, TargetDate, FirstPeriod, NumberOfData);
            EmaSecond = MovingAverage.ExponentialMR(Code, TargetDate, SecondPeriod, NumberOfData);

            MacdLine = new double[EmaFirst.Length];

            for (int i = 0; i < EmaFirst.Length; i++)
            {
                MacdLine[i] = EmaFirst[i] - EmaSecond[i];
            }

            calculateTrigger();
        }

    }
}
