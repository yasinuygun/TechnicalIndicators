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
        private readonly DateTime TargetDate;

        public double[] EmaFirst { get; set; }
        public double[] EmaSecond { get; set; }

        public double[] MacdLine { get; set; }
        public double[] TriggerLine { get; set; }

        public MovingAverageConvergenceDivergence(string code, DateTime targetDate, int FirstPeriod = 12, int SecondPeriod = 26, int TriggerPeriod = 9, bool mapReduce = false)
        {
            if (FirstPeriod <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (SecondPeriod <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");
            else if (SecondPeriod <= FirstPeriod)
                throw new IndicatorException("İkinci girilen period ilk girilen period değerinden küçük olamaz!");
            else if (FirstPeriod <= TriggerPeriod)
                throw new IndicatorException("Üçüncü girilen period ikinci girilen period değerinden küçük olamaz!");
            else if (TriggerPeriod <= 0)
                throw new IndicatorException("Periyot pozitif sayı olmalıdır.");

            Code = code;
            TargetDate = targetDate;
            this.FirstPeriod = FirstPeriod;
            this.SecondPeriod = SecondPeriod;
            this.TriggerPeriod = TriggerPeriod;
            if (!mapReduce)
                CalculateMovingAverageConvergenceDivergence();
            else
                CalculateMovingAverageConvergenceDivergenceMR(Code, TargetDate);
        }

        private void CalculateMovingAverageConvergenceDivergence()
        {
            var data = IndicatorService.GetData(Code, TargetDate, "Kapanis", SecondPeriod);

            if (data.Count < SecondPeriod)
            {
                //throw new IndicatorException(IndicatorException.DATA_NOT_ENOUGH_MESSAGE);
                SecondPeriod = data.Count;
                FirstPeriod = SecondPeriod - 1;
                TriggerPeriod = FirstPeriod - 1;
            }

            calculateMACD(data);
            calculateTrigger();
        }

        private void calculateMACD(List<BsonDocument> data)
        {
            EmaFirst = MovingAverage.calculateEMA(FirstPeriod, data);
            EmaSecond = MovingAverage.calculateEMA(SecondPeriod, data);

            MacdLine = new double[EmaFirst.Length];

            for (int i = 0; i < EmaFirst.Length; i++)
            {
                MacdLine[i] = EmaFirst[i] - EmaSecond[i];
            }
        }

        private void calculateTrigger()
        {
            TriggerLine = MovingAverage.calculateEMA(TriggerPeriod, MacdLine);
        }

        private void CalculateMovingAverageConvergenceDivergenceMR(string code, DateTime targetDate)
        {
            EmaFirst = MovingAverage.ExponentialMR(code, targetDate, FirstPeriod);
            EmaSecond = MovingAverage.ExponentialMR(code, targetDate, SecondPeriod);

            MacdLine = new double[EmaFirst.Length];

            for (int i = 0; i < EmaFirst.Length; i++)
            {
                MacdLine[i] = EmaFirst[i] - EmaSecond[i];
            }

            calculateTrigger();
        }

    }
}
