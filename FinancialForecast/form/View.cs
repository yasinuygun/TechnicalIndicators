using System;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;
using TechnicalIndicators.indicators;
using MongoDB.Bson;

namespace TechnicalIndicators.form
{
    public partial class View : Form
    {
        private string Code;
        private DateTime TargetDate;
        private bool MapReduceAllowed;

        private int MAVPeriod; // MAV Parameter
        private int FirstPeriod, SecondPeriod, TriggerPeriod; // MACD Parameters
        private int RSIPeriod; // RSI Parameter
        private int FastKPeriod, FastDPeriod, SlowDPeriod; // Stochastics Parameters
        private int WilliamsRPeriod; // Williams' %R Parameter

        public View()
        {
            InitializeComponent();
        }

        private void View_Load(object sender, EventArgs e)
        {
            this.Location = new Point(20, 20);
            this.FormBorderStyle = FormBorderStyle.FixedSingle;
            this.MaximizeBox = false;

            codeSelectionPanel.BringToFront();

            /* Load codes to the combobox. */
            codeSelectionComboBox.Items.AddRange(IndicatorService.GetCodeList().Select(p => p.GetElement(0).Value.ToString()).ToArray());
            codeSelectionComboBox.SelectedIndex = 0;

            indicatorSelectionComboBox.SelectedIndex = 0;
        }

        private void codeSelectionNext_Click(object sender, EventArgs e)
        {
            Code = codeSelectionComboBox.SelectedItem.ToString();

            dateSelectionTitle.Text = Code + " için hangi tarih en son gün olarak seçilsin?";

            var filter = new BsonDocument("Kod", Code);
            var sort = new BsonDocument("Tarih", -1);
            var projection = new BsonDocument { { "_id", 0 }, { "Tarih", 1 } };

            var endDate = MongoDBService.GetService().FindOneSortProject(filter, sort, projection).GetElement(0).Value.ToLocalTime();
            monthCalendar.SetDate(endDate);

            dateSelectionPanel.BringToFront();
        }

        private void dateSelectionBack_Click(object sender, EventArgs e)
        {
            codeSelectionPanel.BringToFront();
        }

        private void dateSelectionNext_Click(object sender, EventArgs e)
        {
            TargetDate = monthCalendar.SelectionEnd;

            indicatorSelectionTitle.Text = Code + " üzerinde hesap yapılacak indikatörü seçiniz.";
            indicatorSelectionPanel.BringToFront();
        }

        private void indicatorSelectionBack_Click(object sender, EventArgs e)
        {
            dateSelectionPanel.BringToFront();
        }

        private void indicatorSelectionNext_Click(object sender, EventArgs e)
        {
            /*
                Order of indicators in the combobox.
                0- Moving Average
                1- Moving Average Convergence Divergence
                2- Relative Strength Index
                3- Stochastics
                4- Williams' %R
            */

            MapReduceAllowed = mapReduceCheck.Checked;

            switch (indicatorSelectionComboBox.SelectedIndex)
            {
                case 0:
                    mavParamaterTitle.Text = Code + " için Moving Average periyodunu giriniz.";
                    mavPeriod.Maximum = IndicatorService.DataCount(Code, TargetDate);
                    mavPeriod.Value = Math.Min(15, mavPeriod.Maximum);
                    mavParameterPanel.BringToFront();
                    break;
                case 1:
                    macdParameterTitle.Text = Code + " için Moving Average Convergence Divergence parametrelerini giriniz.";
                    secondPeriod.Maximum = IndicatorService.DataCount(Code, TargetDate);
                    firstPeriod.Maximum = secondPeriod.Maximum - 1;
                    triggerPeriod.Maximum = firstPeriod.Maximum - 1;
                    firstPeriod.Value = Math.Min(12, firstPeriod.Maximum);
                    secondPeriod.Value = Math.Min(26, secondPeriod.Maximum);
                    triggerPeriod.Value = Math.Min(9, triggerPeriod.Maximum);
                    macdParameterPanel.BringToFront();
                    break;
                case 2:
                    rsiParameterTitle.Text = Code + " için Relative Strength Index periyodunu giriniz.";
                    rsiPeriod.Maximum = IndicatorService.DataCount(Code, TargetDate);
                    rsiPeriod.Value = Math.Min(15, rsiPeriod.Maximum);
                    rsiParameterPanel.BringToFront();
                    break;
                case 3:
                    stochasticsParameterTitle.Text = Code + " için Stochastics Index parametrelerini giriniz.";
                    fastKPeriod.Maximum = IndicatorService.DataCount(Code, TargetDate);
                    fastKPeriod.Value = Math.Min(14, fastKPeriod.Maximum);
                    fastDPeriod.Maximum = fastKPeriod.Maximum - 1;
                    fastDPeriod.Value = Math.Min(3, fastDPeriod.Maximum);
                    slowDPeriod.Maximum = fastDPeriod.Maximum - 1;
                    slowDPeriod.Value = Math.Min(3, slowDPeriod.Maximum);
                    stochasticsParameterPanel.BringToFront();
                    break;
                case 4:
                    williamsRParameterTitle.Text = Code + " için Williams' %R periyodunu giriniz.";
                    williamsRPeriod.Maximum = IndicatorService.DataCount(Code, TargetDate);
                    williamsRPeriod.Value = Math.Min(15, williamsRPeriod.Maximum);
                    williamsRParameterPanel.BringToFront();
                    break;
            }
        }

        private void mavParameterBack_Click(object sender, EventArgs e)
        {
            indicatorSelectionPanel.BringToFront();
        }

        private void mavParameterNext_Click(object sender, EventArgs e)
        {
            mavResultTitle.Text = Code + " için Moving Average sonucu";
            MAVPeriod = (int)mavPeriod.Value;

            mavResultChart.Series.Clear();

            var closeSeries = new Series
            {
                Name = "Kapanış",
                Color = System.Drawing.Color.Black,
                ChartType = SeriesChartType.Line
            };
            var smaSeries = new Series
            {
                Name = "Simple Moving Average",
                Color = System.Drawing.Color.Red,
                ChartType = SeriesChartType.Line
            };
            var wmaSeries = new Series
            {
                Name = "Weighted Moving Average",
                Color = System.Drawing.Color.Green,
                ChartType = SeriesChartType.Line
            };
            var emaSeries = new Series
            {
                Name = "Exponential Moving Average",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            var closeData = IndicatorService.GetData(Code, TargetDate, new string[] { "Tarih", "Kapanis" }, MAVPeriod);
            double[] smaData, wmaData, emaData;
            if (MapReduceAllowed)
            {
                smaData = MovingAverage.SimpleMR(Code, TargetDate, MAVPeriod, MAVPeriod);
                wmaData = MovingAverage.WeightedMR(Code, TargetDate, MAVPeriod, MAVPeriod);
                emaData = MovingAverage.ExponentialMR(Code, TargetDate, MAVPeriod, MAVPeriod);
            }
            else
            {
                smaData = MovingAverage.Simple(Code, TargetDate, MAVPeriod, MAVPeriod);
                wmaData = MovingAverage.Weighted(Code, TargetDate, MAVPeriod, MAVPeriod);
                emaData = MovingAverage.Exponential(Code, TargetDate, MAVPeriod, MAVPeriod);
            }

            DateTime date;
            for (int i = 0; i < smaData.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();

                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
                smaSeries.Points.AddXY(date, smaData[i]);
                wmaSeries.Points.AddXY(date, wmaData[i]);
                emaSeries.Points.AddXY(date, emaData[i]);
            }

            mavResultChart.ChartAreas[0].AxisY.Minimum = Math.Floor(new double[] { closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Min(), smaData.Min(), wmaData.Min(), emaData.Min() }.Min());
            mavResultChart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(new double[] { closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Max(), smaData.Max(), wmaData.Max(), emaData.Max() }.Max());

            mavResultChart.Series.Add(closeSeries);
            mavResultChart.Series.Add(smaSeries);
            mavResultChart.Series.Add(wmaSeries);
            mavResultChart.Series.Add(emaSeries);

            mavResultChart.Invalidate();

            mavResultPanel.BringToFront();
        }

        private void mavResultBack_Click(object sender, EventArgs e)
        {
            mavParameterPanel.BringToFront();
        }

        private void macdParameterBack_Click(object sender, EventArgs e)
        {
            indicatorSelectionPanel.BringToFront();
        }

        private void macdParameterNext_Click(object sender, EventArgs e)
        {
            macdResultTitle.Text = Code + " için Moving Average Convergence Divergence sonucu";
            FirstPeriod = (int)firstPeriod.Value;
            SecondPeriod = (int)secondPeriod.Value;
            TriggerPeriod = (int)triggerPeriod.Value;

            closeEma1Ema2Chart.Series.Clear();
            macdLineChart.Series.Clear();
            triggerLineChart.Series.Clear();

            var closeSeries = new Series
            {
                Name = "Kapanış",
                Color = System.Drawing.Color.Black,
                ChartType = SeriesChartType.Line
            };

            var emaFirstSeries = new Series
            {
                Name = "EMA Kısa Period",
                Color = System.Drawing.Color.Red,
                ChartType = SeriesChartType.Line
            };

            var emaSecondSeries = new Series
            {
                Name = "EMA Uzun Period",
                Color = System.Drawing.Color.Green,
                ChartType = SeriesChartType.Line
            };

            var macdLineSeries = new Series
            {
                Name = "MACD",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            var triggerLineSeries = new Series
            {
                Name = "Trigger",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            closeEma1Ema2Chart.Series.Add(closeSeries);
            closeEma1Ema2Chart.Series.Add(emaFirstSeries);
            closeEma1Ema2Chart.Series.Add(emaSecondSeries);

            macdLineChart.Series.Add(macdLineSeries);

            triggerLineChart.Series.Add(triggerLineSeries);

            var closeData = IndicatorService.GetData(Code, TargetDate, new string[] { "Tarih", "Kapanis" }, 30);

            MovingAverageConvergenceDivergence macd = new MovingAverageConvergenceDivergence(Code, TargetDate, FirstPeriod, SecondPeriod, TriggerPeriod, 30, MapReduceAllowed);
            double[] emaFirst = macd.EmaFirst;
            double[] emaSecond = macd.EmaSecond;
            double[] macdLine = macd.MacdLine;
            double[] triggerLine = macd.TriggerLine;

            DateTime date;
            int i;
            for (i = 0; i < triggerLine.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());

                emaFirstSeries.Points.AddXY(date, emaFirst[i]);
                emaSecondSeries.Points.AddXY(date, emaSecond[i]);
                macdLineSeries.Points.AddXY(date, macdLine[i]);
                triggerLineSeries.Points.AddXY(date, triggerLine[i]);
            }
            for (; i < emaFirst.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());

                emaFirstSeries.Points.AddXY(date, emaFirst[i]);
                emaSecondSeries.Points.AddXY(date, emaSecond[i]);
                macdLineSeries.Points.AddXY(date, macdLine[i]);
            }

            closeEma1Ema2Chart.ChartAreas[0].AxisY.Minimum = Math.Floor(new double[] { closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Min(), emaFirst.Min(), emaSecond.Min() }.Min());
            closeEma1Ema2Chart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(new double[] { closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Max(), emaFirst.Max(), emaSecond.Max() }.Max());

            macdLineChart.ChartAreas[0].AxisY.Minimum = Math.Floor(macdLine.Min());
            macdLineChart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(macdLine.Max());

            triggerLineChart.ChartAreas[0].AxisY.Minimum = Math.Floor(triggerLine.Min());
            triggerLineChart.ChartAreas[0].AxisY.Maximum = Math.Floor(triggerLine.Max());


            closeEma1Ema2Chart.Invalidate();
            macdLineChart.Invalidate();
            triggerLineChart.Invalidate();

            macdResultPanel.BringToFront();
        }

        private void macdResultBack_Click(object sender, EventArgs e)
        {
            macdParameterPanel.BringToFront();
        }

        private void rsiParameterBack_Click(object sender, EventArgs e)
        {
            indicatorSelectionPanel.BringToFront();
        }

        private void rsiParameterNext_Click(object sender, EventArgs e)
        {
            rsiResultTitle.Text = Code + " için Relative Strength Index sonucu";
            RSIPeriod = (int)rsiPeriod.Value;

            rsiDataResultChart.Series.Clear();
            rsiIndicatorResultChart.Series.Clear();

            var closeSeries = new Series
            {
                Name = "Kapanış",
                Color = System.Drawing.Color.Black,
                ChartType = SeriesChartType.Line
            };

            var rsiSeries = new Series
            {
                Name = "Relative Strength Index",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            rsiDataResultChart.Series.Add(closeSeries);
            rsiIndicatorResultChart.Series.Add(rsiSeries);

            var closeData = IndicatorService.GetData(Code, TargetDate, new string[] { "Tarih", "Kapanis" }, RSIPeriod);
            double[] rsi;
            if (MapReduceAllowed)
                rsi = RelativeStrengthIndex.RsiMR(Code, TargetDate, RSIPeriod, RSIPeriod);
            else
                rsi = RelativeStrengthIndex.Rsi(Code, TargetDate, RSIPeriod, RSIPeriod);

            DateTime date;
            for (int i = 0; i < rsi.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
                rsiSeries.Points.AddXY(date, rsi[i]);
            }

            rsiDataResultChart.ChartAreas[0].AxisY.Minimum = Math.Floor(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Min());
            rsiDataResultChart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Max());

            rsiIndicatorResultChart.ChartAreas[0].AxisY.Minimum = 0;
            rsiIndicatorResultChart.ChartAreas[0].AxisY.Maximum = 100;

            rsiDataResultChart.Invalidate();
            rsiIndicatorResultChart.Invalidate();

            rsiResultPanel.BringToFront();
        }

        private void rsiResultBack_Click(object sender, EventArgs e)
        {
            rsiParameterPanel.BringToFront();
        }

        private void stochasticsParameterBack_Click(object sender, EventArgs e)
        {
            indicatorSelectionPanel.BringToFront();
        }

        private void stochasticsParameterNext_Click(object sender, EventArgs e)
        {
            stochasticsResultTitle.Text = Code + " için Stochastics sonucu";
            FastKPeriod = (int)fastKPeriod.Value;
            FastDPeriod = (int)fastDPeriod.Value;
            SlowDPeriod = (int)slowDPeriod.Value;

            stochasticsCloseChart.Series.Clear();
            stochasticsIndicatorChart.Series.Clear();

            var closeSeries = new Series
            {
                Name = "Kapanış",
                Color = System.Drawing.Color.Black,
                ChartType = SeriesChartType.Line
            };

            stochasticsCloseChart.Series.Add(closeSeries);

            var fastKSeries = new Series
            {
                Name = "Fast K",
                Color = System.Drawing.Color.Red,
                ChartType = SeriesChartType.Line
            };

            var fastDSeries = new Series
            {
                Name = "Fast D",
                Color = System.Drawing.Color.Green,
                ChartType = SeriesChartType.Line
            };

            var slowDSeries = new Series
            {
                Name = "Slow D",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            stochasticsIndicatorChart.Series.Add(fastKSeries);
            stochasticsIndicatorChart.Series.Add(fastDSeries);
            stochasticsIndicatorChart.Series.Add(slowDSeries);

            var closeData = IndicatorService.GetData(Code, TargetDate, new string[] { "Tarih", "Kapanis" }, RSIPeriod);

            Stochastics stochastics;
            if (MapReduceAllowed)
                stochastics = new Stochastics(Code, TargetDate, FastKPeriod, FastDPeriod, SlowDPeriod, FastKPeriod, true);
            else
                stochastics = new Stochastics(Code, TargetDate, FastKPeriod, FastDPeriod, SlowDPeriod, FastKPeriod, false);

            int i;
            DateTime date;
            for (i = 0; i < stochastics.SlowD.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                fastKSeries.Points.AddXY(date, stochastics.FastK[i]);
                fastDSeries.Points.AddXY(date, stochastics.FastD[i]);
                slowDSeries.Points.AddXY(date, stochastics.SlowD[i]);
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
            }
            for (; i < stochastics.FastD.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                fastKSeries.Points.AddXY(date, stochastics.FastK[i]);
                fastDSeries.Points.AddXY(date, stochastics.FastD[i]);
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
            }
            for (; i < stochastics.FastK.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                fastKSeries.Points.AddXY(date, stochastics.FastK[i]);
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
            }

            stochasticsCloseChart.ChartAreas[0].AxisY.Minimum = Math.Floor(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Min());
            stochasticsCloseChart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Max());

            stochasticsIndicatorChart.ChartAreas[0].AxisY.Minimum = 0;
            stochasticsIndicatorChart.ChartAreas[0].AxisY.Maximum = 100;

            stochasticsCloseChart.Invalidate();
            stochasticsIndicatorChart.Invalidate();

            stochasticsResultPanel.BringToFront();
        }

        private void stochasticsResultBack_Click(object sender, EventArgs e)
        {
            stochasticsParameterPanel.BringToFront();
        }

        private void williamsRParameterBack_Click(object sender, EventArgs e)
        {
            indicatorSelectionPanel.BringToFront();
        }

        private void williamsRParameterNext_Click(object sender, EventArgs e)
        {
            williamsRResultTitle.Text = Code + " için Williams' %R sonucu";
            WilliamsRPeriod = (int)williamsRPeriod.Value;

            williamsRCloseChart.Series.Clear();
            williamsRIndicatorChart.Series.Clear();

            var closeSeries = new Series
            {
                Name = "Kapanış",
                Color = System.Drawing.Color.Black,
                ChartType = SeriesChartType.Line
            };

            var wsrSeries = new Series
            {
                Name = "Williams' R",
                Color = System.Drawing.Color.Blue,
                ChartType = SeriesChartType.Line
            };

            williamsRCloseChart.Series.Add(closeSeries);
            williamsRIndicatorChart.Series.Add(wsrSeries);

            var closeData = IndicatorService.GetData(Code, TargetDate, new string[] { "Tarih", "Kapanis" }, RSIPeriod);
            DateTime date;
            double[] wsr;

            if (MapReduceAllowed)
                wsr = WilliamsR.WsrMR(Code, TargetDate, WilliamsRPeriod, WilliamsRPeriod);
            else
                wsr = WilliamsR.Wsr(Code, TargetDate, WilliamsRPeriod, WilliamsRPeriod);


            for (int i = 0; i < wsr.Length; i++)
            {
                var close = closeData.ElementAt(i);
                date = close.GetElement("Tarih").Value.AsBsonDateTime.ToLocalTime();
                closeSeries.Points.AddXY(date, close.GetElement("Kapanis").Value.ToDouble());
                wsrSeries.Points.AddXY(date, wsr[i]);
            }

            williamsRCloseChart.ChartAreas[0].AxisY.Minimum = Math.Floor(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Min());
            williamsRCloseChart.ChartAreas[0].AxisY.Maximum = Math.Ceiling(closeData.Select(p => p.GetElement("Kapanis").Value.ToDouble()).Max());

            williamsRIndicatorChart.ChartAreas[0].AxisY.Minimum = -100;
            williamsRIndicatorChart.ChartAreas[0].AxisY.Maximum = 0;

            williamsRCloseChart.Invalidate();
            williamsRIndicatorChart.Invalidate();

            williamsRResultPanel.BringToFront();
        }

        private void williamsRResultBack_Click(object sender, EventArgs e)
        {
            williamsRParameterPanel.BringToFront();
        }
    }
}
