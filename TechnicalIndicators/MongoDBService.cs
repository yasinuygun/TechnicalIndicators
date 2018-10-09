using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TechnicalIndicators
{
    class MongoDBService
    {
        private static MongoDBService Service = null;

        private IMongoCollection<BsonDocument> collection;

        private MongoDBService(string connectionString, string databaseName, string collectionName)
        {
            try
            {
                collection = new MongoClient(connectionString).GetDatabase(databaseName).GetCollection<BsonDocument>(collectionName);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
        }

        public static void InitiateService(string connectionString, string databaseName, string collectionName)
        {
            if (Service == null)
                Service = new MongoDBService(connectionString, databaseName, collectionName);
        }

        public static MongoDBService GetService()
        {
            return Service;
        }

        public IMongoCollection<BsonDocument> GetCollection()
        {
            return collection;
        }

        public BsonDocument FindOne(BsonDocument filter)
        {
            BsonDocument result = null;
            try
            {
                result = collection.Find(filter).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public BsonDocument FindOneSort(BsonDocument filter, BsonDocument sort)
        {
            BsonDocument result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public BsonDocument FindOneProject(BsonDocument filter, BsonDocument projection)
        {
            BsonDocument result = null;
            try
            {
                result = collection.Find(filter).Project(projection).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public BsonDocument FindOneSortProject(BsonDocument filter, BsonDocument sort, BsonDocument projection)
        {
            BsonDocument result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Project(projection).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public BsonDocument FindOneSortProjectSkip(BsonDocument filter, BsonDocument sort, BsonDocument projection, int skip)
        {
            BsonDocument result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Project(projection).Skip(skip).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindMany(BsonDocument filter)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySort(BsonDocument filter, BsonDocument sort)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManyProject(BsonDocument filter, BsonDocument projection)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Project(projection).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySortProject(BsonDocument filter, BsonDocument sort, BsonDocument projection)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Project(projection).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySortLimit(BsonDocument filter, BsonDocument sort, int limit)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Limit(limit).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySortProjectSkip(BsonDocument filter, BsonDocument sort, BsonDocument projection, int skip)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Project(projection).Skip(skip).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySortProjectLimit(BsonDocument filter, BsonDocument sort, BsonDocument projection, int limit)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Limit(limit).Project(projection).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> FindManySortProjectSkipLimit(BsonDocument filter, BsonDocument sort, BsonDocument projection, int skip, int limit)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.Find(filter).Sort(sort).Limit(limit).Project(projection).Skip(skip).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public BsonDocument MapReduceOne(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<BsonDocument, BsonDocument> options)
        {
            BsonDocument result = null;
            try
            {
                result = collection.MapReduce(map, reduce, options).FirstOrDefault();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public List<BsonDocument> MapReduceMany(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<BsonDocument, BsonDocument> options)
        {
            List<BsonDocument> result = null;
            try
            {
                result = collection.MapReduce(map, reduce, options).ToList();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public int Count(BsonDocument filter)
        {
            int result = 0;
            try
            {
                result = (int)collection.Find(filter).CountDocuments();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }

        public int Count(BsonDocument filter, int skip)
        {
            int result = 0;
            try
            {
                result = (int)collection.Find(filter).Skip(skip).CountDocuments();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Thread.Sleep(5000);
                Environment.Exit(1);
            }
            return result;
        }
    }
}
