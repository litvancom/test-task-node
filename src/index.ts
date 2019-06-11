import { Db, MongoClient, ObjectId } from 'mongodb';

const dbUser = process.env.DB_USER;
const dbPassword = process.env.DB_PASSWORD;
const url = 'mongodb://localhost:27017';
const dbName = 'data';
const logCollectionName = 'test';
const dictionaryCollectionName = 'dictionary';
const migrationCollectionName = 'dictionary';
const migrationVersion = 1;

interface LogDocument {
  _id: ObjectId
  Exchange: string | number
  Base: string | number
  Quote: string | number
  TimeStart: Date
  PriceOpen: number
  PriceHigh: number
  PriceLow: number
  PriceClose: number
  source: string | number
}

(async () => {
  const client = await MongoClient.connect(url, {
    useNewUrlParser: true,
    auth: { user: dbUser, password: dbPassword },
  });
  const db: Db = client.db(dbName);
  const logCollection = db.collection<LogDocument | any>(logCollectionName);
  const dictionaryCollection = db.collection<LogDocument | any>(dictionaryCollectionName);
  const migrationCollection = db.collection<LogDocument | any>(migrationCollectionName);

  const currentVersion = await migrationCollection.aggregate([
    {
      $group: {
        _id: '$_id',
        migrationVersion: { $max: '$migrationVersion' },
      },
    },
  ]).next();

  if (currentVersion >= migrationVersion) {
    await client.close();
    throw new Error('Migration has already been done');
  }

  const distinctFields = [
    'Exchange',
    'Base',
    'Quote',
    'source',
  ];

  // Transforming data to dictionaries
  await Promise.all(distinctFields.map(async (distinctName) => {
    const values: any[] = await logCollection.distinct(distinctName, {});
    // Persisting distict values to dictionary
    const dictionary = values.map((value, index) => ({ value, index }));
    await dictionaryCollection.insertOne({ [distinctName]: dictionary });
    return Promise.all(values.map((value, index) => {
      return logCollection.updateMany({ [distinctName]: value }, {
        $set: {
          [distinctName]: index,
        },
      });
    }));
  }));

  // Group data by one day
  const cursor = logCollection.aggregate<{ logs: LogDocument[] }>([
    {
      $group: {
        _id: {
          year: { $year: '$TimeStart' },
          month: { $month: '$TimeStart' },
          day: { $dayOfMonth: '$TimeStart' },
        },
        logs: {
          $push: {
            Exchange: '$Exchange',
            Base: '$Base',
            Quote: '$Quote',
            TimeStart: '$TimeStart',
            PriceOpen: '$PriceOpen',
            PriceHigh: '$PriceHigh',
            PriceLow: '$PriceLow',
            PriceClose: '$PriceClose',
            source: '$source',
          },
        },
      },

    },
  ], { allowDiskUse: true });

  // Write data to grouped arrays and remove old
  do {
    const item = await cursor.next();
    await logCollection.insertOne({
      items: item.logs,
    });
    await logCollection.bulkWrite(item.logs.map(value => ({ deleteOne: { _id: value._id } })));
  } while (await cursor.hasNext());

  await migrationCollection.insertOne({ migrationVersion });
  await client.close();

})().catch(e => {
  console.log(e);
});