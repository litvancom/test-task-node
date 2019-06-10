import { Db, MongoClient, ObjectId } from 'mongodb';
import { Int32 } from 'bson';

const dbUser = process.env.DB_USER;
const dbPassword = process.env.DB_PASSWORD;
const url = 'mongodb://localhost:27017';
const dbName = 'data';
const logCollectionName = 'test';

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

  const distinctFields = [
    'Exchange',
    'Base',
    'Quote',
    'source',
  ];

  // Transforming data to dictionaries
  await Promise.all(distinctFields.map(async (distinctName) => {
    const values: any[] = await logCollection.distinct(distinctName, {});
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
      items: item.logs.map((value: any) => {
        value.TimeStart = new Int32(value.TimeStart.getSeconds());
        return value;
      }),
    });
    await logCollection.bulkWrite(item.logs.map(value => ({ deleteOne: { _id: value._id } })));
  } while (await cursor.hasNext());

  await client.close();

})().catch(e => {
  console.log(e);
});