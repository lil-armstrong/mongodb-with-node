
const { MongoClient, ServerApiVersion } = require('mongodb');

const uri = "mongodb://localhost:27017";

const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: ServerApiVersion.v1 });

client.on('close', () => {
  console.log("Connection closed")
})

async function run (db_name ="test") {
  try{
    await client.connect();
    const db = client.db(db_name);

    await db.command({ping: 1});

    console.log("Connected successfully to MongoDB server using connection string " + uri )
    return db
  }catch(err){
    await client.close()
    client.emit('close')
    console.dir(err);
    process.exit()
  }
}

run("sample_training").then(async (db)=>{
  console.log(db);
  const trips = db.collection("trips")

  // Create
  // insertOne, insertMany
  const start_time = new Date(), stop_time = new Date();
  stop_time.setSeconds(start_time.getSeconds() + 1200)

  const doc = {
    tripduration: 889,
    'start station id': 268,
    'start station name': 'Howard St & Centre St',
    'end station id': 3002,
    'end station name': 'South End Ave & Liberty St',
    bikeid: 22794,
    usertype: 'Subscriber',
    'birth year': 1961,
    gender: 2,
    'start station location': { type: 'Point', coordinates: [ -73.99973337, 40.71910537 ] },
    'end station location': { type: 'Point', coordinates: [ -74.015756, 40.711512 ] },
    'start time': start_time,
    'stop time': stop_time ,
    description:"from-node"
  }
  await trips?.insertOne(doc)

  // Update
  await trips.updateMany({description:"from-node"}, {$set: {updatedAt: new Date()}})
  // Delete
  // Retrieve
  const findCursor =  trips.find({
    description:"from-node"
  }, {
    limit: 40
  })

  const cursorArray = await findCursor?.toArray();
  console.log({
    count: cursorArray?.length,
    doc: cursorArray
  })

  // findCursor.forEach((doc) => console.dir(doc))
})


process.on("exit", async (code) => {
  console.log({code})
  client.close(true, () => {
    console.log(`Connection closed for db: ${client?.db?.databaseName}`)
  })
  client.emit('close')
});
