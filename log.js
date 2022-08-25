
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

async function createCappedCollection (db, collectionName, options){
  return await db.createCollection(collectionName,options)
}

run("test").then(async (db)=>{
  //
  const collectionName = "robot_logs"
  const collections = (await client.db().listCollections().toArray()).map( col => col.name);
  let collection = db.collection(collectionName);
  console.log({collections});

  if(!(collections.includes(collectionName))){
    collection = await createCappedCollection(db, collectionName,  {
      capped: true,
      size: 100000,
      max: 10
    })
  }

  const isCapped = await collection.isCapped();

  let count = 0;

  // Function to store event in DB
  function logEvent(number){
    collection.insertOne({v: number, createdAt: new Date()})
  }

  // Read events from DB
  async function getEvents () {
    return await collection.find().limit(10).toArray()
  }

  // Store events every one seconds
  const storeLog = setInterval(()=>{
    logEvent(count++)
  }, 1000)

  // Read the events every 1.5 seconds
  const readLogs =   setInterval(async ()=>{
    console.log(await getEvents())
  }, 1500)

  // Stop storing after 10 seconds
  setTimeout(()=>{
    clearInterval(storeLog)
    clearInterval(readLogs)
  }, 10000)


})

process.on("exit", async (code) => {
  console.log({code})
  client.close(true, () => {
    console.log(`Connection closed for db: ${client?.db?.databaseName}`)
  })
  client.emit('close')
});
