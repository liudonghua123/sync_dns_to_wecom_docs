import { Client, Databases } from "node-appwrite";
import process from "process";

const { APPWRITE_ENDPOINT, APPWRITE_FUNCTION_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID, APPWRITE_COLLECTION_ID } = process.env;

if (!APPWRITE_ENDPOINT || !APPWRITE_FUNCTION_PROJECT_ID || !APPWRITE_DATABASE_ID || !APPWRITE_COLLECTION_ID) {
  console.info(`appwrite envs not configured!`);
  process.exit(-1);
}

async function list_documents(client: Client, log) {
  const databases = new Databases(client);
  const { total, documents } = await databases.listDocuments(
    APPWRITE_DATABASE_ID!,
    APPWRITE_COLLECTION_ID!
  );
  log(`Found ${total} documents in the collection`);
}

// This is your Appwrite function
// It's executed each time we get a request
export default async ({ req, res, log, error }: { req: REQ_TYPE; res: RES_TYPE; log: LOG_TYPE; error: ERROR_TYPE }) => {
  // Why not try the Appwrite SDK?
  //
  const client = new Client()
    .setEndpoint(APPWRITE_ENDPOINT)
    .setProject(APPWRITE_FUNCTION_PROJECT_ID)

  // // setKey is optional if you deploy the function and run it in the Appwrite instance.
  if (process.env.NODE_ENV === 'development') {
    if (!APPWRITE_API_KEY) {
      throw new Error('APPWRITE_API_KEY is required in development mode');
    }
    client.setKey(APPWRITE_API_KEY!);
  }

  await list_documents(client, log);

  // You can log messages to the console
  log(`Bun.version: ${Bun.version}`);

  // If something goes wrong, log an error
  error("Hello, Errors!");

  // The `req` object contains the request data
  if (req.method === "GET") {
    // Send a response with the res object helpers
    // `res.send()` dispatches a string back to the client
    return res.send("Hello, World!");
  }

  // `res.json()` is a handy helper for sending JSON
  return res.json({
    motto: "Build like a team of hundreds_",
    learn: "https://appwrite.io/docs",
    connect: "https://appwrite.io/discord",
    getInspired: "https://builtwith.appwrite.io",
  });
};
