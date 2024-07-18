/// reference path="./types.d.ts"

import { Client, Databases, ID } from "node-appwrite";
import process from "process";
import { init, shared_rrs_get } from "zdns";
import { RESOURCE } from "zdns/lib/types";
import { SmartSheet } from "wecom-wedoc";

const { APPWRITE_ENDPOINT, APPWRITE_FUNCTION_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID, APPWRITE_COLLECTION_ID, ZDNS_API_HOST, ZDNS_API_USERNAME, ZDNS_API_PASSWORD, CORP_ID, SECRET, DOC_ID, SHEET_ID } = process.env;

if (!APPWRITE_ENDPOINT || !APPWRITE_FUNCTION_PROJECT_ID || !APPWRITE_DATABASE_ID || !APPWRITE_COLLECTION_ID || !ZDNS_API_HOST || !ZDNS_API_USERNAME || !ZDNS_API_PASSWORD || !CORP_ID || !SECRET || !DOC_ID || !SHEET_ID) {
  console.info(`appwrite envs not configured!`);
  process.exit(-1);
}

// options for wecom-wedoc
const option = {
  corpId: CORP_ID,
  secret: SECRET,
};


// define global logging functions, default to console.log and console.error, will be overridden in the entrypoint function
let log = console.log;
let error = console.error;

/**
 * initialize the appwrite client
 * @returns appwrite client
 */
function init_appwrite_client() {
  const client = new Client()
    .setEndpoint(APPWRITE_ENDPOINT)
    .setProject(APPWRITE_FUNCTION_PROJECT_ID);
  // setKey is optional if you deploy the function and run it in the Appwrite instance.
  if (process.env.NODE_ENV === 'development') {
    if (!APPWRITE_API_KEY) {
      throw new Error('APPWRITE_API_KEY is required in development mode');
    }
    client.setKey(APPWRITE_API_KEY!);
  }
  return client;
}

/**
 * insert a log into the appwrite database
 * @param client appwrite client
 * @param param1 the log data
 */
async function insert_log(client: Client, { start_datetime, end_datetime, message }: { start_datetime: number, end_datetime: number, message: string }) {
  const databases = new Databases(client);
  const document = await databases.createDocument(
    APPWRITE_DATABASE_ID!,
    APPWRITE_COLLECTION_ID!,
    ID.unique(),
    {
      start_datetime,
      end_datetime,
      message,
    }
  );
  log(`inserted log: ${JSON.stringify(document)}`);
}

/**
 * get dns records from zdns api
 * @returns dns records
 */
async function get_dns_records() {
  init({ app_username: ZDNS_API_USERNAME!, app_password: ZDNS_API_PASSWORD!, base_url: ZDNS_API_HOST! });
  const response = await shared_rrs_get({});
  log(`fetched ${response.total_size} dns records`);
  return response.resources;
}

/**
 * process dns records
 * @param dns_records dns records
 * @returns the processed dns records, merged by name, with rdata joined by ','
 */
function process_dns_records(dns_records: RESOURCE[]) {
  // dns_records is an array of objects like:
  // {"name":"minos.ynu.edu.cn.","type":"CNAME","klass":"IN","ttl":3600,"rdata":"webexp.ynu.edu.cn.","reverse_name":"cn.edu.ynu.minos","is_enable":"yes","row_id":912,"comment":"","audit_status":"","expire_time":"","expire_style":"","create_time":"2024-07-15","expire_is_enable":"no","id":"CNAME$912"}
  // filter out the records that are not enabled
  const enabled_records = dns_records.filter((record) => record.is_enable === 'yes');
  log(`filtered out ${dns_records.length - enabled_records.length} disabled records`);
  // filter out the records that are not cname, a or aaaa
  const specified_type_records = enabled_records.filter((record) => ['CNAME', 'A', 'AAAA'].includes(record.type));
  log(`filtered out ${enabled_records.length - specified_type_records.length} non-CNAME, A or AAAA records`);
  // map only name without suffix dot and rdata
  const simple_record_data = specified_type_records.map((record) => ({ name: record.name.substring(0, record.name.length - 1), rdata: record.rdata }));
  // merge rdata join with ',' when the name is the same
  const records_data: { name: string, rdata: string }[] = []
  for (const record of simple_record_data) {
    const existing_record = records_data.find((r) => r.name === record.name);
    if (existing_record) {
      log(`merging ${record.name} ${record.rdata} with ${existing_record.rdata}`);
      existing_record.rdata = `${existing_record.rdata},${record.rdata}`;
    } else {
      records_data.push(record);
    }
  }
  log(`merged ${simple_record_data.length} records into ${records_data.length} records`);
  return records_data;
}

/**
 * process wedocs according to the dns records
 * @param dns_records_data the dns records data
 * @returns the statistics of the process
 */
async function process_wedocs(dns_records_data: { name: string; rdata: string; }[]) {
  // gather the statistics
  const statistics = { added: 0, updated: 0, deleted: 0 };
  const { FIELD_DNS_NAME = '信息系统域名', FIELD_DNS_RDATA = '域名解析记录', REMOVE_IRRELEVANT_RECORDS = 'false' } = process.env
  const remove_irrelevant_records = REMOVE_IRRELEVANT_RECORDS === 'true' || true;
  // query the existed sheet records
  let res = await SmartSheet.Record.records({ docid: DOC_ID!, sheet_id: SHEET_ID! }, option);
  log(`fetched ${res.length} records of sheet ${SHEET_ID} in doc ${DOC_ID}`);
  // use field FIELD_DNS_NAME as the key to update/insert records
  const record_name_id_mappings = Object.fromEntries(
    res.map((record: any) => [record.values[FIELD_DNS_NAME][0].text, record.record_id])
  );
  // for SmartSheet.Record.del
  const record_id_name_mappings = Object.fromEntries(
    Object.entries(record_name_id_mappings).map(([name, record_id]) => [record_id, name])
  );
  // calculate the records to insert
  const records_to_insert = dns_records_data.filter(({ name }) => !(name in record_name_id_mappings));
  log(`found ${records_to_insert.length} records to insert`);
  let records = records_to_insert.map(({ name, rdata }) => ({
    record_id: name,
    values: {
      [FIELD_DNS_NAME]: [{ type: 'text', text: name }],
      [FIELD_DNS_RDATA]: [{ type: 'text', text: rdata }],
    }
  }));
  // @ts-ignore
  res = await SmartSheet.Record.add({ docid: DOC_ID!, sheet_id: SHEET_ID!, records }, option);
  // @ts-ignore, res is always the original response
  log(`inserted ${res.records.length} records`);
  // @ts-ignore
  statistics.added = res.records.length;
  // calculate the records to update
  const records_to_update = dns_records_data.filter(({ name }) => name in record_name_id_mappings);
  log(`found ${records_to_update.length} records to update`);
  records = records_to_update.map(({ name, rdata }) => ({
    record_id: record_name_id_mappings[name],
    values: {
      [FIELD_DNS_NAME]: [{ type: 'text', text: name }],
      [FIELD_DNS_RDATA]: [{ type: 'text', text: rdata }],
    }
  }));
  // @ts-ignore
  res = await SmartSheet.Record.update({ docid: DOC_ID!, sheet_id: SHEET_ID!, records }, option);
  log(`updated ${res.length} records`);
  statistics.updated = res.length;
  if (remove_irrelevant_records) {
    // calculate the records to delete
    const dns_record_names = dns_records_data.map(({ name }) => name);
    const records_to_delete = Object.keys(record_name_id_mappings).filter((name) => !dns_record_names.includes(name));
    log(`found ${records_to_delete.length} records to delete`);
    res = await SmartSheet.Record.del({ docid: DOC_ID!, sheet_id: SHEET_ID!, record_ids: records_to_delete.map((name) => record_name_id_mappings[name]) }, option);
    log(`deleted ${records_to_delete.length} records`);
    statistics.deleted = records_to_delete.length;
  }
  return statistics;
}

export default async ({ req, res, log: _log, error: _error }: { req: REQ_TYPE; res: RES_TYPE; log: LOG_TYPE; error: ERROR_TYPE }) => {
  // override the default log and error functions
  log = _log;
  error = _error;
  const start_datetime = Date.now();
  const client = init_appwrite_client();

  // fetch dns records
  const dns_records = await get_dns_records();
  const records_data = process_dns_records(dns_records);

  // process wedocs according to the dns records
  const statistics = await process_wedocs(records_data);
  log(`processed wedocs with statistics: ${JSON.stringify(statistics)}`);

  // insert into the statistics log into the appwrite database
  // TODO: need to fix: AppwriteException: Collection with the requested ID could not be found.
  // await insert_log(client, { start_datetime, end_datetime: Date.now(), message: JSON.stringify(statistics) });

  return res.empty();
};
