import main from './src/main.js';
// @ts-ignore
import process from 'process';

process.env.NODE_ENV = 'development';

// For local development you can use the following start entrypoint

// see also https://github.com/open-runtimes/open-runtimes/blob/2c4e95e3cee1fe4254910329333ea1ace52fdf08/runtimes/node-18.0/src/server.js#L73-L121
// see also https://appwrite.io/docs/products/functions/develop#request
const req = {
  bodyRaw: '',
  body: {},
  headers: {},
  method: 'GET',
  host: 'localhost',
  scheme: 'http',
  query: {},
  queryString: '',
  port: 80,
  url: 'http://localhost/',
  path: '/',
};
// see also https://appwrite.io/docs/products/functions/develop#response
const res = {
  json: (data: any, statusCode = 200, headers = {}) => console.log(data, statusCode, headers),
  send: (data: any, statusCode = 200, headers = {}) => console.log(data, statusCode, headers),
  empty: () => {
    console.log('', 204, {});
  },
  redirect: (url: any, statusCode = 301, headers = {}) => {
    headers['location'] = url;
    console.log('', statusCode, headers);
  },
};

// invoke the main function of the cloud function
main({ req, res, log: console.log, error: console.error });
