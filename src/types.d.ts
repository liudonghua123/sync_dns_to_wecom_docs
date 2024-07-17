// https://stackoverflow.com/questions/42233987/how-to-configure-custom-global-interfaces-d-ts-files-for-typescript

export {};

declare global {
  interface REQ_TYPE {
    bodyRaw: string;
    body: any;
    headers: any;
    method: string;
    host: string;
    scheme: string;
    query: any;
    queryString: string;
    port: number;
    url: string;
  }

  interface RES_TYPE {
    json: (data: any, statusCode?: number, headers?: any) => void;
    send: (data: any, statusCode?: number, headers?: any) => void;
    empty: () => void;
    redirect: (url: any, statusCode?: number, headers?: any) => void;
  }

  interface LOG_TYPE {
    (message: any): void;
  }
  interface ERROR_TYPE {
    (message: any): void;
  }
}