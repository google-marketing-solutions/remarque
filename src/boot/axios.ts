/*
 Copyright 2023-2025 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import { boot } from 'quasar/wrappers';
import axios, { AxiosError, AxiosInstance, AxiosRequestConfig } from 'axios';
import { Loading, Dialog, DialogChainObject } from 'quasar';
import { assertIsError } from '../helpers/utils';

declare module '@vue/runtime-core' {
  interface ComponentCustomProperties {
    $axios: AxiosInstance;
  }
}

// Be careful when using SSR for cross-request state pollution
// due to creating a Singleton instance here;
// If any client changes this (global) instance, it might be a
// good idea to move this instance creation inside of the
// "export default () => {}" function below (which runs individually
// for each client)
const api = axios.create({ baseURL: '/' });

export default boot(({ app }) => {
  // for use inside Vue files (Options API) through this.$axios and this.$api

  app.config.globalProperties.$axios = axios;
  // ^ ^ ^ this will allow you to use this.$axios (for Vue Options API form)
  //       so you won't necessarily have to import axios in each vue file

  app.config.globalProperties.$api = api;
  // ^ ^ ^ this will allow you to use this.$api (for Vue Options API form)
  //       so you can easily perform requests against your app's API
});

export class ServerError extends Error {
  debugInfo?: string;
  type?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  error?: any;
}

let activeTarget = '';

/**
 * Set the active target globally in the app
 * (name of currently active configuration).
 * @param target a target name
 */
function setActiveTarget(target: string | undefined) {
  activeTarget = target || '';
}

/**
 * Add a 'api' prefix and 'target' query argument with active target into an url.
 * @param url a base url
 * @returns
 */
function getUrl(url: string) {
  if (activeTarget) {
    if (url.includes('?')) {
      url += '&';
    } else {
      url += '?';
    }
    url += 'target=' + activeTarget;
  }
  return '/api/' + url;
}

function handleServerError(e: unknown) {
  if (e instanceof AxiosError) {
    if (e.response?.data) {
      const error = e.response.data.error;
      if (error) {
        const type = error.type;
        const ex = new ServerError(error?.message || e.response.data.error);
        console.error(error);
        ex.debugInfo = error.debugInfo;
        ex.type = type;
        ex.error = error;
        e = ex;
      }
    }
  }
  return e;
}

async function postApi<T>(
  url: string,
  params: unknown,
  loading?: () => void,
  options?: AxiosRequestConfig,
) {
  try {
    const res = await api.post<T>(getUrl(url), params, options);
    loading && loading();
    return res;
  } catch (e: unknown) {
    loading && loading();
    e = handleServerError(e);
    throw e;
  }
}

async function postApiUi<T>(
  url: string,
  params: unknown,
  message: string,
  options?: AxiosRequestConfig,
) {
  const constroller = new AbortController();
  let progressDlg: DialogChainObject | null = Dialog.create({
    message,
    progress: true, // we enable default settings
    persistent: true, // we want the user to not be able to close it
    ok: false, // we want the user to not be able to close it
    cancel: true,
    focus: 'none',
  });
  progressDlg.onCancel(() => {
    progressDlg = null;
    constroller.abort();
  });

  const loading = () => progressDlg && progressDlg.hide();
  options = options || {};
  options.signal = constroller.signal;
  try {
    return await postApi<T>(url, params, loading, options);
  } catch (e: unknown) {
    assertIsError(e);
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function downloadFile(data: any, filename: string, mime: string, bom?: any) {
  const blobData = typeof bom !== 'undefined' ? [bom, data] : [data];
  const blob = new Blob(blobData, { type: mime || 'application/octet-stream' });
  const blobURL =
    window.URL && window.URL.createObjectURL
      ? window.URL.createObjectURL(blob)
      : window.webkitURL.createObjectURL(blob);
  const tempLink = document.createElement('a');
  tempLink.style.display = 'none';
  tempLink.href = blobURL;
  tempLink.setAttribute('download', filename);

  // Safari thinks _blank anchor are pop ups. We only want to set _blank
  // target if the browser does not support the HTML5 download attribute.
  // This allows you to download files in desktop safari if pop up blocking
  // is enabled.
  if (typeof tempLink.download === 'undefined') {
    tempLink.setAttribute('target', '_blank');
  }

  document.body.appendChild(tempLink);
  tempLink.click();

  // Fixes "webkit blob resource error 1"
  setTimeout(() => {
    document.body.removeChild(tempLink);
    window.URL.revokeObjectURL(blobURL);
  }, 200);
}

async function getFile(url: string, params?: unknown, loading?: () => void) {
  try {
    const res = await api.get(getUrl(url), { responseType: 'blob', params });
    loading && loading();
    downloadFile(
      res.data,
      res.headers['filename'] || 'google-ads.yaml',
      'application/text',
    );
    return res;
  } catch (e: unknown) {
    loading && loading();
    assertIsError(e);
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
  }
}

async function getApi<T>(url: string, params?: unknown, loading?: () => void) {
  try {
    const res = await api.get<T>(getUrl(url), { params: params });
    loading && loading();
    return res;
  } catch (e: unknown) {
    loading && loading();
    e = handleServerError(e);
    throw e;
  }
}

async function getApiUi<T>(url: string, params: unknown, message: string) {
  Loading.show({ message });
  const loading = () => Loading.hide();
  try {
    return await getApi<T>(url, params, loading);
  } catch (e: unknown) {
    assertIsError(e);
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
    // TODO: show e.debugInfo
  }
}

async function executeWithWaiting<T>(
  callback: () => Promise<T>,
  message: string,
) {
  Loading.show({ message });
  const loading = () => Loading.hide();
  try {
    const res = await callback();
    loading();
    return res;
  } catch (e: unknown) {
    loading();
    assertIsError(e);
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
  }
}

export {
  api,
  postApi,
  getApi,
  postApiUi,
  getApiUi,
  getFile,
  setActiveTarget,
  executeWithWaiting,
};
