import { boot } from 'quasar/wrappers';
import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import { QVueGlobals, Loading, QSpinnerGears, Dialog, DialogChainObject } from 'quasar';

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

let activeTarget = '';
function setActiveTarget(target: string | undefined) {
  activeTarget = target || '';
}

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

async function postApi(
  url: string,
  params: any,
  loading?: () => void,
  options?: AxiosRequestConfig,
) {
  try {
    const res = await api.post(getUrl(url), params, options);
    loading && loading();
    return res;
  } catch (e: any) {
    loading && loading();
    if (e.response && e.response.data) {
      const debugInfo = e.response.data.error?.debugInfo;
      const type = e.response.data.error?.type;
      e = new Error(e.response.data.error?.message || e.response.data.error);
      console.error(debugInfo);
      e.debugInfo = debugInfo;
      e.type = type;
    }
    throw e;
  }
}

async function postApiUi(
  url: string,
  params: any,
  message: string,
  options?: AxiosRequestConfig,
) {
  const constroller = new AbortController();
  let progressDlg: DialogChainObject|null = Dialog.create({
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

  // Loading.show({
  //   message: message,
  // });
  //const loading = () => Loading.hide();
  const loading = () => progressDlg && progressDlg.hide();
  options = options || {};
  options.signal = constroller.signal;
  try {
    return await postApi(url, params, loading, options);
  } catch (e: any) {
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
  }
}

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
  setTimeout(function () {
    document.body.removeChild(tempLink);
    window.URL.revokeObjectURL(blobURL);
  }, 200);
}

async function getFile(url: string, params?: any, loading?: () => void) {
  try {
    const res = await api.get(getUrl(url), { responseType: 'blob', params });
    loading && loading();
    downloadFile(
      res.data,
      res.headers['filename'] || 'google-ads.yaml',
      'application/text',
    );
    return res;
  } catch (e: any) {
    loading && loading();
  }
}

async function getApi(url: string, params?: any, loading?: () => void) {
  try {
    const res = await api.get(getUrl(url), { params: params });
    loading && loading();
    return res;
  } catch (e: any) {
    loading && loading();
    if (e.response && e.response.data) {
      const debugInfo = e.response.data.error?.debugInfo;
      const type = e.response.data.error?.type;
      e = new Error(e.response.data.error?.message || e.response.data.error);
      console.error(debugInfo);
      e.debugInfo = debugInfo;
      e.type = type;
    }
    throw e;
  }
}

async function getApiUi(url: string, params: any, message: string) {
  Loading.show({ message });
  const loading = () => Loading.hide();
  try {
    return await getApi(url, params, loading);
  } catch (e: any) {
    Dialog.create({
      title: 'Error',
      message: e.message,
    });
  }
}

export { api, postApi, getApi, postApiUi, getApiUi, getFile, setActiveTarget };
