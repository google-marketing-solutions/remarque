import { boot } from 'quasar/wrappers';
import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';

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
function setActiveTarget(target: string|undefined) {
  activeTarget = target || '';
}

function getUrl(url: string) {
  if (activeTarget) {
    if (url.includes("?")) {
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
  options?: AxiosRequestConfig
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

export { api, postApi, getApi, setActiveTarget };
