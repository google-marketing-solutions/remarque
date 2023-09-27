<template>
  <router-view />
</template>

<script lang="ts">
import { defineComponent, ref } from 'vue';
import { useQuasar } from 'quasar';
import { configurationStore } from 'stores/configuration';
import { useRouter } from 'vue-router';

export default defineComponent({
  name: 'App',
  created: async () => {
    const $q = useQuasar();
    const store = configurationStore();
    const router = useRouter();

    // TODO: to pass query param through routers (but it causes infinite recursion)
    // router.beforeEach((to, from) => {
    //   if (!Object.keys(to.query).length) {
    //     return Object.assign({}, to, { query: from.query })
    //   }
    // });
    router.beforeEach((to, from) => {
      if (!Object.keys(to.query).length && Object.keys(from.query).length > 0) {
        return Object.assign({}, to, { query: from.query });
      }
    });
    const progress = $q.dialog({
      message: 'Initialing. Loading configuration...',
      progress: true, // we enable default settings
      persistent: true, // we want the user to not be able to close it
      ok: false // we want the user to not be able to close it
    });
    return new Promise(async resolve => {
      try {
        await store.loadConfiguration();
        progress.update({
          message: 'Initializing. Fetching audiences...',
        });
        await store.loadAudiences();
        progress.hide();
        console.log('App.created completed');
        resolve(null);
      } catch (e: any) {
        console.log('App failed to initialize');
        console.log(e);
        progress.hide();
        resolve(null);
        $q.dialog({
          title: 'Error',
          message: e.message,
        }).onDismiss(() => {
          router.push({ path: '/configuration' })

        });
      }
    });
  }
});
</script>
