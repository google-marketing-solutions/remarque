<!--
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
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
  async created() {
    const router = useRouter();

    router.beforeEach((to, from) => {
      if (!Object.keys(to.query).length && Object.keys(from.query).length > 0) {
        return Object.assign({}, to, { query: from.query });
      }
    });
    this.initialize();
  },
  methods: {
    async initialize() {
      const $q = useQuasar();
      const store = configurationStore();
      const router = useRouter();

      const progress = $q.dialog({
        message: 'Initializing. Loading configuration...',
        progress: true, // we enable default settings
        persistent: true, // we want the user to not be able to close it
        ok: false // we want the user to not be able to close it
      });

      try {
        await store.loadConfiguration();
        progress.update({
          message: 'Initializing. Fetching audiences...',
        });
        await store.loadAudiences();
        progress.hide();
        console.log('App.created completed');
      } catch (e: any) {
        console.log('App failed to initialize');
        console.log(e);
        progress.hide();
        $q.dialog({
          title: 'Error',
          message: e.message,
        }).onDismiss(() => {
          router.push({ path: '/configuration' })
        });
      }
    }
  }
});
</script>
