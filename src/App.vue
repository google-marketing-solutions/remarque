<!--
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
-->
<template>
  <router-view />
</template>

<script setup lang="ts">
import { onMounted, watch } from 'vue';
import { useQuasar } from 'quasar';
import { useConfigurationStore } from 'stores/configuration';
import { useAudiencesStore } from 'stores/audiences';
import { useRouter } from 'vue-router';
import { assertIsError } from 'src/helpers/utils';

const router = useRouter();
const $q = useQuasar();
const store = useConfigurationStore();
const storeAudiences = useAudiencesStore();

router.beforeEach((to, from) => {
  if (!Object.keys(to.query).length && Object.keys(from.query).length > 0) {
    return Object.assign({}, to, { query: from.query });
  }
});

async function initialize() {
  const progress = $q.dialog({
    message: 'Initializing. Loading configuration...',
    progress: true, // we enable default settings
    persistent: true, // we want the user to not be able to close it
    ok: false, // we want the user to not be able to close it
  });

  try {
    await store.loadConfiguration();
    progress.update({
      message: 'Initializing. Fetching audiences...',
    });
    await storeAudiences.loadAudiences();
    progress.hide();
    console.log('App.setup completed');
  } catch (e: unknown) {
    console.log('App failed to initialize');
    console.log(e);
    progress.hide();
    assertIsError(e);
    $q.dialog({
      title: 'Error',
      message: e.message,
    }).onDismiss(() => {
      router.push({ path: '/configuration' });
    });
  }
}

// Load preference on component mount
onMounted(() => {
  const darkModePreference = localStorage.getItem('darkMode');
  if (darkModePreference !== null) {
    $q.dark.set(darkModePreference === 'true');
  }
});

// Save preference whenever it changes
watch(
  () => $q.dark.isActive,
  (isDark) => {
    localStorage.setItem('darkMode', isDark.toString());
  },
);

initialize();
</script>
