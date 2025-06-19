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
  <q-layout view="hHh Lpr lFf">
    <q-header elevated>
      <q-toolbar>
        <q-btn
          flat
          dense
          round
          icon="menu"
          aria-label="Menu"
          @click="toggleLeftDrawer"
        />
        <q-toolbar-title>
          <router-link to="/" style="text-decoration: none; color: white"
            >Remarque</router-link
          ></q-toolbar-title
        >
        <q-btn
          :icon="$q.dark.isActive ? 'light_mode' : 'dark_mode'"
          flat
          round
          @click="$q.dark.toggle()"
          aria-label="Toggle dark mode"
        />
        <q-btn-dropdown
          stretch
          flat
          :label="store.activeTarget"
          v-if="store.targets?.length > 1"
        >
          <q-list>
            <q-item
              v-for="t in store.targets"
              :key="`${t.name}`"
              clickable
              v-close-popup
              tabindex="0"
              @click="store.switchTarget(t.name)"
              :active="store.activeTarget == t.name"
            >
              <q-item-section>
                <q-item-label>{{ t.name }}</q-item-label>
              </q-item-section>
            </q-item>
          </q-list>
        </q-btn-dropdown>
      </q-toolbar>
    </q-header>

    <q-drawer v-model="leftDrawerOpen" show-if-above bordered>
      <SideMenu />
    </q-drawer>

    <q-page-container>
      <router-view />
    </q-page-container>

    <q-footer
      :class="$q.dark.isActive ? 'bg-grey-10 text-white' : 'bg-white text-dark'"
    >
      <div class="text-body1 text-center q-ma-sm">
        &copy;&nbsp;Google gTech Ads, 2025. Built
        {{ formattedBuildTime }} (git#{{ GIT_HASH }}) (not an official Google
        product)
      </div>
    </q-footer>
  </q-layout>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { useQuasar } from 'quasar';
import SideMenu from 'components/SideMenu.vue';
import { useConfigurationStore } from 'stores/configuration';

const $q = useQuasar();

defineOptions({
  name: 'MainLayout',
});

const leftDrawerOpen = ref(false);

function toggleLeftDrawer() {
  leftDrawerOpen.value = !leftDrawerOpen.value;
}

const BUILD_TIMESTAMP = process.env.BUILD_TIMESTAMP;
const GIT_HASH = process.env.GIT_HASH;

const formattedBuildTime = BUILD_TIMESTAMP
  ? new Date(BUILD_TIMESTAMP).toLocaleString()
  : '';

const store = useConfigurationStore();
</script>
