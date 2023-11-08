<template>
  <q-layout view="hHh Lpr lFf">
    <q-header elevated>
      <q-toolbar>
        <q-btn flat dense round icon="menu" aria-label="Menu" @click="toggleLeftDrawer" />
        <q-toolbar-title> <router-link to="/" style="text-decoration: none;color: white;">Remarque</router-link></q-toolbar-title>
        <q-btn-dropdown stretch flat :label="store.activeTarget" v-if="store.targets?.length > 1">
          <q-list>
            <q-item v-for="t in store.targets" :key="`${t.name}`" clickable v-close-popup tabindex="0"
              @click="store.activateTarget(t.name)" :active="store.activeTarget == t.name">
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

    <q-footer></q-footer>
  </q-layout>
</template>

<script lang="ts">
import { defineComponent, ref } from 'vue';

import { configurationStore } from 'stores/configuration';
import SideMenu from 'components/SideMenu.vue';

export default defineComponent({
  name: 'MainLayout',

  components: {
    SideMenu,
  },

  setup() {
    const leftDrawerOpen = ref(false);
    const store = configurationStore();

    return {
      leftDrawerOpen,
      store,
      toggleLeftDrawer() {
        leftDrawerOpen.value = !leftDrawerOpen.value;
      },
    };
  },
});
</script>
