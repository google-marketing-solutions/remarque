<template>
  <div>
    <q-separator height="3" />
    <q-list separator>
      <q-item clickable v-ripple to="/configuration">
        <q-item-section>
          <q-item-label>Configuration</q-item-label>
          <q-item-label caption lines="2">Application setup, manage configurations, define GA4 source.</q-item-label>
        </q-item-section>
        <q-item-section side>
          <q-icon name="cog" />
        </q-item-section>
      </q-item>

      <q-item clickable v-ripple to="/audiences">
        <q-item-section>
          <q-item-label>Audiences</q-item-label>
          <q-item-label caption lines="2">Define audiences for apps and countries, change period, define
            funnel.</q-item-label>
        </q-item-section>
      </q-item>

      <q-item clickable v-ripple to="/google-ads">
        <q-item-section>
          <q-item-label>Sampling & Google Ads</q-item-label>
          <q-item-label caption lines="2">Manage audiences in Google Ads.</q-item-label>
        </q-item-section>
      </q-item>
    </q-list>
  </div>
</template>

<script lang="ts">
import { defineComponent } from 'vue';

export default defineComponent({
  name: 'SideMenu',
});
</script>
