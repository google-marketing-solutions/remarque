<template>
  <q-page class="items-center justify-evenly" padding>
    <div class="row">
      <div class="text-h2">Configuration</div>
    </div>

    <div class="row q-pa-md">
      <q-card class="col-5 card" flat bordered>
        <q-card-section>
          <div class="text-h6">GA4 BigQuery Table</div>
          <div class="text-subtitle2">events_* tables with app events</div>
        </q-card-section>

        <q-input outlined v-model="store.ga4_project" label="Project id" placeholder="BigQuery project id" hint=""
          :hide-bottom-space=true />

        <q-input outlined v-model="store.ga4_dataset" label="Dataset id" placeholder="BigQuery dataset id"
          hint="Usually analytics_XXX where XXX GA property id" :hide-bottom-space=true />

        <q-input outlined v-model="store.ga4_table" label="Table id" placeholder="BigQuery table id"
          hint="Usually events (do not include _* part)" :hide-bottom-space=true />

        <q-space />

        <q-btn label="Connect" @click="onGA4Connect" class="q-mt-md"></q-btn>
      </q-card>
    </div>

    <div class="row q-pa-md">
      <q-card class="col-4 card" flat bordered>
        <q-card-section>
          <div class="text-h6">BigQuery</div>
          <div class="text-subtitle2">BQ dataset where all intermediate data will be kept</div>
        </q-card-section>

        <q-input outlined v-model="store.bq_dataset_id" label="Dataset id" placeholder="BigQuery dataset id" hint="" />

        <q-input outlined v-model="store.bq_dataset_location" label="Dataset location"
          placeholder="BigQuery dataset location (us or eu)" hint="" />
      </q-card>
    </div>

    <div class="row q-pa-md">
      <q-card class="col-6 card" flat bordered>
        <q-card-section>
          <div class="text-h6">Google Ads</div>
          <div class="text-subtitle2"></div>
        </q-card-section>

        <q-input outlined v-model="store.ads_customer_id" label="Account id" placeholder="Google Ads account id"
          hint="" />
        <q-input outlined v-model="store.ads_developer_token" label="Developer token"
          placeholder="Google Ads developer token" hint="" />
        <q-input outlined v-model="store.ads_client_id" label="Client id" placeholder="GCP oauth client id" hint="" />
        <q-input outlined v-model="store.ads_client_secret" label="Client secret" placeholder="GCP oauth client secret"
          hint="" />
        <q-input outlined v-model="store.ads_refresh_token" label="Refresh token" placeholder="GCP oauth refresh token"
          hint="">
          <!-- <template v-slot:after>
            <q-btn round dense flat icon="generating_tokens" @click="onGenerateToken" />
          </template> -->
        </q-input>
        <q-input outlined v-model="store.ads_login_customer_id" label="MCC id" placeholder="Google Ads MCC id" hint="" />
      </q-card>
    </div>

    <q-btn label="Setup" @click="onSetup" :fab="true" color="negative"></q-btn>
    <q-btn label="Reload" @click="onReload" :fab="true" class="q-ml-md"></q-btn>
  </q-page>
</template>

<style scoped>
.card {
  padding-left: 20px;
  padding-right: 20px;
  padding-bottom: 20px;
  min-width: 400px;
}
</style>

<script lang="ts">
import { configurationStore } from 'stores/configuration';
import { useQuasar } from 'quasar';
import { postApi } from 'boot/axios';
import { defineComponent } from 'vue';

export default defineComponent({
  name: 'DatasourcePage',
  components: {},
  setup: () => {
    const store = configurationStore();
    const $q = useQuasar();

    const onGA4Connect = async () => {
      $q.loading.show({ message: 'Testing GA4 data access...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await postApi('setup/connect_ga4', {
          ga4_project: store.ga4_project,
          ga4_dataset: store.ga4_dataset,
          ga4_table: store.ga4_table,
        }, loading);
        $q.notify({ message: 'GA4 data source connected', icon: 'success', timeout: 1000 });
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onSetup = async () => {
      $q.loading.show({ message: 'Initializing application...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await postApi('setup', {
          ga4_project: store.ga4_project,
          ga4_dataset: store.ga4_dataset,
          ga4_table: store.ga4_table,
          bq_dataset_id: store.bq_dataset_id,
          bq_dataset_location: store.bq_dataset_location,
          ads_client_id: store.ads_client_id,
          ads_client_secret: store.ads_client_secret,
          ads_customer_id: store.ads_customer_id,
          ads_developer_token: store.ads_developer_token,
          ads_login_customer_id: store.ads_login_customer_id,
          ads_refresh_token: store.ads_refresh_token,
        }, loading);
        $q.dialog({
          title: 'Succeeded',
          message: 'Application successfully initialized',
        });
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    }
    const onReload = async () => {
      const loading = $q.notify('Reloading...');
      try {
        await store.loadConfiguration();
        loading();
      } catch (e: any) {
        loading();
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    return {
      store,
      onGA4Connect,
      onSetup,
      onReload
    };
  },
});
</script>
