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
  <q-page class="items-center justify-evenly" padding>
    <div class="q-pa-md q-gutter-sm" v-if="data.notInitialized">
      <q-banner inline-actions rounded class="bg-red text-white">
        The application has not been initialized. Please fill all fields here
        and run Setup action below.
      </q-banner>
    </div>
    <div class="row">
      <div class="text-h2">Configuration</div>
    </div>

    <div class="row q-pa-md">
      <q-input
        class="col-7 q-mb-md"
        outlined
        v-model="store.name"
        label="Name"
        placeholder="Configuration name"
        hint="Configuration name can be empty if it's only one"
        :hide-bottom-space="true"
      />
      <q-btn
        label="New"
        @click="onNewTarget"
        :fab="true"
        class="q-ml-md"
        v-if="!store.is_new"
      />
      <q-btn
        label="Delete"
        @click="onDeleteTarget"
        :fab="true"
        class="q-ml-md"
        v-if="!store.is_new"
      />

      <q-card class="col-7 card" flat bordered>
        <q-card-section>
          <div class="text-h6">GA4 BigQuery Table</div>
          <div class="text-subtitle2">events_* tables with app events</div>
        </q-card-section>

        <q-input
          class="q-mb-md"
          outlined
          v-model="store.ga4_project"
          label="Project id"
          placeholder="BigQuery project id"
          hint="Leave empty for using the current GCP project"
          :hide-bottom-space="true"
        />

        <q-input
          class="q-mb-md"
          outlined
          v-model="store.ga4_dataset"
          label="Dataset id"
          placeholder="BigQuery dataset id"
          hint="Usually analytics_XXX where XXX GA property id"
          :hide-bottom-space="true"
        />

        <q-input
          class="q-mb-md"
          outlined
          v-model="store.ga4_table"
          label="Table id"
          placeholder="BigQuery table id"
          hint="By default 'events' if empty (do not include _* part)"
          :hide-bottom-space="true"
        />

        <q-space />

        <q-btn label="Check" @click="onGA4Connect" class="q-mt-md"></q-btn>
      </q-card>
    </div>

    <div class="row q-pa-md">
      <q-card class="col-7 card" flat bordered>
        <q-form class="q-gutter-y-md q-pa-md">
          <q-card-section>
            <div class="text-h6">BigQuery</div>
            <div class="text-subtitle2">
              BQ dataset where all application data will be kept
            </div>
          </q-card-section>
          <q-banner inline-actions rounded class="bg-grey-3 q-mb-md">
            <template v-slot:avatar>
              <q-icon name="info" color="primary" />
            </template>

            Please note that the dataset will be created in the same location
            (us or eu) where your GA4 dataset is located.
          </q-banner>

          <q-input
            outlined
            v-model="store.bq_dataset_id"
            label="Dataset id"
            placeholder="BigQuery dataset id"
            hint="by default - 'remarque'"
          />

          <q-input
            outlined
            v-model="store.bq_dataset_location"
            label="Dataset location"
            readonly
            class="q-mt-md"
            placeholder="BigQuery dataset location (us or eu)"
            hint=""
          />

          <!-- <q-input
            outlined
            v-model="store.ga4_loopback_window"
            label="GA4 loopback window"
            hint="Loopback window for GA4 users data used for users_normalized table, by default 1 year (1Y). Format: nYmMoD where n = number of years, m - number of months, o - number of days"
          ></q-input> -->
          <q-item class="q-pa-none">
            <q-item-section>
              <q-input
                outlined
                v-model="store.ga4_loopback_window"
                label="GA4 loopback window"
                stack-label
              />
              <q-item-label caption class="q-pt-sm">
                Loopback window for GA4 users data used for users_normalized
                table, by default 1 year (1Y). Format: nYmMoD where n = number
                of years, m - number of months, o - number of days
              </q-item-label>
            </q-item-section>
          </q-item>

          <!-- <q-toggle v-model="store.ga4_loopback_recreate" label="Recreate users_normalized table" /> -->
          <q-item tag="label" v-ripple class="q-pa-none">
            <q-item-section>
              <q-item-label>Recreate users_normalized table</q-item-label>
              <q-item-label caption
                ><code>users_normalized</code> table contains user ids with
                attributes for "loopback window" period (1 year by default).
                When the option is enabled the table will be recreated every
                day. This might incur significant costs in BigQuery. By default
                (when disabled) every day a wildcard table
                users_normalized_yyyymmdd will be created and all such tables
                are unioned via a view <code>users_normalized</code>.
              </q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-toggle v-model="store.ga4_loopback_recreate" />
            </q-item-section>
          </q-item>
        </q-form>
      </q-card>
    </div>

    <div class="row q-pa-md">
      <q-card class="col-7 card" flat bordered>
        <q-form class="q-gutter-y-md q-pa-md">
          <q-card-section>
            <div class="text-h6">Google Ads</div>
            <div class="text-subtitle2"></div>
          </q-card-section>

          <q-input
            outlined
            v-model="store.ads_customer_id"
            label="Account id"
            placeholder="Google Ads account id"
            hint=""
          />
          <q-input
            outlined
            v-model="store.ads_developer_token"
            label="Developer token"
            placeholder="Google Ads developer token"
            hint=""
          />
          <q-input
            outlined
            v-model="store.ads_client_id"
            label="Client id"
            placeholder="GCP oauth client id"
            hint=""
          />
          <q-input
            outlined
            v-model="store.ads_client_secret"
            label="Client secret"
            placeholder="GCP oauth client secret"
            hint=""
          />
          <q-input
            outlined
            v-model="store.ads_refresh_token"
            label="Refresh token"
            placeholder="GCP oauth refresh token"
            hint=""
          >
            <!-- <template v-slot:after>
            <q-btn round dense flat icon="generating_tokens" @click="onGenerateToken" />
          </template> -->
          </q-input>
          <q-input
            outlined
            v-model="store.ads_login_customer_id"
            label="MCC id"
            placeholder="Google Ads MCC id"
            hint=""
          />

          <div class="row q-col-gutter-lg">
            <q-file
              class="col-6"
              v-model="data.file"
              label="Select google-ads.yaml"
              accept=".yaml"
            ></q-file>
            <div class="col-6">
              <q-btn
                label="Upload google-ads.yaml"
                color="primary"
                @click="uploadGoogleAdsConfig"
                class="q-ml-md q-my-xs full-width"
              ></q-btn>

              <q-btn
                label="Validate"
                color="primary"
                @click="validateGoogleAdsConfig"
                class="q-ml-md q-my-xs full-width"
              ></q-btn>
            </div>
          </div>
          <div class="row q-col-gutter-lg">
            <div class="col-6"></div>
            <div class="col-6">
              <q-btn
                label="Download google-ads.yaml"
                color="primary"
                @click="downloadGoogleAdsConfig"
                class="q-ml-md q-my-xs full-width"
              ></q-btn>
            </div>
          </div>
        </q-form>
      </q-card>
    </div>

    <q-btn label="Setup" @click="onSetup" :fab="true" color="negative"></q-btn>
    <q-btn
      label="Reload"
      @click="onReload"
      :fab="true"
      class="q-ml-md"
      v-if="!store.is_new"
    ></q-btn>
    <q-btn
      label="Cancel"
      @click="onCancel"
      :fab="true"
      class="q-ml-md"
      v-if="store.is_new"
    ></q-btn>
  </q-page>
</template>

<style scoped>
.card {
  padding-left: 20px;
  padding-right: 20px;
  padding-bottom: 20px;
  min-width: 400px;
}
.q-item {
  align-items: flex-start;
}

.q-item-label.caption {
  white-space: normal;
  line-height: 1.4;
}
</style>

<script lang="ts">
import { useQuasar } from 'quasar';
import { onBeforeRouteLeave } from 'vue-router';
import { postApiUi, getFile } from 'boot/axios';
import {
  ConfigTarget,
  States,
  useConfigurationStore,
} from 'stores/configuration';
import { defineComponent, ref } from 'vue';
import { assertIsError } from '../helpers/utils';

/**
 * Response type for 'setup' endpoint.
 */
interface SetupResponse {
  targets: ConfigTarget[];
}
/**
 * Response type for 'setup/upload_ads_cred' endpoint.
 */
interface UploadAdsCredentialsResponse {
  client_id: string;
  client_secret: string;
  customer_id: string;
  developer_token: string;
  login_customer_id: string;
  refresh_token: string;
}
export default defineComponent({
  name: 'DatasourcePage',
  components: {},
  setup: () => {
    const store = useConfigurationStore();
    const $q = useQuasar();

    const onGA4Connect = async () => {
      const res = await postApiUi(
        'setup/connect_ga4',
        {
          ga4_project: store.ga4_project,
          ga4_dataset: store.ga4_dataset,
          ga4_table: store.ga4_table,
        },
        'Testing GA4 data access...',
      );
      if (res) {
        $q.dialog({ ok: true, message: 'Successfully connected' });
      }
    };
    const onNewTarget = () => {
      store.initTarget({});
      store.name = '';
      store.name_org = '';
      store.is_new = true;
      store.activeTarget = '';
    };
    const onDeleteTarget = async () => {
      // we should NOT be able to delete the default target (store.activeTarget)
      if (!store.activeTarget || store.activeTarget === 'default') {
        console.error(
          'Trying to delete the default target (which is not allowed)',
        );
        return;
      }
      $q.dialog({
        title: 'Confirm',
        message: 'Are you sure you want to delete the configuration',
        cancel: true,
        persistent: true,
      }).onOk(async () => {
        // remove on the server
        await postApiUi(
          'setup/delete?target=' + store.activeTarget,
          {},
          'Saving...',
        );
        // now remove locally
        const index = store.targets.findIndex(
          (t) => t.name === store.activeTarget,
        );
        if (index >= 0) {
          store.targets.splice(index, 1);
        }
        store.activateTarget(store.targets.length ? store.targets[0].name : '');
      });
    };
    const onCancel = () => {
      const target = store.targets[0];
      store.initTarget(target);
      store.is_new = false;
      store.activeTarget = target.name;
    };
    const onSetup = async () => {
      const res = await postApiUi<SetupResponse>(
        'setup',
        {
          name: store.name,
          name_org: store.name_org,
          is_new: store.is_new,
          ga4_project: store.ga4_project,
          ga4_dataset: store.ga4_dataset,
          ga4_table: store.ga4_table,
          ga4_loopback_window: store.ga4_loopback_window,
          ga4_loopback_recreate: store.ga4_loopback_recreate,
          bq_dataset_id: store.bq_dataset_id,
          ads_client_id: store.ads_client_id,
          ads_client_secret: store.ads_client_secret,
          ads_customer_id: store.ads_customer_id,
          ads_developer_token: store.ads_developer_token,
          ads_login_customer_id: store.ads_login_customer_id,
          ads_refresh_token: store.ads_refresh_token,
        },
        'Initializing application...',
      );
      if (res) {
        store.targets = res.data.targets;
        store.is_new = false;
        $q.dialog({
          title: 'Succeeded',
          message: 'Application successfully initialized',
        }).onDismiss(() => {
          store.activateTarget(store.name);
        });
      }
    };
    const onReload = async () => {
      const loading = $q.notify('Reloading...');
      try {
        await store.loadConfiguration();
        loading();
      } catch (e: unknown) {
        loading();
        assertIsError(e);
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const data = ref({
      notInitialized: false,
      file: <File | null>null,
    });
    store.$subscribe((mutation, state) => {
      if (state.state === States.NOT_INITIALIZED) {
        data.value.notInitialized = true;
      }
    });
    onBeforeRouteLeave(() => {
      if (store.is_new) {
        const answer = window.confirm(
          'You have unsaved changes. Do you really want to leave?',
        );
        // cancel the navigation and stay on the same page
        if (!answer) return false;
        store.activateTarget(store.activeTarget);
      }
    });

    const downloadGoogleAdsConfig = async () => {
      await getFile('setup/download_ads_cred', {});
    };

    const uploadGoogleAdsConfig = async () => {
      if (!data.value.file) {
        $q.dialog({
          ok: true,
          message: 'Please choose a file to upload (google-ads.yaml)',
        });
        return;
      }
      const formData = new FormData();
      formData.append('file', data.value.file);

      const response = await postApiUi<UploadAdsCredentialsResponse>(
        'setup/upload_ads_cred',
        formData,
        'Saving...',
        {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
        },
      );
      console.log(response);
      $q.notify({
        color: 'green-4',
        textColor: 'white',
        icon: 'cloud_upload',
        message: 'File uploaded successfully',
      });
      if (response) {
        store.ads_client_id = response.data.client_id;
        store.ads_client_secret = response.data.client_secret;
        store.ads_customer_id = response.data.customer_id;
        store.ads_developer_token = response.data.developer_token;
        store.ads_login_customer_id = response.data.login_customer_id;
        store.ads_refresh_token = response.data.refresh_token;
        const target = store.targets.find((t) => t.name === store.activeTarget);
        if (target) {
          target.ads_client_id = store.ads_client_id;
          target.ads_client_secret = store.ads_client_secret;
          target.ads_customer_id = store.ads_customer_id;
          target.ads_developer_token = store.ads_developer_token;
          target.ads_login_customer_id = store.ads_login_customer_id;
          target.ads_refresh_token = store.ads_refresh_token;
        }
      }
    };
    const validateGoogleAdsConfig = async () => {
      const config = {
        ads_client_id: store.ads_client_id,
        ads_client_secret: store.ads_client_secret,
        ads_customer_id: store.ads_customer_id,
        ads_developer_token: store.ads_developer_token,
        ads_login_customer_id: store.ads_login_customer_id,
        ads_refresh_token: store.ads_refresh_token,
      };
      const response = await postApiUi(
        'setup/validate_ads_cred',
        config,
        'Validating Ads credentials',
      );
      console.log(response);
      if (response) {
        $q.notify({
          color: 'green-4',
          textColor: 'white',
          icon: 'cloud_upload',
          message: 'Ads API credentials validated successfully',
        });
      }
    };
    return {
      store,
      data: data,
      onGA4Connect,
      onSetup,
      onReload,
      onCancel,
      onDeleteTarget,
      onNewTarget,
      uploadGoogleAdsConfig,
      downloadGoogleAdsConfig,
      validateGoogleAdsConfig,
    };
  },
});
</script>
