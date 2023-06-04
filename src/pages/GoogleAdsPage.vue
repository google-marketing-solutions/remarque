<template>
  <q-page class="items-center justify-evenly" padding>
    <h2>Google Ads</h2>

    <q-btn label="Run sampling" @click="onSampling" :fab="true" color="primary"></q-btn>

    <q-btn label="Upload audiences" @click="onAudiencesUpload" :fab="true" color="primary"></q-btn>

    <div class="q-mt-md">
      <q-card class="card" flat bordered>
        <q-card-section class="q-col-gutter-md" style="padding-top: 0; padding-bottom: 30px;">
          <div class="text-h6">Schedule execution</div>
          <div class="row">
            <div class="col-4">
              <q-toggle v-model="store.scheduled" indeterminate-value="null" label="Enabled" />
            </div>
          </div>
          <div class="row">
            <div class="col-4">
              <!-- <q-time v-model="store.schedule" format24h /> -->
              <q-input filled v-model="store.schedule" mask="time" :rules="['time']">
                <template v-slot:append>
                  <q-icon name="access_time" class="cursor-pointer">
                    <q-popup-proxy cover transition-show="scale" transition-hide="scale">
                      <q-time v-model="store.schedule" format24h>
                        <div class="row items-center justify-end">
                          <q-btn v-close-popup label="Close" color="primary" flat />
                        </div>
                      </q-time>
                    </q-popup-proxy>
                  </q-icon>
                </template>
              </q-input>
            </div>
            <div class="col-1"></div>
            <div class="col-4">
              <q-input outlined v-model="store.schedule_timezone" label="Timezone" placeholder="" :hide-bottom-space=true
                hint="Name of a timezone from tz database, e.g. Europe/Moscow, America/Los_Angeles, UTC">
                <!-- TODO: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones-->
                <template v-slot:append>
                  <q-icon name="language" />
                </template>
              </q-input>
            </div>
          </div>
        </q-card-section>
        <q-card-actions class="q-pa-md">
          <q-btn label="Load" icon="download" size="md" @click="onScheduleLoad" color="primary" style="width:130px" />
          <q-btn label="Save" icon="save" size="md" @click="onScheduleSave" color="primary" style="width:130px" />
        </q-card-actions>
      </q-card>
    </div>

    <div class="q-mt-md">
      <q-card class="card" flat bordered>
        <q-card-section>
          <div class="text-h6">Audiences segments uploaded to Google Ads</div>
          <div>
            <q-banner class="bg-grey-3">
              <template v-slot:avatar>
                <q-icon name="info" color="primary" />If you scheduled execution then for each defined audience
                there will be segments with sampled users uploaded to Google Ads as customer match user lists.
              </template>
            </q-banner>
          </div>
        </q-card-section>
        <q-card-actions class="q-pa-md">
          <q-btn label="Load" icon="download" size="md" @click="onFetchAudiencesStatus" color="primary"
            style="width:130px" />
        </q-card-actions>

        <q-card-section>
          <div class="">
            <q-table title="Audiences" style="height: 300px" flat bordered :rows="data.audiences" row-key="name"
              :columns="data.audiences_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]" v-model:selected="data.selectedAudience" selection="single" hide-bottom>
              <template v-slot:body-cell-actions="props">
                <q-td :props="props">
                  <q-btn dense round flat color="grey" @click="onOpenChart(props)" icon="query_stats"></q-btn>
                </q-td>
              </template>

            </q-table>
          </div>
        </q-card-section>
        <q-card-section>
          <div class="">
            <q-table title="Log" style="height: 300px" flat bordered :rows="data.audience_log" row-key="name"
              :columns="data.audience_status_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]" hide-bottom>
            </q-table>
          </div>
        </q-card-section>

        <q-card-section v-if="data.selectedAudience.length">
          <apexchart v-if="data.chart.series.length" style="width:100%" type="line" :options="data.chart.options"
            :series="data.chart.series"></apexchart>
          <q-banner class="bg-grey-2">
            <!-- TODO: input for date range -->
            <q-btn label="Load conversions" @click="onLoadConversions" color="primary" icon="query_stats"></q-btn>
          </q-banner>
        </q-card-section>
      </q-card>
    </div>
  </q-page>
  <q-dialog v-model="data.resultDialog.show">
    <q-card style="width: 300px" class="q-px-sm q-pb-md">
      <q-card-section>
        <div class="text-h6">{{ data.resultDialog.header }}</div>
      </q-card-section>
      <q-card-section><span v-html="data.resultDialog.message"></span></q-card-section>
      <q-separator />
      <q-card-actions align="right">
        <q-btn v-close-popup flat color="primary" label="Close" />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script lang="ts">
import { defineComponent, ref, watch } from 'vue';
import { useQuasar } from 'quasar';
import { AudienceInfo, configurationStore } from 'stores/configuration';
import { getApi, postApi } from 'boot/axios';
import { formatArray, formatDate } from '../helpers/utils';

interface AudienceLog {
  //status: any;
  date: any;
  test_user_count: number;
  control_user_count: number;
  uploaded_user_count: number;
  new_user_count: number;
  job_status: any;
  job_failure: any;
}
interface Conversions {
  date: string;
  cum_test_regs: number;
  cum_control_regs: number;
}
interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions[];
}
export default defineComponent({
  name: 'GoogleAdsPage',
  components: {},
  setup: () => {
    const store = configurationStore();
    const $q = useQuasar();
    const data = ref({
      audiences: [] as AudienceWithLog[], //store.audiences,
      audiences_data: {},
      audience_log: [],
      selectedAudience: [] as AudienceWithLog[],
      audiences_columns: [
        { name: 'name', label: 'Name', field: 'name', sortable: true },
        { name: 'app_id', label: 'App id', field: 'app_id', sortable: true },
        { name: 'countries', label: 'Countries', field: 'countries', sortable: true, format: formatArray },
        { name: 'events_include', label: 'Include events', field: 'events_include', sortable: true, format: formatArray },
        { name: 'events_exclude', label: 'Exclude events', field: 'events_exclude', sortable: true, format: formatArray },
        { name: 'days_ago_start', label: 'Start', field: 'days_ago_start' },
        { name: 'days_ago_end', label: 'End', field: 'days_ago_end' },
        { name: 'actions', label: 'Actions', field: '', align: 'center' },
      ],
      audience_status_columns: [
        //{ name: 'status', label: 'Status', field: 'status', sortable: true },
        { name: 'date', label: 'Date', field: 'date', sortable: true },
        { name: 'test_user_count', label: 'Test User count', field: 'test_user_count', sortable: true },
        { name: 'control_user_count', label: 'Control User Count', field: 'control_user_count', sortable: true },
        { name: 'uploaded_user_count', label: 'Uploaded User Count', field: 'user_count', sortable: true },
        { name: 'new_user_count', label: 'New User count', field: 'new_user_count', sortable: true },
        { name: 'job_status', label: 'Job Status', field: 'job_status', sortable: true },
        { name: 'job_failure', label: 'Job Failure', field: 'job_failure', sortable: true },
      ],
      chart: {
        options: {
          chart: {
            height: 350,
            type: 'line',
          },
          xaxis: {
            categories: [] as string[]
          }
        },
        series: [] as any
      },
      resultDialog: {
        show: false,
        header: '',
        message: '',
      }
    });

    const onSampling = async () => {
      $q.loading.show({ message: 'Running sampling for audiences...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await postApi('sampling/run', {}, loading);
        /*
          "result": {
            "userlist1": {
              "control_count": 0,
              "test_count": 0
            }
          }
         */
        if (res.data && res.data.result) {
          let results = Object.entries(res.data.result);
          let html = '';
          for (const item of results) {
            html += `<div class="text-subtitle1">Audience '${item[0]}' results:</div>`;
            const result = <any>item[1];
            html += `<div class="text-caption">Control user count: ${result.control_count}<br>Test count: ${result.test_count}<br></div>`;
          }
          data.value.resultDialog.header = 'Sampling completed';
          data.value.resultDialog.message = html;
          data.value.resultDialog.show = true;
        }
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onAudiencesUpload = async () => {
      const progressDlg = $q.dialog({
        message: 'Uploading audiences to Google Ads...',
        progress: true, // we enable default settings
        persistent: true, // we want the user to not be able to close it
        ok: false // we want the user to not be able to close it
      });
      const loading = () => progressDlg.hide();
      try {
        let res = await postApi('ads/upload', {}, loading);
        /*
          {
            "userlist1": {
              "control_user_count": 5806,
              "failed_user_count": 0,
              "job_resource_name": "customers/xxx/offlineUserDataJobs/yyy",
              "new_user_count": 0,
              "test_user_count": 10856,
              "uploaded_user_count": 10856
            }
          }
        */
        if (res.data && res.data.result) {
          let results = Object.entries(res.data.result);
          let html = '';
          for (const item of results) {
            html += `<div class="text-subtitle1">Audience '${item[0]}' results:</div>`;
            const result = <any>item[1];
            html += `<div class="text-caption"><ul>
              <li>Control user count: ${result.control_user_count}</li>
              <li>Test user count: ${result.test_user_count}</li>
              <li>Uploaded user count: ${result.uploaded_user_count}</li>
              <li>New user count: ${result.new_user_count}</li>
              </ul></div>`;
          }
          data.value.resultDialog.header = 'Audience uploading to Google Ads completed';
          data.value.resultDialog.message = html;
          data.value.resultDialog.show = true;
        }
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onScheduleLoad = async () => {
      $q.loading.show({ message: 'Fetching Cloud Scheduler job...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await getApi('schedule', {}, loading);
        if (res.data) {
          store.scheduled = res.data.scheduled;
          store.schedule = res.data.schedule;
          store.schedule_timezone = res.data.schedule_timezone;
        } else {
          store.scheduled = false;
          store.schedule = '';
          store.schedule_timezone = '';
        }
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onScheduleSave = async () => {
      $q.loading.show({ message: 'Updating Cloud Scheduler job...' });
      const loading = () => $q.loading.hide();
      try {
        await postApi('schedule/edit', {
          scheduled: store.scheduled,
          schedule: store.schedule,
          schedule_timezone: store.schedule_timezone
        }, loading);
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };

    watch(() => data.value.selectedAudience, (newValue: any[]) => {
      if (newValue && newValue.length) {
        let newActiveAudience = newValue[0];
        data.value.audience_log = newActiveAudience.log;
        if (newActiveAudience.conversions) {
          updateConversionsChart(newActiveAudience.conversions);
        } else {
          data.value.chart.series = [];
        }
      }
    });

    const onFetchAudiencesStatus = async () => {
      $q.loading.show({ message: 'Fetching audiences status...' });
      const loading = () => $q.loading.hide();
      try {
        data.value.audiences = [];
        data.value.audience_log = [];
        let res = await getApi('audiences/status', {}, loading);
        console.log(res.data);
        const result = res.data.result;
        data.value.audiences_data = result;
        let audiences = <any[]>[];
        Object.keys(result).map(name => {
          const audience = result[name];
          audiences.push({
            'name': audience.name,
            'app_id': audience.app_id,
            'countries': audience.countries,
            'events_include': audience.events_include,
            'events_exclude': audience.events_exclude,
            'days_ago_start': audience.days_ago_start,
            'days_ago_end': audience.days_ago_end,
            'user_list': audience.user_list,
            'log': audience.log
          });
        });
        data.value.audiences = audiences;
        if (audiences.length > 0) {
          data.value.selectedAudience = [audiences[0]];
        }
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onLoadConversions = async () => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        audience.conversions = await loadConversions(audience.name);
        updateConversionsChart(audience.conversions);
      }
    };
    const loadConversions = async (audienceName: string) => {
      $q.loading.show({ message: 'Fetching audience conversion history...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await getApi('audiences/conversions', { audience: audienceName }, loading);
        //$q.notify({ message: 'Sampling completed', icon: 'success', timeout: 1000 });
        const all_conversions = res.data.results;
        let conversions;
        if (all_conversions) {
          conversions = all_conversions[audienceName]
        }
        if (!conversions) {
          $q.dialog({
            title: audienceName,
            message: 'The audience has no conversions',
          });
          return;
        }
        // we exepect an object with fields: date, cum_test_regs, cum_control_regs
        console.log(conversions);
        return conversions;
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const updateConversionsChart = (conversions: Conversions[] | undefined) => {
      if (!conversions || !conversions.length) {
        data.value.chart.series = [];
        return;
      }
      const labels = [] as string[];
      const test_data = [] as number[];
      const control_data = [] as number[];
      for (const item of conversions) {
        labels.push(formatDate(new Date(item.date)));
        test_data.push(item.cum_test_regs);
        control_data.push(item.cum_control_regs);
      }
      data.value.chart.options.xaxis.categories = labels;
      data.value.chart.series = [
        { name: 'test', data: test_data },
        { name: 'control', data: control_data },
      ]
    };
    const onOpenChart = async (props: any) => {
      const audience = props.row;
      audience.conversions = await loadConversions(audience.name);
      updateConversionsChart(audience.conversions);
    }
    return {
      store,
      data,
      onSampling,
      onAudiencesUpload,
      onScheduleLoad,
      onScheduleSave,
      onFetchAudiencesStatus,
      onLoadConversions,
      onOpenChart
    };
  }
});
</script>
