<template>
  <q-page class="items-center justify-evenly" padding>
    <div class="row">
      <div class="text-h2">Google Ads</div>
    </div>
    <div class="row" style="margin-top: 20px">
      <q-btn label="Run sampling" @click="onSampling" :fab="true" color="primary"></q-btn>
      <q-btn label="Upload audiences" @click="onAudiencesUpload" :fab="true" color="primary"></q-btn>
    </div>

    <div class="q-mt-md">
      <q-card class="card" flat bordered>
        <q-card-section>
          <div class="text-h6">Audiences segments uploaded to Google Ads</div>
          <div>
            <q-banner class="bg-grey-3">
              <template v-slot:avatar>
                <q-icon name="info" color="primary" size="md" style="margin-right: 5px;" />
                If you scheduled execution then for each defined audience
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
            <q-toggle v-model="data.audiences_wrap" label="Word wrap" />
            <q-table title="Audiences" class="qtable-sticky-header" style="height: 300px" flat bordered
              :rows="data.audiences" row-key="name" :columns="data.audiences_columns" virtual-scroll
              :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]" v-model:selected="data.selectedAudience"
              :wrap-cells="data.audiences_wrap" selection="single" hide-bottom>
              <template v-slot:body-cell-actions="props">
                <q-td :props="props">
                  <q-btn dense round flat color="grey" @click="onOpenChart(props)" icon="query_stats"></q-btn>
                </q-td>
              </template>
              <template v-slot:body-cell-mode="props">
                <q-td :props="props">
                  <q-chip :color="props.row.mode === 'off' ? 'red' : 'green'" text-color="white" dense
                    class="text-weight-bolder" square>{{ props.row.mode === 'off' ? 'Off' : props.row.mode === 'test' ?
                      'Test ' : 'Prod' }}</q-chip>
                </q-td>
              </template>
              <template v-slot:body-cell-countries="props">
                <q-td :props="props">
                  <div class="limited-width">
                    {{ formatArray(props.row.countries) }}
                    <q-tooltip>{{ formatArray(props.row.countries) }}</q-tooltip>
                  </div>
                </q-td>
              </template>
            </q-table>
          </div>
        </q-card-section>
        <q-card-section>
          <div class="">
            <q-table title="Upload history" class="qtable-sticky-header" style="height: 300px" flat bordered
              :rows="data.audience_log" row-key="name" :columns="data.audience_status_columns" virtual-scroll
              :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]" hide-bottom>
            </q-table>
          </div>
        </q-card-section>

        <q-card-section v-if="data.selectedAudience.length">
          <q-banner class="bg-grey-2">
            <div class="row">
              <div class="col q-pa-xs">
                <q-banner class="bg-grey-3">
                  <template v-slot:avatar><q-icon name="info" color="primary" size="md" style="margin-right: 5px;" />
                    If you don't specify the start date then the day of first upload to Google Ads will be used.<br>
                    If you don't specify the end date then yesterday will be used.
                  </template>
                </q-banner>
              </div>
            </div>
            <div class="row">
              <div class="col q-pa-xs" style="max-width:250px">
                <q-input filled v-model="data.conversions_from" mask="####-##-##" label="Start date" clearable>
                  <template v-slot:append>
                    <q-icon name="event" class="cursor-pointer">
                      <q-popup-proxy ref="qStartProxy" cover transition-show="scale" transition-hide="scale">
                        <q-date v-model="data.conversions_from" mask="YYYY-MM-DD" :no-unset="true"
                          @update:model-value="$refs.qStartProxy.hide()">
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col q-pa-xs" style="max-width:250px">
                <q-input filled v-model="data.conversions_to" mask="####-##-##" label="End date" clearable>
                  <template v-slot:append>
                    <q-icon name="event" class="cursor-pointer">
                      <q-popup-proxy ref="qEndProxy" cover transition-show="scale" transition-hide="scale">
                        <q-date v-model="data.conversions_to" mask="YYYY-MM-DD" today-btn :no-unset="true"
                          @update:model-value="$refs.qEndProxy.hide()">
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col q-pa-xs">
                <!-- <q-input filled v-model="data.country" label="Country" clearable></q-input> -->
                <q-select filled v-model="data.conversions_selected_countries" multiple
                  :options="data.conversions_countries" label="Country" style="width: 250px" clearable />
              </div>

              <div class="col q-pa-xs">
                <q-banner class="bg-grey-3">
                  p-val: <q-badge>{{ data.pvalFormatted }}</q-badge>
                  <br>If pval &lt;=0.05, then results are statistically significant
                </q-banner>
              </div>

            </div>
            <q-btn label="Load conversions" @click="onLoadConversions" color="primary" icon="query_stats"></q-btn>
          </q-banner>
          <apexchart v-if="data.chart.series.length" style="width:100%" :options="data.chart.options"
            :series="data.chart.series"></apexchart>
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

<style>
.limited-width {
  max-width: 100px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>

<script lang="ts">
import { computed, defineComponent, ref, watch } from 'vue';
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
  new_test_user_count: number;
  new_control_user_count: number;
  total_test_user_count: number;
  total_control_user_count: number;

  job_status: any;
  job_failure: any;
}
interface Conversions {
  data: ConversionsData[];
  start_date: string;
  end_date: string;
  pval: number;
}
interface ConversionsData {
  date: string;
  cum_test_regs: number;
  cum_control_regs: number;
}
interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
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
        { name: 'mode', label: 'Mode', field: 'mode' },
        { name: 'name', label: 'Name', field: 'name', sortable: true },
        { name: 'app_id', label: 'App id', field: 'app_id', sortable: true },
        { name: 'countries', label: 'Countries', field: 'countries', sortable: true, format: formatArray },
        { name: 'events_include', label: 'Include events', field: 'events_include', sortable: true, format: formatArray },
        { name: 'events_exclude', label: 'Exclude events', field: 'events_exclude', sortable: true, format: formatArray },
        { name: 'days_ago_start', label: 'Start', field: 'days_ago_start' },
        { name: 'days_ago_end', label: 'End', field: 'days_ago_end' },
        { name: 'ttl', label: 'TTL', field: 'ttl' },
        { name: 'actions', label: 'Actions', field: '', align: 'center' },
      ],
      audiences_wrap: true,
      audience_status_columns: [
        //{ name: 'status', label: 'Status', field: 'status', sortable: true },
        { name: 'date', label: 'Date', field: 'date', sortable: true },
        { name: 'test_user_count', label: 'Test Users', field: 'test_user_count', sortable: true },
        { name: 'control_user_count', label: 'Control Users', field: 'control_user_count', sortable: true },
        { name: 'uploaded_user_count', label: 'Uploaded Users', field: 'user_count', sortable: true },
        { name: 'new_test_user_count', label: 'New Test Users', field: 'new_test_user_count', sortable: true },
        { name: 'new_control_user_count', label: 'New Control Users', field: 'new_control_user_count', sortable: true },
        { name: 'total_test_user_count', label: 'Total Test Users', field: 'total_test_user_count', sortable: true },
        { name: 'total_control_user_count', label: 'Total Control Users', field: 'total_control_user_count', sortable: true },
        { name: 'job_status', label: 'Job Status', field: 'job_status', sortable: true },
        { name: 'job_failure', label: 'Job Failure', field: 'job_failure', sortable: true },
      ],
      chart: {
        options: {
          chart: {
            height: 100,
            type: 'line',
          },
          stroke: {
            curve: 'straight'
          },
          title: {
            text: 'Conversions',
            align: 'left'
          },
          grid: {
            row: {
              colors: ['#f3f3f3', 'transparent'],
              opacity: 0.5
            },
          },
          labels: [],
          xaxis: {
            type: 'category',
          }
        },
        series: [] as any,
      },
      resultDialog: {
        show: false,
        header: '',
        message: '',
      },
      conversions_from: <string | undefined>undefined,
      conversions_to: <string | undefined>undefined,
      conversions_selected_countries: [],
      conversions_countries: [],
      pval: <number | undefined>undefined,
      pvalFormatted: computed(() => {
        return data.value.pval ? data.value.pval.toFixed(5) : '-'
      })
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
              "new_test_user_count": 0,
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
              <li>New test user count: ${result.new_test_user_count}</li>
              <li>New control user count: ${result.new_control_user_count}</li>
              <li>Total test user count: ${result.total_test_user_count}</li>
              <li>Total control user count: ${result.total_control_user_count}</li>
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

    watch(() => data.value.selectedAudience, (newValue: any[]) => {
      if (newValue && newValue.length) {
        let newActiveAudience = newValue[0];
        data.value.audience_log = newActiveAudience.log;
        data.value.conversions_selected_countries = [];
        data.value.conversions_countries = newActiveAudience.countries;
        updateConversionsChart(newActiveAudience.conversions);
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
            'mode': audience.mode,
            'name': audience.name,
            'app_id': audience.app_id,
            'countries': audience.countries,
            'events_include': audience.events_include,
            'events_exclude': audience.events_exclude,
            'days_ago_start': audience.days_ago_start,
            'days_ago_end': audience.days_ago_end,
            'user_list': audience.user_list,
            'ttl': audience.ttl,
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
        let date_start = <string | undefined>data.value.conversions_from;
        let date_end = <string | undefined>data.value.conversions_to;
        let country = data.value.conversions_selected_countries;
        if (country && country.length) {
          country = country.join(',');
        }
        audience.conversions = await loadConversions(audience.name, date_start, date_end, country);
        updateConversionsChart(audience.conversions);
      }
    };

    const loadConversions = async (audienceName: string, date_start: string | undefined, date_end: string | undefined, country: string | undefined): Promise<Conversions | undefined> => {
      data.value.chart.series = [];
      $q.loading.show({ message: 'Fetching the audience conversion history...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await getApi('audiences/conversions', { audience: audienceName, date_start, date_end, country }, loading);
        const results = res.data.results;
        let result;
        if (results) {
          result = results[audienceName]
        }
        if (!result) {
          $q.dialog({
            title: audienceName,
            message: 'The audience has no conversions',
          });
          return;
        }
        // 'result' object for a particular audience is expected to be: conversions, date_start, date_end, pval, chi
        // 'result.conversions' is an array of objects with fields: date, cum_test_regs, cum_control_regs
        console.log(result);
        return { data: result.conversions, start_date: result.date_start, end_date: result.date_end, pval: result.pval };
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };

    const updateConversionsChart = (conversions?: Conversions) => {
      if (!conversions || !conversions.data || !conversions.data.length) {
        data.value.chart.series = [];
        data.value.conversions_from = undefined;
        data.value.conversions_to = undefined;
        data.value.pval = undefined;
        return;
      }
      data.value.conversions_from = conversions.start_date;
      data.value.conversions_to = conversions.end_date;
      data.value.pval = conversions.pval;

      let graph_data = {} as Record<string, any>;
      // TODO: should we limit graph with on X axis back only N days from the end date?
      for (const item of conversions.data) {
        const label = formatDate(new Date(item.date));
        // NOTE: dates should not repeat otherwise there will be no graph
        graph_data[label] = {
          test: item.cum_test_regs,
          control: item.cum_control_regs
        };
      }
      const test_data = Object.entries(graph_data).map(item => { return { x: item[0], y: item[1].test }; });
      const control_data = Object.entries(graph_data).map(item => { return { x: item[0], y: item[1].control }; });
      data.value.chart.series = [
        { name: 'treatment', data: test_data },
        { name: 'control', data: control_data },
      ]
    };

    const onOpenChart = async (props: any) => {
      const audience = props.row;
      audience.conversions = await loadConversions(audience.name, undefined, undefined, undefined);
      updateConversionsChart(audience.conversions);
    }

    return {
      store,
      data,
      onSampling,
      onAudiencesUpload,
      onFetchAudiencesStatus,
      onLoadConversions,
      onOpenChart,
      formatArray
    };
  }
});
</script>
