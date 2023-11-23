<template>
  <q-page class="items-center justify-evenly" padding>
    <div class="row">
      <div class="text-h2">Google Ads</div>
    </div>
    <div class="row" style="margin-top: 20px">
      <q-btn label="Run All" @click="onExecute" :fab="true" color="primary" class="q-mx-md"></q-btn>
      <q-btn label="Run sampling" @click="onSampling" :fab="true" class="q-mx-md"></q-btn>
      <q-btn label="Upload audiences" @click="onAudiencesUpload" :fab="true" class="q-mx-md"></q-btn>
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
            style="width:130px" class="q-mr-lg" />
          <q-toggle v-model="data.include_log_duplicates" label="Include duplicates" class="q-mx-md" />
          <q-toggle v-model="data.skip_ads" label="Skip Ads info" />
        </q-card-actions>

        <q-card-section>
          <div class="">
            <q-table title="Audiences" class="qtable-sticky-header" style="height: 400px" flat bordered
              :rows="data.audiences" row-key="name" :columns="data.audiences_columns" virtual-scroll
              :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]" v-model:selected="data.selectedAudience"
              :wrap-cells="data.audiences_wrap" selection="single">
              <template v-slot:top="props">
                <div class="col-2 q-table__title">Audiences</div>
                <q-space />
                <div class="col" align="right">
                  <q-toggle v-model="data.audiences_wrap" label="Word wrap" />
                </div>
                <q-btn flat round dense :icon="props.inFullscreen ? 'fullscreen_exit' : 'fullscreen'"
                  @click="props.toggleFullscreen" class="q-ml-md" />
              </template>
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
                  <div v-if="data.audiences_wrap">
                    {{ formatArray(props.row.countries) }}
                  </div>
                  <div class="limited-width" v-if="!data.audiences_wrap">
                    {{ formatArray(props.row.countries) }}
                    <q-tooltip>{{ formatArray(props.row.countries) }}</q-tooltip>
                  </div>
                </q-td>
              </template>
              <template v-slot:bottom>
                <!-- <div class="row">
                  <div v-if="data.selectedAudience.length > 1" class="q-mx-lg col">
                    <q-icon size="2em" name="warning" color="red"/>You have more than one campaign targeted the audience.<br/>
                  </div>
                </div> -->
                <div v-if="data.selectedAudience.length && data.selectedAudience[0].campaigns.length">
                  <q-pagination v-if="data.selectedAudience[0].campaigns.length > 1" v-model="data.currentAdgroupIndex"
                    :min="1" :max="data.selectedAudience[0].campaigns.length" input direction-links />
                  <div class="row">
                    <div class="col">
                      adgroup: <b>{{ data.selectedAudience[0].campaigns[data.currentAdgroupIndex - 1].ad_group_name }}</b> (id: {{
                        data.selectedAudience[0].campaigns[data.currentAdgroupIndex - 1].ad_group_id }}),
                      campaign: <b>{{ data.selectedAudience[0].campaigns[data.currentAdgroupIndex - 1].campaign_name }}</b> (id: {{
                        data.selectedAudience[0].campaigns[data.currentAdgroupIndex - 1].campaign_id }}),
                        CID: {{  data.selectedAudience[0].campaigns[data.currentAdgroupIndex-1].customer_id }}
                    </div>
                  </div>
                </div>
              </template>
            </q-table>
          </div>
        </q-card-section>
        <q-card-section>
          <div class="row">
            <div class="col" align="right">
              <q-btn label="Recalculate" @click="onReclculateAudiencesLog" color="secondary" icon="repeat" class="q-my-md"
                align="right"></q-btn>
            </div>
          </div>
          <div class="">
            <q-table title="Upload history" class="qtable-sticky-header" style="height: 300px" flat bordered
              :rows="data.audience_log" row-key="name" :columns="data.audience_status_columns" virtual-scroll
              :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]" hide-bottom>
              <template v-slot:top="props">
                <div class="col-2 q-table__title">Upload history</div>
                <q-space />
                <q-btn flat round dense :icon="props.inFullscreen ? 'fullscreen_exit' : 'fullscreen'"
                  @click="props.toggleFullscreen" class="q-ml-md" />
              </template>
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
                <q-select filled v-model="data.conversions_selected_countries" multiple
                  :options="data.conversions_countries" label="Country" style="width: 250px" clearable />
              </div>

              <div class="col q-pa-xs">
                <q-banner class="bg-grey-3">
                  p-val: <q-badge>{{ formatFloat(data.pval, 6) }}</q-badge>
                  <br>If pval &lt;=0.05, then results are statistically significant
                </q-banner>
              </div>

            </div>
            <div class="row">
              <div class="col q-pa-xs">
                <q-btn label="Load conversions" @click="onLoadConversions" color="primary" icon="query_stats"
                  class="q-my-md"></q-btn>

                <q-btn-toggle class="q-mx-lg" v-model="data.conversions_mode" no-wrap outline alight="right" :options="[
                  { label: 'Conv Rate', value: 'cr' },
                  { label: 'Absolute', value: 'abs' },
                ]" />

                <q-btn label="Get query" @click="onGetConversionsQuery" class="q-my-md"></q-btn>
              </div>
            </div>
          </q-banner>
          <apexchart v-if="data.chart.series.length" :options="data.chart.options" :series="data.chart.series"
            height="600"></apexchart>
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

<style></style>

<script lang="ts">
import { defineComponent, ref, watch } from 'vue';
import { useQuasar } from 'quasar';
import { AudienceInfo, configurationStore } from 'stores/configuration';
import { postApi, postApiUi, getApiUi } from 'boot/axios';
import { formatArray, formatDate, formatFloat } from '../helpers/utils';

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
  cr_test: number;
  cr_control: number;
  cum_test_regs: number;
  cum_control_regs: number;
}
interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
  campaigns: any[]
}
enum GraphMode {
  cr = 'cr',
  abs = 'abs',
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
      currentAdgroupIndex: 1,
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
      include_log_duplicates: false,
      skip_ads: false,
      audience_status_columns: [
        //{ name: 'status', label: 'Status', field: 'status', sortable: true },
        { name: 'date', label: 'Date', field: 'date', sortable: true, format: (v: any) => formatDate(v, true) },
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
            type: 'line',
          },
          stroke: {
            curve: 'straight'
          },
          zoom: {
            enabled: true,
            type: 'x',
            autoScaleYaxis: true,
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
          markers: {
            size: 2
          },
          labels: [],
          xaxis: {
            //type: 'category',
            type: 'datetime',
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
      conversions_selected_countries: <string[]>[],
      conversions_countries: [],
      conversions_mode: GraphMode.cr,
      pval: <number | undefined>undefined,
    });

    function showExecutionResultDialog(results: Record<string, any>) {
      let html = '';
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

      for (const item of Object.entries(results)) {
        html += `<div class="text-subtitle1">Audience '${item[0]}' results:</div>`;
        const result = item[1];
        html += `<div class="text-caption"><ul>
              <li>Test user count: ${result.test_user_count}</li>
              <li>Control user count: ${result.control_user_count}</li>
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
    const onExecute = async () => {
      let res = await postApiUi('process', {}, $q, 'Running sampling and uploading...');
      if (res?.data.result) {
        showExecutionResultDialog(res.data.result);
      }
    };
    const onSampling = async () => {
      let res = await postApiUi('sampling/run', {}, $q, 'Running sampling for audiences...');
      /* Expect:
        "result": {
          "userlist1": {
            "control_count": 0,
            "test_count": 0
          }
        }
       */
      if (res?.data.result) {
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
        if (res.data && res.data.result) {
          showExecutionResultDialog(res.data.result);
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
      data.value.currentAdgroupIndex = 1;
      if (newValue && newValue.length) {
        let newActiveAudience = newValue[0];
        data.value.audience_log = newActiveAudience.log;
        data.value.conversions_selected_countries = [];
        data.value.conversions_countries = newActiveAudience.countries;
        updateConversionsChart(newActiveAudience.conversions);
      }
    });

    watch(() => data.value.conversions_mode, (newValue: any) => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        updateConversionsChart(audience.conversions);
      }
    });

    const onFetchAudiencesStatus = async () => {
      data.value.audiences = [];
      data.value.audience_log = [];
      let res = await getApiUi('audiences/status',
        { include_log_duplicates: data.value.include_log_duplicates, skip_ads: data.value.skip_ads },
        $q, 'Fetching audiences status...');
      if (!res?.data.result) return;
      const result = res.data.result;
      data.value.audiences_data = result;
      let audiences = <any[]>[];
      Object.keys(result).map(name => {
        const audience = result[name];
        let logs = audience.log;
        // convert dates from strings to Date objects
        if (logs) {
          logs = logs.map((i: any) => { i.date = new Date(i.date); return i; });
        }
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
          'log': logs,
          'campaigns': audience.campaigns,
        });
      });
      console.log(audiences)
      data.value.audiences = audiences;
      if (audiences.length > 0) {
        data.value.selectedAudience = [audiences[0]];
      }
    };

    const onReclculateAudiencesLog = async () => {
      $q.dialog({
        title: 'Confirm',
        message: 'Are you sure you want to rebuild audiences log?',
        cancel: true,
        persistent: true
      }).onOk(async () => {
        await postApiUi('audiences/recalculate_log', {}, $q, 'Recalculating...');
      });
    };

    const onLoadConversions = async () => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        let date_start = <string | undefined>data.value.conversions_from;
        let date_end = <string | undefined>data.value.conversions_to;
        let country = data.value.conversions_selected_countries;
        let country_str;
        if (country && country.length) {
          country_str = country.join(',');
        }
        audience.conversions = await loadConversions(audience.name, date_start, date_end, country_str);
        updateConversionsChart(audience.conversions);
      }
    };

    const onGetConversionsQuery = async () => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        let date_start = <string | undefined>data.value.conversions_from;
        let date_end = <string | undefined>data.value.conversions_to;
        let country = data.value.conversions_selected_countries;
        let country_str;
        if (country && country.length) {
          country_str = country.join(',');
        }
        let res = await getApiUi('conversions/query', { audience: audience.name, date_start, date_end, country: country_str }, $q, 'Fetching the audience conversion uquery...');
        if (!res?.data) {
          return;
        }
        console.log(res.data.query);
        $q.dialog({
          title: 'SQL Query for conversion calculation',
          message: res.data.query,
          ok: {
            push: true
          },
          class: 'text-pre',
          fullWidth: true
        });
      }
    };

    const loadConversions = async (audienceName: string, date_start: string | undefined, date_end: string | undefined, country: string | undefined): Promise<Conversions | undefined> => {
      data.value.chart.series = [];
      let res = await getApiUi('conversions', { audience: audienceName, date_start, date_end, country }, $q, 'Fetching the audience conversion history...');
      if (!res) return;
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
    };

    function formatGraphValue(val: any) {
      if (Number.isFinite(val)) {
        return GraphMode.cr ? formatFloat(val) : val;
      }
      return 0;
    }
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
        //const label = new Date(item.date);
        const label = formatDate(new Date(item.date));
        // NOTE: dates should not repeat otherwise there will be no graph
        graph_data[label] = {
          date: item.date,
          test: data.value.conversions_mode === GraphMode.cr ? item.cr_test : item.cum_test_regs,
          control: data.value.conversions_mode === GraphMode.cr ? item.cr_control : item.cum_control_regs
        };
      }
      const entries = Object.entries(graph_data);
      const test_data = entries.map(item => { return { x: item[1].date, y: formatGraphValue(item[1].test) }; });
      const control_data = entries.map(item => { return { x: item[1].date, y: formatGraphValue(item[1].control) }; });
      data.value.chart.series = [
        { name: 'treatment', data: test_data },
        { name: 'control', data: control_data },
      ]
    };

    const onOpenChart = async (props: any) => {
      const audience = props.row;
      data.value.selectedAudience = [audience];
      if (audience.conversions) {
        updateConversionsChart(audience.conversions);
      } else {
        onLoadConversions();
      }
    }

    return {
      store,
      data,
      onExecute,
      onSampling,
      onAudiencesUpload,
      onFetchAudiencesStatus,
      onReclculateAudiencesLog,
      onLoadConversions,
      onGetConversionsQuery,
      onOpenChart,
      formatArray,
      formatFloat,
    };
  }
});
</script>
