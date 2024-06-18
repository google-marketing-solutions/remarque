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
    <div class="row">
      <div class="text-h2">Google Ads</div>
    </div>
    <div class="row" style="margin-top: 20px">
      <q-btn
        label="Run All"
        @click="onExecute"
        :fab="true"
        color="primary"
        class="q-mx-md"
      ></q-btn>
      <q-btn
        label="Run sampling"
        @click="onSampling(undefined)"
        :fab="true"
        class="q-mx-md"
      ></q-btn>
      <q-btn
        label="Upload audiences"
        @click="onAudiencesUpload(undefined)"
        :fab="true"
        class="q-mx-md"
      ></q-btn>
    </div>

    <div class="q-mt-md">
      <q-card class="card" flat bordered>
        <q-card-section>
          <div class="text-h6">Audiences segments uploaded to Google Ads</div>
          <div>
            <q-banner class="bg-grey-3">
              <template v-slot:avatar>
                <q-icon
                  name="info"
                  color="primary"
                  size="md"
                  style="margin-right: 5px"
                />
                If you scheduled execution then for each defined audience there
                will be segments with sampled users uploaded to Google Ads as
                customer match user lists.
              </template>
            </q-banner>
          </div>
        </q-card-section>
        <q-card-actions class="q-pa-md">
          <q-btn
            label="Load"
            icon="download"
            size="md"
            @click="onFetchAudiencesStatus"
            color="primary"
            style="width: 130px"
            class="q-mr-lg"
          />
          <q-toggle
            v-model="data.include_log_duplicates"
            label="Include log duplicates"
            class="q-mx-md"
          />
          <q-toggle v-model="data.skip_ads" label="Skip Ads info"></q-toggle>
          <q-icon name="info" size="sm" color="grey"
            ><q-tooltip>Enabling can speed up the loading</q-tooltip></q-icon
          >

          <!-- <q-toggle v-model="data.only_active" label="Only active audiences"/> -->
        </q-card-actions>

        <q-card-section>
          <div class="">
            <q-table
              title="Audiences"
              class="qtable-sticky-header"
              style="height: 400px"
              flat
              bordered
              :rows="data.audiences"
              row-key="name"
              :columns="data.audiences_columns"
              virtual-scroll
              :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]"
              v-model:selected="data.selectedAudience"
              :wrap-cells="data.audiences_wrap"
              selection="single"
              :hide-bottom="true"
            >
              <template v-slot:top="props">
                <div class="col-2 q-table__title">Audiences</div>
                <q-space />
                <div class="col" align="right">
                  <q-toggle v-model="data.audiences_wrap" label="Word wrap" />
                </div>
                <q-btn
                  flat
                  round
                  dense
                  :icon="props.inFullscreen ? 'fullscreen_exit' : 'fullscreen'"
                  @click="props.toggleFullscreen"
                  class="q-ml-md"
                />
              </template>

              <template v-slot:body-cell-actions="props">
                <q-td :props="props">
                  <q-btn
                    dense
                    round
                    flat
                    color="grey"
                    @click="onOpenChart(props.row)"
                    icon="query_stats"
                  ></q-btn>
                  <q-btn-dropdown dense icon="electric_bolt">
                    <q-list>
                      <q-item
                        clickable
                        v-close-popup
                        @click="onProcessAudience(props.row, 'test')"
                      >
                        <q-item-section>
                          <q-item-label
                            >Sample &amp; Split &amp; Upload (as if
                            mode=test)</q-item-label
                          >
                        </q-item-section>
                      </q-item>
                      <q-item
                        clickable
                        v-close-popup
                        @click="onProcessAudience(props.row, 'prod')"
                      >
                        <q-item-section>
                          <q-item-label
                            >Sample &amp; Upload (as if mode=prod)</q-item-label
                          >
                        </q-item-section>
                      </q-item>
                      <!-- <q-item clickable v-close-popup @click="onSampling(props.row)">
                        <q-item-section>
                          <q-item-label>Sample &amp; Split</q-item-label>
                        </q-item-section>
                      </q-item>
                      <q-item clickable v-close-popup @click="onAudiencesUpload(props.row)">
                        <q-item-section>
                          <q-item-label>Upload</q-item-label>
                        </q-item-section>
                      </q-item> -->
                    </q-list>
                  </q-btn-dropdown>
                </q-td>
              </template>

              <template v-slot:body-cell-mode="props">
                <q-td :props="props">
                  <q-chip
                    :color="
                      props.row.mode === 'off'
                        ? 'red'
                        : props.row.mode === 'test'
                          ? 'blue'
                          : 'green'
                    "
                    text-color="white"
                    dense
                    class="text-weight-bolder"
                    square
                    >{{
                      props.row.mode === 'off'
                        ? 'Off'
                        : props.row.mode === 'test'
                          ? 'Test '
                          : 'Prod'
                    }}</q-chip
                  >
                </q-td>
              </template>

              <template v-slot:body-cell-countries="props">
                <q-td :props="props">
                  <div v-if="data.audiences_wrap">
                    {{ formatArray(props.row.countries) }}
                  </div>
                  <div class="limited-width" v-if="!data.audiences_wrap">
                    {{ formatArray(props.row.countries) }}
                    <q-tooltip>{{
                      formatArray(props.row.countries)
                    }}</q-tooltip>
                  </div>
                </q-td>
              </template>
            </q-table>
            <div
              v-if="
                data.selectedAudience.length &&
                data.selectedAudience[0].ads &&
                data.selectedAudience[0].ads.tree.length
              "
            >
              <q-pagination
                v-if="data.selectedAudience[0].ads.tree.length > 1"
                v-model="data.currentAdgroupIndex"
                :min="1"
                :max="data.selectedAudience[0].ads.tree.length"
                input
                direction-links
              />
              <q-splitter
                v-model="data.audience_adstree_splitter"
                class="q-table--bordered"
              >
                <template v-slot:before>
                  <div class="q-pa-md">
                    <q-tree
                      :nodes="[
                        data.selectedAudience[0].ads.tree[
                          data.currentAdgroupIndex - 1
                        ],
                      ]"
                      node-key="type"
                      selected-color="primary"
                      default-expand-all
                      v-model:selected="data.adsTreeSelectedNode"
                    >
                      <template v-slot:default-header="prop">
                        <div v-if="prop.node.status">
                          <div v-if="prop.node.status == 'ENABLED'">
                            <div class="text-positive">
                              {{ prop.node.label }}
                            </div>
                          </div>
                          <div v-if="prop.node.status == 'PAUSED'">
                            <div class="text-warning">
                              {{ prop.node.label }}
                            </div>
                          </div>
                        </div>
                        <div v-else>{{ prop.node.label }}</div>
                      </template>
                    </q-tree>
                  </div>
                </template>

                <template v-slot:after>
                  <q-tab-panels v-model="data.adsTreeSelectedNode">
                    <q-tab-panel name="campaign">
                      <div
                        v-html="
                          renderNodeInfo(
                            data.selectedAudience[0].ads.tree[
                              data.currentAdgroupIndex - 1
                            ],
                            'campaign',
                          )
                        "
                      ></div>
                    </q-tab-panel>
                    <q-tab-panel name="customer">
                      <div
                        v-html="
                          renderNodeInfo(
                            data.selectedAudience[0].ads.tree[
                              data.currentAdgroupIndex - 1
                            ],
                            'customer',
                          )
                        "
                      ></div>
                    </q-tab-panel>
                    <q-tab-panel name="ad_group">
                      <div
                        v-html="
                          renderNodeInfo(
                            data.selectedAudience[0].ads.tree[
                              data.currentAdgroupIndex - 1
                            ],
                            'ad_group',
                          )
                        "
                      ></div>
                    </q-tab-panel>
                    <q-tab-panel name="user_list">
                      <div
                        v-html="
                          renderNodeInfo(
                            data.selectedAudience[0].ads.tree[
                              data.currentAdgroupIndex - 1
                            ],
                            'user_list',
                          )
                        "
                      ></div>
                    </q-tab-panel>
                  </q-tab-panels>
                </template>
              </q-splitter>
            </div>
          </div>
        </q-card-section>

        <q-card-section>
          <q-expansion-item
            :default-opened="true"
            :label="data.isLogPanelExpanded ? '' : 'Upload history'"
            v-model="data.isLogPanelExpanded"
            style="font-size: 20px"
          >
            <div class="row">
              <div class="col" align="right">
                <q-btn
                  label="Recalculate"
                  @click="onReclculateAudiencesLog"
                  color="secondary"
                  icon="repeat"
                  class="q-my-md"
                  align="right"
                ></q-btn>
              </div>
            </div>
            <div class="">
              <q-table
                title="Upload history"
                class="qtable-sticky-header"
                style="height: 300px"
                flat
                bordered
                :rows="data.audience_log"
                row-key="name"
                :columns="data.audience_status_columns"
                virtual-scroll
                :pagination="{ rowsPerPage: 0 }"
                :rows-per-page-options="[0]"
                hide-bottom
              >
                <template v-slot:top="props">
                  <div class="col-2 q-table__title">Upload history</div>
                  <q-space />
                  <q-btn
                    flat
                    round
                    dense
                    :icon="
                      props.inFullscreen ? 'fullscreen_exit' : 'fullscreen'
                    "
                    @click="props.toggleFullscreen"
                    class="q-ml-md"
                  />
                </template>
              </q-table>
            </div>
          </q-expansion-item>
        </q-card-section>

        <q-card-section v-if="data.selectedAudience.length">
          <q-banner class="bg-grey-2">
            <div class="row">
              <div class="col q-pa-xs">
                <q-banner class="bg-grey-3">
                  <template v-slot:avatar
                    ><q-icon
                      name="info"
                      color="primary"
                      size="md"
                      style="margin-right: 5px"
                    />
                    If you don't specify the start date then the day of first
                    upload to Google Ads will be used.<br />
                    If you don't specify the end date then yesterday will be
                    used.
                  </template>
                </q-banner>
              </div>
            </div>
            <div class="row">
              <div class="col q-pa-xs" style="max-width: 250px">
                <q-input
                  filled
                  v-model="data.conversions_from"
                  mask="####-##-##"
                  label="Start date"
                  clearable
                >
                  <template v-slot:append>
                    <q-icon name="event" class="cursor-pointer">
                      <q-popup-proxy
                        ref="qStartProxy"
                        cover
                        transition-show="scale"
                        transition-hide="scale"
                      >
                        <q-date
                          v-model="data.conversions_from"
                          mask="YYYY-MM-DD"
                          :no-unset="true"
                          @update:model-value="$refs.qStartProxy.hide()"
                        >
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col q-pa-xs" style="max-width: 250px">
                <q-input
                  filled
                  v-model="data.conversions_to"
                  mask="####-##-##"
                  label="End date"
                  clearable
                >
                  <template v-slot:append>
                    <q-icon name="event" class="cursor-pointer">
                      <q-popup-proxy
                        ref="qEndProxy"
                        cover
                        transition-show="scale"
                        transition-hide="scale"
                      >
                        <q-date
                          v-model="data.conversions_to"
                          mask="YYYY-MM-DD"
                          today-btn
                          :no-unset="true"
                          @update:model-value="$refs.qEndProxy.hide()"
                        >
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col q-pa-xs" style="width: 250px">
                <q-select
                  filled
                  v-model="data.conversions_selected_countries"
                  multiple
                  :options="data.conversions_countries"
                  label="Country"
                  clearable
                />
              </div>
              <div class="col q-pa-xs">
                <q-input
                  filled
                  v-model="data.conversions_events"
                  label="Conv. event"
                  clearable
                />
              </div>
              <div class="col q-pa-xs">
                <q-banner class="bg-grey-3">
                  p-val:
                  <q-badge :color="data.pval <= 0.05 ? 'green' : 'blue'">{{
                    formatFloat(data.pval, 6)
                  }}</q-badge>
                  <br />If pval &lt;=0.05, then results are statistically
                  significant
                </q-banner>
              </div>
            </div>
            <div class="row">
              <div class="col q-pa-xs">
                <q-btn
                  label="Load conversions"
                  @click="onLoadConversions"
                  color="primary"
                  icon="query_stats"
                  class="q-my-md"
                ></q-btn>
                <q-toggle
                  v-model="data.load_ads_graph"
                  label="Load Ads metrics"
                  :disable="
                    !(
                      data.selectedAudience[0].ads &&
                      data.selectedAudience[0].ads.adgroups.length > 0
                    )
                  "
                />
                <q-btn-toggle
                  class="q-mx-lg"
                  v-model="data.conversions_mode"
                  no-wrap
                  outline
                  alight="right"
                  :options="[
                    { label: 'Conv Rate', value: 'cr' },
                    { label: 'Absolute', value: 'abs' },
                  ]"
                />

                <q-btn
                  label="Get query"
                  @click="onGetConversionsQuery"
                  class="q-my-md"
                ></q-btn>
              </div>
            </div>
          </q-banner>
          <apexchart
            v-if="data.chart.series.length"
            :options="data.chart.options"
            :series="data.chart.series"
            height="600"
          >
          </apexchart>
          <apexchart
            v-if="data.chartAds.series.length"
            :options="data.chartAds.options"
            :series="data.chartAds.series"
            height="600"
          ></apexchart>
          <div
            v-if="
              data.selectedAudience[0].ads &&
              data.selectedAudience[0].ads.campaigns.length &&
              data.selectedAudience[0].conversions &&
              data.selectedAudience[0].conversions.ads_metrics
            "
          >
            <q-pagination
              v-model="data.currentCampaignIndex"
              :min="1"
              :max="data.selectedAudience[0].ads.campaigns.length"
              input
              direction-links
            />
            <div
              v-html="
                data.selectedAudience[0].ads.campaigns[
                  data.currentCampaignIndex - 1
                ].campaign_name +
                ' (' +
                data.selectedAudience[0].ads.campaigns[
                  data.currentCampaignIndex - 1
                ].campaign_id +
                ')'
              "
            ></div>
          </div>
        </q-card-section>
      </q-card>
    </div>
  </q-page>
  <q-dialog v-model="data.resultDialog.show">
    <q-card style="width: 300px" class="q-px-sm q-pb-md">
      <q-card-section>
        <div class="text-h6">{{ data.resultDialog.header }}</div>
      </q-card-section>
      <q-card-section
        ><span v-html="data.resultDialog.message"></span
      ></q-card-section>
      <q-separator />
      <q-card-actions align="right">
        <q-btn v-close-popup flat color="primary" label="Close" />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<style></style>

<script lang="ts">
import { computed, defineComponent, ref, watch } from 'vue';
import { useQuasar } from 'quasar';
import {
  AudienceInfo,
  AudienceMode,
  configurationStore,
} from 'stores/configuration';
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
  ads_metrics?: Record<string, AdsMetric[]>;
}
interface ConversionsData {
  date: string;
  cr_test: number;
  cr_control: number;
  cum_test_regs: number;
  cum_control_regs: number;
}
interface AdsMetric {
  date: string;
  unique_users: number;
  clicks: number;
  average_impression_frequency_per_user?: number;
}
interface AudienceWithLog extends AudienceInfo {
  log?: AudienceLog[];
  conversions?: Conversions;
  ads: {
    campaigns: any[];
    adgroups: any[];
    tree: any;
  };
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
      audiences: [] as AudienceWithLog[], //TODO: use store.audiences,
      audiences_data: {},
      audience_log: [] as AudienceLog[] | undefined,
      selectedAudience: [] as AudienceWithLog[],
      currentAdgroupIndex: 1,
      currentCampaignIndex: 1,
      adsTreeSelectedNode: null,
      audiences_columns: [
        { name: 'mode', label: 'Mode', field: 'mode' },
        { name: 'name', label: 'Name', field: 'name', sortable: true },
        { name: 'app_id', label: 'App id', field: 'app_id', sortable: true },
        {
          name: 'countries',
          label: 'Countries',
          field: 'countries',
          sortable: true,
          format: formatArray,
        },
        {
          name: 'events_include',
          label: 'Include events',
          field: 'events_include',
          sortable: true,
          format: formatArray,
        },
        {
          name: 'events_exclude',
          label: 'Exclude events',
          field: 'events_exclude',
          sortable: true,
          format: formatArray,
        },
        { name: 'days_ago_start', label: 'Start', field: 'days_ago_start' },
        { name: 'days_ago_end', label: 'End', field: 'days_ago_end' },
        { name: 'ttl', label: 'TTL', field: 'ttl' },
        {
          name: 'created',
          label: 'Created',
          field: 'created',
          format: formatDate,
        },
        { name: 'actions', label: 'Actions', field: '' },
      ],
      audiences_wrap: true,
      include_log_duplicates: false,
      skip_ads: false,
      audience_status_columns: [
        //{ name: 'status', label: 'Status', field: 'status', sortable: true },
        {
          name: 'date',
          label: 'Date',
          field: 'date',
          sortable: true,
          format: (v: any) => formatDate(v, true),
        },
        {
          name: 'test_user_count',
          label: 'Test Users',
          field: 'test_user_count',
          sortable: true,
        },
        {
          name: 'control_user_count',
          label: 'Control Users',
          field: 'control_user_count',
          sortable: true,
        },
        {
          name: 'uploaded_user_count',
          label: 'Uploaded Users',
          field: 'user_count',
          sortable: true,
        },
        {
          name: 'new_test_user_count',
          label: 'New Test Users',
          field: 'new_test_user_count',
          sortable: true,
        },
        {
          name: 'new_control_user_count',
          label: 'New Control Users',
          field: 'new_control_user_count',
          sortable: true,
        },
        {
          name: 'total_test_user_count',
          label: 'Total Test Users',
          field: 'total_test_user_count',
          sortable: true,
        },
        {
          name: 'total_control_user_count',
          label: 'Total Control Users',
          field: 'total_control_user_count',
          sortable: true,
        },
        {
          name: 'job_status',
          label: 'Job Status',
          field: 'job_status',
          sortable: true,
        },
        {
          name: 'job_failure',
          label: 'Job Failure',
          field: 'job_failure',
          sortable: true,
        },
      ],
      audience_adstree_splitter: 70,
      isLogPanelExpanded: true,
      load_ads_graph: true,
      chart: {
        options: {
          chart: {
            type: 'line',
          },
          stroke: {
            curve: 'straight',
          },
          zoom: {
            enabled: true,
            type: 'x',
            autoScaleYaxis: true,
          },
          title: {
            text: 'Conversions',
            align: 'left',
          },
          grid: {
            row: {
              colors: ['#f3f3f3', 'transparent'],
              opacity: 0.5,
            },
          },
          markers: {
            size: 2,
          },
          labels: [],
          xaxis: {
            //type: 'category',
            type: 'datetime',
          },
          yaxis: [
            {
              title: {
                text: 'Conversions',
              },
              axisBorder: {
                show: true,
                color: '#FF1654',
              },
            },
          ],
        },
        series: [] as any,
      },
      chartAds: {
        options: {
          chart: {
            type: 'line',
          },
          stroke: {
            curve: 'straight',
          },
          zoom: {
            enabled: true,
            type: 'x',
            autoScaleYaxis: true,
          },
          title: {
            text: 'Ads campaign metrics',
            align: 'left',
          },
          grid: {
            row: {
              colors: ['#f3f3f3', 'transparent'],
              opacity: 0.5,
            },
          },
          markers: {
            size: 2,
          },
          xaxis: {
            type: 'datetime',
          },
          yaxis: [
            {
              seriesName: 'users',
              title: {
                text: 'Unique Users',
              },
              axisBorder: {
                show: true,
              },
            },
            {
              opposite: true,
              seriesName: 'clicks',
              title: {
                text: 'Clicks',
              },
              axisBorder: {
                show: true,
              },
            },
          ],
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
      conversions_countries: [] as string[],
      conversions_events: '',
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
      data.value.resultDialog.header =
        'Audience uploading to Google Ads completed';
      data.value.resultDialog.message = html;
      data.value.resultDialog.show = true;
    }
    const onExecute = async () => {
      let res = await postApiUi(
        'process',
        {},
        'Running sampling and uploading...',
      );
      if (res?.data.result) {
        showExecutionResultDialog(res.data.result);
      }
    };
    const onProcessAudience = async (
      audience: AudienceWithLog,
      mode: string,
    ) => {
      let res = await postApiUi(
        'process',
        { audience: audience.name, mode: mode },
        'Processing the audience...',
      );
      if (res?.data.result) {
        showExecutionResultDialog(res.data.result);
      }
    };
    const onSampling = async (audience?: AudienceWithLog) => {
      let res = await postApiUi(
        'sampling/run',
        { audience: audience ? audience.name : null },
        audience
          ? 'Running sampling for the audience...'
          : 'Running sampling for audiences...',
      );
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
    const onAudiencesUpload = async (audience?: AudienceWithLog) => {
      const progressDlg = $q.dialog({
        message: audience
          ? 'Uploading the audience to Google Ads...'
          : 'Uploading audiences to Google Ads...',
        progress: true, // we enable default settings
        persistent: true, // we want the user to not be able to close it
        ok: false, // we want the user to not be able to close it
      });
      const loading = () => progressDlg.hide();
      try {
        let res = await postApi(
          'ads/upload',
          { audience: audience ? audience.name : null },
          loading,
        );
        if (res.data && res.data.result) {
          showExecutionResultDialog(res.data.result);
        }
      } catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };

    watch(
      () => data.value.selectedAudience,
      (newValue: any[]) => {
        data.value.currentAdgroupIndex = 1;
        if (newValue && newValue.length) {
          let newActiveAudience = <AudienceWithLog>newValue[0];
          data.value.audience_log = newActiveAudience.log;
          data.value.conversions_selected_countries = [];
          data.value.conversions_events = '';
          data.value.conversions_countries = newActiveAudience.countries;
          updateConversionsChart(newActiveAudience.conversions);
          data.value.load_ads_graph =
            newActiveAudience.ads && newActiveAudience.ads.campaigns.length > 0;
        }
      },
    );

    watch(
      () => data.value.currentCampaignIndex,
      (newValue: number) => {
        if (data.value.selectedAudience && data.value.selectedAudience.length) {
          const audience = data.value.selectedAudience[0];
          if (
            audience.ads &&
            audience.conversions &&
            audience.conversions.ads_metrics
          ) {
            updateAdsMetricsChart(audience.conversions.ads_metrics);
          }
        }
      },
    );

    watch(
      () => data.value.conversions_mode,
      (newValue: any) => {
        if (data.value.selectedAudience && data.value.selectedAudience.length) {
          const audience = data.value.selectedAudience[0];
          updateConversionsChart(audience.conversions);
        }
      },
    );

    function getNodeInfo(obj: any, prefix: string) {
      const keys = Object.keys(obj).filter(
        (n) =>
          n.startsWith(prefix + '_') &&
          ['id', 'name', 'status'].indexOf(n.substring(prefix.length + 1)) ==
            -1,
      );
      let info: any = {};
      for (let key of keys) {
        info[key] = obj[key];
      }
      return keys.length ? info : null;
    }
    const onFetchAudiencesStatus = async () => {
      data.value.audiences = [];
      data.value.audience_log = [];
      let res = await getApiUi(
        'audiences/status',
        {
          include_log_duplicates: data.value.include_log_duplicates,
          skip_ads: data.value.skip_ads,
        },
        'Fetching audiences status...',
      );
      if (!res?.data.result) return;
      const result = res.data.result;
      let audiences = <any[]>[];
      Object.keys(result).map((name) => {
        const audience = result[name];
        let logs = audience.log;
        // convert dates from strings to Date objects
        if (logs) {
          logs = logs.map((i: any) => {
            i.date = new Date(i.date);
            return i;
          });
        }
        let ads = {
          campaigns: [],
          adgroups: [],
          tree: [],
        };
        if (audience.campaigns) {
          let campaigns = audience.campaigns.reduce((r: any, a: any) => {
            r[a.campaign_id] = r[a.campaign_id] || {};
            r[a.campaign_id].campaign_id = a.campaign_id;
            r[a.campaign_id].customer_id = a.customer_id;
            r[a.campaign_id].campaign_name = a.campaign_name;
            return r;
          }, Object.create(null));
          ads = {
            campaigns: Object.values(campaigns),
            adgroups: audience.campaigns,
            tree: audience.campaigns.map((i) => {
              return {
                label: `CID ${i.customer_id} - ${i.customer_name}`,
                type: 'customer',
                id: i.customer_id,
                selected: true,
                info: getNodeInfo(i, 'customer'),
                children: [
                  {
                    label: `Campaign ${i.campaign_id} - ${i.campaign_name} (${i.campaign_status})`,
                    status: i.campaign_status,
                    id: i.campaign_id,
                    type: 'campaign',
                    info: getNodeInfo(i, 'campaign'),
                    children: [
                      {
                        label: `AdGroup ${i.ad_group_id} - ${i.ad_group_name} (${i.ad_group_status})`,
                        status: i.ad_group_status,
                        id: i.ad_group_id,
                        type: 'ad_group',
                        info: getNodeInfo(i, 'ad_group'),
                        children: [
                          {
                            label: `UserList ${i.user_list_id} - ${i.user_list_name}`,
                            id: i.user_list_id,
                            type: 'user_list',
                            info: getNodeInfo(i, 'user_list'),
                          },
                        ],
                      },
                    ],
                  },
                ],
              };
            }),
          };
        }
        audiences.push({
          mode: audience.mode,
          name: audience.name,
          app_id: audience.app_id,
          countries: audience.countries,
          events_include: audience.events_include,
          events_exclude: audience.events_exclude,
          days_ago_start: audience.days_ago_start,
          days_ago_end: audience.days_ago_end,
          user_list: audience.user_list,
          ttl: audience.ttl,
          created: audience.created,
          log: logs,
          ads: ads,
        });
      });
      console.log(audiences);
      data.value.audiences = audiences;
      if (audiences.length > 0) {
        data.value.selectedAudience = [audiences[0]];
      }
    };

    const onRecalculateAudiencesLog = async () => {
      $q.dialog({
        title: 'Confirm',
        message: 'Are you sure you want to rebuild audiences log?',
        cancel: true,
        persistent: true,
      }).onOk(async () => {
        await postApiUi('audiences/recalculate_log', {}, 'Recalculating...');
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
        let events = data.value.conversions_events;
        audience.conversions = await loadConversions(
          audience.name,
          date_start,
          date_end,
          country_str,
          events,
          data.value.load_ads_graph ? audience.ads.campaigns : null,
        );
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
        let res = await getApiUi(
          'conversions/query',
          {
            audience: audience.name,
            date_start,
            date_end,
            country: country_str,
          },
          'Fetching the audience conversion uquery...',
        );
        if (!res?.data) {
          return;
        }
        console.log(res.data.query);
        $q.dialog({
          title: 'SQL Query for conversion calculation',
          message: res.data.query,
          ok: {
            push: true,
          },
          class: 'text-pre',
          fullWidth: true,
        });
      }
    };

    const loadConversions = async (
      audienceName: string,
      date_start: string | undefined,
      date_end: string | undefined,
      country: string | undefined,
      events: string | undefined,
      campaigns?: any,
    ): Promise<Conversions | undefined> => {
      data.value.chart.series = [];
      data.value.chartAds.series = [];
      // NOTE: if 'campaigns' is specified it says that we want to fetch campaign's metrics
      let res = await postApiUi(
        'conversions',
        {
          audience: audienceName,
          date_start,
          date_end,
          country,
          events,
          campaigns,
        },
        'Fetching the audience conversion history...',
      );
      if (!res) return;
      const results = res.data.results;
      let result;
      if (results) {
        result = results[audienceName];
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
      let ads_metrics_grouped = result.ads_metrics
        ? result.ads_metrics.reduce((r: any, a: any) => {
            r[a.campaign] = r[a.campaign] || [];
            r[a.campaign].push(a);
            return r;
          }, Object.create(null))
        : {};

      return {
        data: result.conversions,
        start_date: result.date_start,
        end_date: result.date_end,
        pval: result.pval,
        ads_metrics: ads_metrics_grouped,
      };
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
        data.value.chartAds.series = [];
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
          test:
            data.value.conversions_mode === GraphMode.cr
              ? item.cr_test
              : item.cum_test_regs,
          control:
            data.value.conversions_mode === GraphMode.cr
              ? item.cr_control
              : item.cum_control_regs,
        };
      }
      const entries = Object.entries(graph_data);
      const test_data = entries.map((item) => {
        return { x: item[1].date, y: formatGraphValue(item[1].test) };
      });
      const control_data = entries.map((item) => {
        return { x: item[1].date, y: formatGraphValue(item[1].control) };
      });
      data.value.chart.series = [
        { name: 'treatment', data: test_data },
        { name: 'control', data: control_data },
      ];
      if (conversions.ads_metrics) {
        updateAdsMetricsChart(conversions.ads_metrics);
      }
    };

    const updateAdsMetricsChart = (
      ads_metrics: Record<string, AdsMetric[]>,
    ) => {
      const audience = data.value.selectedAudience[0];
      if (!audience.ads) {
        // TODO: clear graph
      } else {
        const campaign =
          audience.ads.campaigns[data.value.currentCampaignIndex - 1];
        const ads_metrics_item = ads_metrics[campaign.campaign_id];
        if (ads_metrics_item) {
          data.value.chartAds.series = [
            {
              name: 'users',
              data: ads_metrics_item.map((i) => {
                return { x: i.date, y: i.unique_users };
              }),
            },
            {
              name: 'clicks',
              data: ads_metrics_item.map((i) => {
                return { x: i.date, y: i.clicks };
              }),
            },
          ];
        } else {
          data.value.chartAds.series = [];
        }
      }
    };

    const onOpenChart = async (audience: AudienceWithLog) => {
      data.value.selectedAudience = [audience];
      if (audience.conversions) {
        updateConversionsChart(audience.conversions);
      } else {
        onLoadConversions();
      }
    };

    const getAdsTreeNode = (node: any, nodeKey: string): any | undefined => {
      if (node.type == nodeKey) {
        return node;
      }
      if (node.children) {
        for (let child of node.children) {
          const res = getAdsTreeNode(child, nodeKey);
          if (res) return res;
        }
      }
    };

    const renderNodeInfo = (node: any, nodeKey: string): string | undefined => {
      node = getAdsTreeNode(node, nodeKey);
      if (node && node.info) {
        let html = '';
        for (let key of Object.keys(node.info)) {
          const prop = key.substring(nodeKey.length + 1).replace('_', ' ');
          if (prop === 'link') {
            html =
              `<li><a href='${node.info[key]}' target='_blank'>Open in Google Ads</a></li>` +
              html;
          } else {
            html += '<li>' + prop + ': ' + node.info[key] + '</li>';
          }
        }
        if (html) {
          html = '<ul>' + html + '</ul>';
        }
        return html;
      }
    };

    return {
      store,
      data,
      onExecute,
      onProcessAudience,
      onSampling,
      onAudiencesUpload,
      onFetchAudiencesStatus,
      onReclculateAudiencesLog: onRecalculateAudiencesLog,
      onLoadConversions,
      onGetConversionsQuery,
      onOpenChart,
      formatArray,
      formatFloat,
      renderNodeInfo,
    };
  },
});
</script>
