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
  <q-page class="items-center justify-evenly" padding>
    <div class="row">
      <div class="text-h2">Google Ads</div>
    </div>
    <div class="row" style="margin-top: 20px">
      <q-btn
        label="Process audiences"
        @click="onExecute"
        :fab="true"
        color="primary"
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
            icon="sync"
            size="md"
            @click="onFetchAudiencesStatus"
            color="primary"
            style="width: 130px"
            class="q-mr-lg"
          />
          <q-toggle
            v-model="data.includeLogDuplicates"
            label="Include log duplicates"
            class="q-mx-md"
          />
          <q-toggle v-model="data.skipLoadAds" label="Skip Ads info"></q-toggle>
          <q-icon name="info" size="sm" color="grey"
            ><q-tooltip>Enabling can speed up the loading</q-tooltip></q-icon
          >

          <!-- <q-toggle v-model="data.only_active" label="Only active audiences"/> -->
        </q-card-actions>

        <q-card-section>
          <div class="row q-col-gutter-md q-mb-md">
            <div class="col-12 col-md-6">
              <q-input
                v-model="data.audienceNameFilter"
                label="Filter by audience name"
                dense
                clearable
                debounce="300"
              >
                <template v-slot:append>
                  <q-icon name="search" />
                </template>
              </q-input>
            </div>
            <div class="col-12 col-md-6">
              <q-select
                v-model="data.countryFilter"
                :options="uniqueCountries"
                label="Filter by country"
                dense
                clearable
                multiple
                use-chips
              />
            </div>
          </div>
          <div class="">
            <q-table
              title="Audiences"
              class="qtable-sticky-header"
              style="height: 400px"
              flat
              bordered
              :rows="filteredAudiences"
              row-key="name"
              :columns="data.audiencesColumns"
              virtual-scroll
              :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]"
              v-model:selected="data.selectedAudience"
              :wrap-cells="data.audiencesTableTextWrap"
              selection="single"
              :hide-bottom="true"
            >
              <template v-slot:top="props">
                <div class="col-2 q-table__title">Audiences</div>
                <q-space />
                <div class="col" align="right">
                  <q-toggle
                    v-model="data.audiencesTableTextWrap"
                    label="Word wrap"
                  />
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
                  <q-btn-dropdown dense icon="electric_bolt">
                    <q-list>
                      <q-item
                        clickable
                        v-close-popup
                        @click="onOpenChart(props.row)"
                      >
                        <q-item-section>
                          <q-item-label>Open conversions graph</q-item-label>
                        </q-item-section>
                      </q-item>
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
                  <div v-if="data.audiencesTableTextWrap">
                    {{ formatArray(props.row.countries) }}
                  </div>
                  <div
                    class="limited-width"
                    v-if="!data.audiencesTableTextWrap"
                  >
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
                data.selectedAudience &&
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
                v-model="data.audienceAdsTreeSplitterWith"
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
            :label="
              data.isLogPanelExpanded ? '' : 'Upload history (click to expand)'
            "
            v-model="data.isLogPanelExpanded"
            style="font-size: 20px"
          >
            <div class="row">
              <div class="col" align="right">
                <q-btn
                  label="Recalculate"
                  @click="onRecalculateAudiencesLog"
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
                :rows="
                  (data.selectedAudience.length
                    ? data.selectedAudience[0].log
                    : []) || []
                "
                row-key="name"
                :columns="data.currentAudienceLogColumns"
                :no-data-label="
                  data.selectedAudience && data.selectedAudience.length
                    ? data.selectedAudience[0].log
                      ? 'Audience has no uploads'
                      : 'Audiences logs are not loaded. Please reload audiences'
                    : 'Please select an audience'
                "
                virtual-scroll
                :pagination="{ rowsPerPage: 0 }"
                :rows-per-page-options="[0]"
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

        <q-card-section
          v-if="data.selectedAudience && data.selectedAudience.length"
        >
          <q-banner class="bg-grey-2">
            <div class="text-h6">Conversions</div>
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
            <div class="row justify-start">
              <div class="col col-xl-2 q-pa-xs">
                <q-input
                  filled
                  v-model="data.conversionsFilterStartDate"
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
                          v-model="data.conversionsFilterStartDate"
                          mask="YYYY-MM-DD"
                          :no-unset="true"
                          @update:model-value="
                            ($refs.qStartProxy as any).hide()
                          "
                        >
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col col-xl-2 q-pa-xs">
                <q-input
                  filled
                  v-model="data.conversionsFilterEndDate"
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
                          v-model="data.conversionsFilterEndDate"
                          mask="YYYY-MM-DD"
                          today-btn
                          :no-unset="true"
                          @update:model-value="($refs.qEndProxy as any).hide()"
                        >
                        </q-date>
                      </q-popup-proxy>
                    </q-icon>
                  </template>
                </q-input>
              </div>
              <div class="col col-lg-3 q-pa-xs" style="width: 250px">
                <q-select
                  filled
                  v-model="data.conversionsFilterCountries"
                  multiple
                  :options="data.conversionsFilterCountriesOptions"
                  label="Country"
                  clearable
                />
              </div>
              <div class="col col-lg-3 q-pa-xs">
                <q-input
                  filled
                  v-model="data.conversionsFilterEvents"
                  label="Conv. event"
                  hint="By default 'exclude events' is used but you can enter any other event (please make sure they are in GA dataset)"
                  clearable
                  hide-bottom-space
                />
              </div>
            </div>
            <div class="row justify-start">
              <div class="col col-6 col-lg-3 q-pa-xs">
                <q-btn-toggle
                  style="margin-left: 0"
                  v-model="data.conversionsType"
                  no-wrap
                  outline
                  alight="right"
                  :options="[
                    { label: 'users', value: 'users' },
                    { label: 'events', value: 'events' },
                    { label: 'value', value: 'value' },
                  ]"
                />
                &nbsp;
                <q-icon name="info" size="sm" color="grey"
                  ><q-tooltip
                    >Conversion type: <br /><strong>Users</strong> shows numbers
                    of unique users who made conversion events. <br />
                    <strong>Events</strong> shows numbers of conversion
                    events.<br />
                    <strong>Value</strong> shows summary conversion value.
                  </q-tooltip></q-icon
                >
              </div>
              <div class="col col-6 col-lg-4 q-pa-xs">
                <q-btn-toggle
                  v-model="data.conversionsMode"
                  no-wrap
                  outline
                  alight="right"
                  :options="getConversionsModeOptions()"
                />
              </div>
              <div class="col col-6 col-lg-3 q-pa-xs">
                <q-btn-toggle
                  style="margin-left: 0"
                  v-model="data.conversionsCalcStrategy"
                  no-wrap
                  outline
                  alight="right"
                  :options="[
                    { label: 'Bounded', value: 'bounded' },
                    { label: 'Unbounded', value: 'unbounded' },
                  ]"
                />
                &nbsp;
                <q-icon name="info" size="sm" color="grey"
                  ><q-tooltip
                    ><strong>Bounded</strong> means we take conversions only for
                    days when users were included in the treatment group. <br />
                    <strong>Unbounded</strong> means we take conversions for all
                    time after users got into the treatment group.
                    <br />Changing the calculation strategy requires reloading.
                  </q-tooltip></q-icon
                >
              </div>
              <div class="col col-6 col-lg-2">
                <q-input
                  v-model="data.conversionsFilterConvWindow"
                  label="Conv. window"
                  hint="Conv. window in days for unbounded strategy, by default 14"
                  type="number"
                  min="1"
                  clearable
                  :disable="data.conversionsCalcStrategy !== 'unbounded'"
                />
              </div>
              <div class="col q-pa-xs"></div>
            </div>
            <div class="row items-center">
              <div class="col col-4 col-md-3 q-pa-xs">
                <q-btn
                  label="Load conversions"
                  @click="onLoadConversions"
                  color="primary"
                  icon="query_stats"
                  class="q-my-md"
                ></q-btn>
              </div>
              <div class="col col-4 col-md-3q-pa-xs">
                <q-toggle
                  v-model="data.loadAdsGraph"
                  label="Load Ads metrics"
                  :disable="
                    !(
                      data.selectedAudience[0].ads &&
                      data.selectedAudience[0].ads.adgroups.length > 0
                    )
                  "
                />
              </div>
              <div class="col col-4 col-md-3 q-pa-xs">
                <q-btn
                  label="Get query"
                  @click="onGetConversionsQuery"
                  class="q-my-md q-px-xl"
                ></q-btn>
              </div>
              <div class="col q-pa-xs"></div>
            </div>
          </q-banner>
          <apexchart
            v-if="data.chart.series.length"
            :options="data.chart.options"
            :series="data.chart.series"
            height="600"
          >
          </apexchart>
          <div class="row">
            <div class="col q-pa-xs">
              <q-banner class="bg-grey-3">
                p-value:
                <q-badge
                  :color="data.pval && data.pval <= 0.05 ? 'green' : 'blue'"
                  >{{ formatFloat(data.pval, 6) }}</q-badge
                >
                <br />If p-value &lt;=0.05, then results are statistically
                significant
              </q-banner>
            </div>
          </div>
          <div class="row q-pt-sm">
            <div class="q-table-container">
              <q-markup-table separator="horizontal" :wrap-cells="true">
                <thead>
                  <tr>
                    <th class="text-left">Group</th>
                    <th class="text-right">Total user count</th>
                    <th class="text-right">Converted users</th>
                    <th class="text-right">CR of users, %</th>
                    <th class="text-right">Conversions (events)</th>
                    <th class="text-right">Avg conversions per session</th>
                    <th class="text-right">Avg conversions per user</th>
                    <th class="text-right">Total conv. value, $</th>
                    <th class="text-right">Avg value per user, $</th>
                    <th class="text-right">Avg value per conv, $</th>
                    <th class="text-right">Total session count</th>
                    <th class="text-right">Avg sessions per user</th>
                  </tr>
                </thead>
                <tbody v-if="data.conversionsSummary">
                  <tr>
                    <td class="text-left">Test</td>
                    <td class="text-right">
                      {{ data.conversionsSummary.total_test_user_count }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_test_users }}
                    </td>
                    <td class="text-right">
                      {{ formatFloat(data.conversionsSummary.cr_test * 100) }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_test_events }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.events_per_session_test,
                          5,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.events_per_user_test,
                          5,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_test_conv_value }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.value_per_user_test,
                          4,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.value_per_event_test,
                          4,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.test_session_count }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.test_session_count /
                            data.conversionsSummary.total_test_user_count,
                        )
                      }}
                    </td>
                  </tr>
                  <tr>
                    <td class="text-left">Control</td>
                    <td class="text-right">
                      {{ data.conversionsSummary.total_control_user_count }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_control_users }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(data.conversionsSummary.cr_control * 100)
                      }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_control_events }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.events_per_session_control,
                          5,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.events_per_user_control,
                          5,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.cum_control_conv_value }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.value_per_user_control,
                          4,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.value_per_event_control,
                          4,
                        )
                      }}
                    </td>
                    <td class="text-right">
                      {{ data.conversionsSummary.control_session_count }}
                    </td>
                    <td class="text-right">
                      {{
                        formatFloat(
                          data.conversionsSummary.control_session_count /
                            data.conversionsSummary.total_control_user_count,
                        )
                      }}
                    </td>
                  </tr>
                </tbody>
              </q-markup-table>
            </div>
          </div>
          <apexchart
            v-if="data.chartAds.series.length"
            :options="data.chartAds.options"
            :series="data.chartAds.series"
            height="600"
          ></apexchart>
          <div
            v-if="
              data.selectedAudience &&
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
  <ProcessingDialog
    v-model="data.showProcessingDialog"
    :audiences="audiences"
  />
</template>

<style>
/* .col {
  border: 1px solid #000;
} */
</style>

<script lang="ts">
import { defineComponent, ref, watch, nextTick, computed } from 'vue';
import { useQuasar } from 'quasar';
import ProcessingDialog from 'components/ProcessingDialog.vue';
import { useConfigurationStore } from 'stores/configuration';
import {
  AudienceWithLog,
  Conversions,
  ConversionsData,
  AdsMetric,
  useAudiencesStore,
  AudienceLog,
  CampaignInfo,
  UserlistAssignmentData,
  AdsTreeData,
  AdsTreeNodeType,
} from 'stores/audiences';
import { postApiUi, getApiUi } from 'boot/axios';
import { formatArray, formatDate, formatFloat } from '../helpers/utils';
import { ApexOptions } from 'apexcharts';
import {
  AudienceConversionsQueryResponse,
  AudienceConversionsResponse,
  AudiencesLogRebuildResponse,
  AudiencesProcessResponse,
  AudiencesProcessResult,
  AudiencesStatusResponse,
} from 'src/boot/api';

enum ConversionType {
  USERS = 'users',
  EVENTS = 'events',
  VALUE = 'value',
}
enum ConversionUsersMode {
  CONV_RATE = 'cr',
  ABSOLUTE = 'users_abs',
}
enum ConversionEventsMode {
  ABSOLUTE = 'events_abs',
  AVG_BY_USER = 'events_by_user',
  AVG_BY_SESSION = 'events_by_session',
}
enum ConversionValueMode {
  ABSOLUTE = 'value_abs',
  AVG_BY_USER = 'value_by_user',
  AVG_BY_EVENT = 'value_by_event',
}
enum ConversionCalcStrategy {
  BOUNDED = 'bounded',
  UNBOUNDED = 'unbounded',
}

export default defineComponent({
  name: 'GoogleAdsPage',
  components: {
    ProcessingDialog,
  },
  setup: () => {
    const store = useConfigurationStore();
    const storeAudiences = useAudiencesStore();
    const $q = useQuasar();
    const audiences = computed(
      (): AudienceWithLog[] => storeAudiences.audiences,
    );
    const data = ref({
      selectedAudience: [] as AudienceWithLog[],
      currentAdgroupIndex: 1,
      currentCampaignIndex: 1,
      adsTreeSelectedNode: null,
      audiencesColumns: [
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
        { name: 'ratio', label: 'Ratio', field: 'split_ratio' },
        { name: 'ttl', label: 'TTL', field: 'ttl' },
        {
          name: 'uploads',
          label: 'Uploads',
          field: (row: AudienceWithLog) => (row.log ? row.log.length : '?'),
        },
        {
          name: 'created',
          label: 'Created',
          field: 'created',
          format: formatDate,
        },
        { name: 'actions', label: 'Actions', field: '' },
      ],
      audiencesTableTextWrap: true,
      audienceNameFilter: '',
      countryFilter: [] as string[],
      includeLogDuplicates: false,
      skipLoadAds: false,
      currentAudienceLogColumns: [
        {
          name: 'date',
          label: 'Date',
          field: 'date',
          sortable: true,
          format: (v: unknown) => formatDate(v, true),
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
          field: 'uploaded_user_count',
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
      audienceAdsTreeSplitterWith: 70,
      isLogPanelExpanded: true,
      loadAdsGraph: true,
      chart: {
        options: <ApexOptions>{
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
        series: [] as ApexAxisChartSeries,
      },
      chartAds: {
        options: <ApexOptions>{
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
        series: [] as ApexAxisChartSeries,
      },
      resultDialog: {
        show: false,
        header: '',
        message: '',
      },
      conversionsFilterStartDate: <string | undefined>undefined,
      conversionsFilterEndDate: <string | undefined>undefined,
      conversionsFilterCountries: <string[]>[],
      conversionsFilterCountriesOptions: [] as string[],
      conversionsFilterEvents: '',
      conversionsType: ConversionType.USERS,
      conversionsMode: <
        ConversionUsersMode | ConversionEventsMode | ConversionValueMode
      >ConversionUsersMode.ABSOLUTE,
      conversionsCalcStrategy: ConversionCalcStrategy.BOUNDED,
      conversionsFilterConvWindow: <number | undefined>undefined,
      conversionsSummary: <ConversionsData | undefined>undefined,
      pval: <number | undefined>undefined,
      showProcessingDialog: false,
    });

    function showExecutionResultDialog(results: AudiencesProcessResult) {
      let html = '';
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
      data.value.showProcessingDialog = true;
    };

    const onProcessAudience = async (
      audience: AudienceWithLog,
      mode: string,
    ) => {
      const res = await postApiUi<AudiencesProcessResponse>(
        'process',
        { audience: audience.name, mode: mode },
        'Processing the audience...',
      );
      if (res?.data.result) {
        showExecutionResultDialog(res.data.result);
      }
    };

    // Add computed property for unique countries
    const uniqueCountries = computed(() => {
      const allCountries = new Set<string>();
      audiences.value.forEach((audience) => {
        audience.countries.forEach((country) => {
          allCountries.add(country);
        });
      });
      return Array.from(allCountries).sort();
    });

    // Add computed property for filtered audiences
    const filteredAudiences = computed(() => {
      return audiences.value.filter((audience) => {
        // Filter by name
        const nameMatch =
          data.value.audienceNameFilter === '' ||
          audience.name
            .toLowerCase()
            .includes(data.value.audienceNameFilter.toLowerCase());

        // Filter by country
        const countryMatch =
          data.value.countryFilter.length === 0 ||
          data.value.countryFilter.some((country) =>
            audience.countries.includes(country),
          );

        return nameMatch && countryMatch;
      });
    });
    // watchers for filters
    watch(
      () => data.value.audienceNameFilter,
      (newValue) => {
        if (newValue === null) {
          data.value.audienceNameFilter = '';
        }
      },
    );

    watch(
      () => data.value.countryFilter,
      (newValue) => {
        if (newValue === null) {
          data.value.countryFilter = [];
        }
      },
    );

    // on selected audience change we update conversions graph
    watch(
      () => data.value.selectedAudience,
      (newValue: AudienceWithLog[]) => {
        data.value.currentAdgroupIndex = 1;
        if (newValue && newValue.length) {
          const newActiveAudience = newValue[0];
          data.value.conversionsFilterCountries = [];
          data.value.conversionsFilterEvents = '';
          data.value.conversionsFilterCountriesOptions =
            newActiveAudience.countries;
          updateConversionsChart(newActiveAudience.conversions);
          data.value.loadAdsGraph = !!(
            newActiveAudience.ads && newActiveAudience.ads.campaigns.length > 0
          );
        }
      },
    );

    watch(
      () => data.value.currentCampaignIndex,
      () => {
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
      () => data.value.conversionsType,
      (convType: ConversionType) => {
        switch (convType) {
          case ConversionType.USERS:
            data.value.conversionsMode = ConversionUsersMode.ABSOLUTE;
            break;
          case ConversionType.EVENTS:
            data.value.conversionsMode = ConversionEventsMode.ABSOLUTE;
            break;
          case ConversionType.VALUE:
            data.value.conversionsMode = ConversionValueMode.ABSOLUTE;
            break;
          default:
        }
      },
    );

    watch(
      [() => data.value.conversionsMode, () => data.value.conversionsType],
      () => {
        if (data.value.selectedAudience && data.value.selectedAudience.length) {
          const audience = data.value.selectedAudience[0];
          nextTick(() => {
            updateConversionsChart(audience.conversions);
          });
        }
      },
    );

    function getNodeInfo(
      obj: UserlistAssignmentData,
      prefix: string,
    ): Record<string, string | number> | undefined {
      const keys = Object.keys(obj).filter(
        (n) =>
          n.startsWith(prefix + '_') &&
          ['id', 'name', 'status'].indexOf(n.substring(prefix.length + 1)) ===
            -1,
      );
      const info: Record<string, string | number> = {};
      for (const key of keys) {
        info[key] = obj[key as keyof UserlistAssignmentData];
      }
      return keys.length ? info : undefined;
    }

    const onFetchAudiencesStatus = async () => {
      const res = await getApiUi<AudiencesStatusResponse>(
        'audiences/status',
        {
          include_log_duplicates: data.value.includeLogDuplicates,
          skip_ads: data.value.skipLoadAds,
        },
        'Fetching audiences status...',
      );
      if (!res?.data.result) return;
      const result = res.data.result;
      const audiences: AudienceWithLog[] = [];
      Object.keys(result).map((name) => {
        const audience = result[name];
        let logs = audience.log;
        // convert dates from strings to Date objects
        if (logs) {
          logs = logs.map((i: AudienceLog) => {
            i.date = new Date(i.date);
            return i;
          });
        }
        let ads = {
          campaigns: <CampaignInfo[]>[],
          adgroups: <UserlistAssignmentData[]>[],
          tree: <AdsTreeData[]>[],
        };
        if (audience.campaigns) {
          const campaigns = audience.campaigns.reduce(
            (r: Record<string, CampaignInfo>, a: CampaignInfo) => {
              r[a.campaign_id] = r[a.campaign_id] || {};
              r[a.campaign_id].campaign_id = a.campaign_id;
              r[a.campaign_id].customer_id = a.customer_id;
              r[a.campaign_id].campaign_name = a.campaign_name;
              return r;
            },
            Object.create(null),
          );
          ads = {
            campaigns: Object.values(campaigns),
            adgroups: audience.campaigns,
            tree: audience.campaigns.map((i: UserlistAssignmentData) => {
              return {
                label: `CID ${i.customer_id} - ${i.customer_name}`,
                type: <AdsTreeNodeType>'customer',
                id: i.customer_id,
                selected: true,
                info: getNodeInfo(i, 'customer'),
                children: [
                  <AdsTreeData>{
                    label: `Campaign ${i.campaign_id} - ${i.campaign_name} (${i.campaign_status})`,
                    status: i.campaign_status,
                    id: i.campaign_id,
                    type: <AdsTreeNodeType>'campaign',
                    info: getNodeInfo(i, 'campaign'),
                    children: [
                      <AdsTreeData>{
                        label: `AdGroup ${i.ad_group_id} - ${i.ad_group_name} (${i.ad_group_status})`,
                        status: i.ad_group_status,
                        id: i.ad_group_id,
                        type: <AdsTreeNodeType>'ad_group',
                        info: getNodeInfo(i, 'ad_group'),
                        children: [
                          <AdsTreeData>{
                            label: `UserList ${i.user_list_id} - ${i.user_list_name}`,
                            id: i.user_list_id,
                            type: <AdsTreeNodeType>'user_list',
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

        // the object in response is actually AudienceInfo,
        // now we just extend it with log and campaigns (ads)
        audiences.push({
          ...audience,
          log: logs,
          ads: ads,
        });
      });

      storeAudiences.audiences = audiences;
      if (audiences.length > 0) {
        if (data.value.selectedAudience.length) {
          // let's restore selection
          const previouslySelected = audiences.find(
            (val) => val.name === data.value.selectedAudience[0].name,
          );
          if (previouslySelected) {
            data.value.selectedAudience = [previouslySelected];
          } else {
            data.value.selectedAudience = [audiences[0]];
          }
        } else {
          data.value.selectedAudience = [audiences[0]];
        }
      }
    };

    const onRecalculateAudiencesLog = async () => {
      const audienceName = data.value.selectedAudience?.length
        ? data.value.selectedAudience[0].name
        : undefined;

      $q.dialog({
        title: 'Confirm',
        message: `Are you sure you want to rebuild ${audienceName ? 'log for audience ' + audienceName : 'audiences log'}?`,
        cancel: true,
        persistent: true,
      }).onOk(async () => {
        const res = await postApiUi<AudiencesLogRebuildResponse>(
          'audiences/recalculate_log',
          { audience: audienceName },
          'Recalculating...',
        );
        if (res?.data?.result) {
          for (const name of Object.keys(res.data.result)) {
            (storeAudiences.getAudience(name) as AudienceWithLog).log =
              res.data.result[name];
          }
          if (audienceName && !res.data.result[audienceName]) {
            // if we was asked to rebuild only one audience
            // then report if its new upload history is empty
            $q.dialog({
              title: 'Upload history recalculation',
              message: 'The audience has no upload history',
            });
          }
        }
      });
    };

    const onLoadConversions = async () => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        const date_start = <string | undefined>(
          data.value.conversionsFilterStartDate
        );
        const date_end = <string | undefined>(
          data.value.conversionsFilterEndDate
        );
        const country = data.value.conversionsFilterCountries;
        let country_str;
        if (country && country.length) {
          country_str = country.join(',');
        }
        const events = data.value.conversionsFilterEvents;
        audience.conversions = await loadConversions(
          audience.name,
          date_start,
          date_end,
          country_str,
          events,
          data.value.conversionsFilterConvWindow,
          data.value.loadAdsGraph && audience.ads
            ? audience.ads.campaigns
            : undefined,
        );
        updateConversionsChart(audience.conversions);
      }
    };

    const onGetConversionsQuery = async () => {
      if (data.value.selectedAudience && data.value.selectedAudience.length) {
        const audience = data.value.selectedAudience[0];
        const date_start = <string | undefined>(
          data.value.conversionsFilterStartDate
        );
        const date_end = <string | undefined>(
          data.value.conversionsFilterEndDate
        );
        const country = data.value.conversionsFilterCountries;
        let country_str;
        if (country && country.length) {
          country_str = country.join(',');
        }
        const res = await getApiUi<AudienceConversionsQueryResponse>(
          'conversions/query',
          {
            audience: audience.name,
            strategy: data.value.conversionsCalcStrategy,
            date_start,
            date_end,
            country: country_str,
            conv_window: data.value.conversionsFilterConvWindow,
          },
          'Fetching the audience conversion query...',
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
      dateStart: string | undefined,
      dateEnd: string | undefined,
      country: string | undefined,
      events: string | undefined,
      convWindow: number | undefined,
      campaigns?: CampaignInfo[],
    ): Promise<Conversions | undefined> => {
      data.value.chart.series = [];
      data.value.chartAds.series = [];
      data.value.conversionsSummary = undefined;
      // NOTE: if 'campaigns' is specified it says that we want to fetch campaign's metrics
      const res = await postApiUi<AudienceConversionsResponse>(
        'conversions',
        {
          audience: audienceName,
          strategy: data.value.conversionsCalcStrategy,
          date_start: dateStart,
          date_end: dateEnd,
          country: country,
          events: events,
          conv_window: convWindow,
          campaigns: campaigns,
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

      // calculate more metrics (rations based on returned ones)
      if (result.conversions) {
        for (const i of result.conversions) {
          if (i.total_test_user_count > 0) {
            // conversion rate (cr) - avg converted user counts:
            i.cr_test = i.cum_test_users / i.total_test_user_count;
            // avg conv events per user
            i.events_per_user_test =
              i.cum_test_events / i.total_test_user_count;
            // avg value per user
            i.value_per_user_test =
              i.cum_test_conv_value / i.total_test_user_count;
          }
          if (i.total_control_user_count > 0) {
            // conversion rate (cr) - avg converted user counts:
            i.cr_control = i.cum_control_users / i.total_control_user_count;
            // avg conv events per user
            i.events_per_user_control =
              i.cum_control_events / i.total_control_user_count;
            // avg value per user
            i.value_per_user_control =
              i.cum_control_conv_value / i.total_control_user_count;
          }
          // avg conv events per session
          if (i.test_session_count) {
            i.events_per_session_test =
              i.cum_test_events / i.test_session_count;
          }
          if (i.control_session_count) {
            i.events_per_session_control =
              i.cum_control_events / i.control_session_count;
          }
          // avg conversion value:
          if (i.cum_test_events > 0) {
            i.value_per_event_test = i.cum_test_conv_value / i.cum_test_events;
          }
          if (i.cum_control_events > 0) {
            i.value_per_event_control =
              i.cum_control_conv_value / i.cum_control_events;
          }
        }
      }
      const ads_metrics_grouped = result.ads_metrics
        ? result.ads_metrics.reduce(
            (r: Record<string, AdsMetric[]>, a: AdsMetric) => {
              r[a.campaign] = r[a.campaign] || [];
              r[a.campaign].push(a);
              return r;
            },
            Object.create(null),
          )
        : {};

      return {
        data: result.conversions,
        start_date: result.date_start,
        end_date: result.date_end,
        pval: result.pval,
        pval_events: result.pval_events,
        ads_metrics: ads_metrics_grouped,
      };
    };

    function selectConversionsGraphItem(
      item: ConversionsData,
      groupName: 'test' | 'control',
    ) {
      const convType = data.value.conversionsType;
      const convMode = data.value.conversionsMode;
      if (convType === ConversionType.USERS) {
        if (groupName === 'test') {
          return convMode === ConversionUsersMode.CONV_RATE
            ? item.cr_test
            : item.cum_test_users;
        } else {
          return convMode === ConversionUsersMode.CONV_RATE
            ? item.cr_control
            : item.cum_control_users;
        }
      } else if (convType === ConversionType.EVENTS) {
        if (groupName === 'test') {
          if (convMode === ConversionEventsMode.ABSOLUTE) {
            return item.cum_test_events;
          } else if (convMode === ConversionEventsMode.AVG_BY_SESSION) {
            return item.events_per_session_test;
          } else if (convMode === ConversionEventsMode.AVG_BY_USER) {
            return item.events_per_user_test;
          }
        } else {
          if (convMode === ConversionEventsMode.ABSOLUTE) {
            return item.cum_control_events;
          } else if (convMode === ConversionEventsMode.AVG_BY_SESSION) {
            return item.events_per_session_control;
          } else if (convMode === ConversionEventsMode.AVG_BY_USER) {
            return item.events_per_user_control;
          }
        }
      } else if (convType === ConversionType.VALUE) {
        if (groupName === 'test') {
          if (convMode === ConversionValueMode.ABSOLUTE) {
            return item.cum_test_conv_value;
          } else if (convMode === ConversionValueMode.AVG_BY_EVENT) {
            return item.value_per_event_test;
          } else if (convMode === ConversionValueMode.AVG_BY_USER) {
            return item.value_per_user_test;
          }
        } else {
          if (convMode === ConversionValueMode.ABSOLUTE) {
            return item.cum_control_conv_value;
          } else if (convMode === ConversionValueMode.AVG_BY_EVENT) {
            return item.value_per_event_control;
          } else if (convMode === ConversionValueMode.AVG_BY_USER) {
            return item.value_per_user_control;
          }
        }
      }
      return 0; // it should never happen
    }

    const updateConversionsChart = (conversions?: Conversions) => {
      if (!conversions || !conversions.data || !conversions.data.length) {
        data.value.chart.series = [];
        data.value.chartAds.series = [];
        data.value.conversionsFilterStartDate = undefined;
        data.value.conversionsFilterEndDate = undefined;
        data.value.conversionsSummary = undefined;
        data.value.pval = undefined;
        return;
      }
      data.value.conversionsFilterStartDate = conversions.start_date;
      data.value.conversionsFilterEndDate = conversions.end_date;
      data.value.conversionsSummary =
        conversions.data[conversions.data.length - 1];
      switch (data.value.conversionsType) {
        case ConversionType.USERS:
          data.value.pval = conversions.pval;
          break;
        case ConversionType.EVENTS:
          data.value.pval = conversions.pval_events;
          break;
        default:
          data.value.pval = undefined;
          break;
      }
      let title = '';
      switch (data.value.conversionsMode) {
        case ConversionUsersMode.ABSOLUTE:
          title = 'Absolute number of converted users';
          break;
        case ConversionUsersMode.CONV_RATE:
          title =
            'Conversion rate: number of converted users / number of users in a group (treatment/control)';
          break;
        case ConversionEventsMode.ABSOLUTE:
          title = 'Absolute number of conversion events';
          break;
        case ConversionEventsMode.AVG_BY_SESSION:
          title =
            'Average number of conversion events per session (by users in a group)';
          break;
        case ConversionEventsMode.AVG_BY_USER:
          title = 'Average number of conversion events per user (in a group)';
          break;
        case ConversionValueMode.ABSOLUTE:
          title = 'Total conversion value (by users in a group)';
          break;
        case ConversionValueMode.AVG_BY_EVENT:
          title = 'Average conversion value per conversion event';
          break;
        case ConversionValueMode.AVG_BY_USER:
          title = 'Average conversion value per converted user';
          break;
        default:
      }
      const graph_data = {} as Record<
        string,
        {
          date: string;
          test: number;
          control: number;
        }
      >;
      // TODO: should we limit graph with on X axis back only N days from the end date?
      for (const item of conversions.data) {
        const label = <string>formatDate(new Date(item.date));
        // NOTE: dates should not repeat otherwise there will be no graph
        graph_data[label] = {
          date: item.date,
          test: selectConversionsGraphItem(item, 'test'),
          control: selectConversionsGraphItem(item, 'control'),
        };
      }
      const entries = Object.entries(graph_data);
      const test_data = entries.map((item) => {
        return { x: item[1].date, y: item[1].test };
      });
      const control_data = entries.map((item) => {
        return { x: item[1].date, y: item[1].control };
      });
      data.value.chart.series = [
        { name: 'treatment', data: test_data },
        { name: 'control', data: control_data },
      ];
      const getYAxisFormatter = (val: number) => {
        if (data.value.conversionsType === ConversionType.VALUE) {
          return `$${val.toFixed(2)}`;
        }
        if (data.value.conversionsMode === ConversionUsersMode.CONV_RATE) {
          return `${(val * 100).toFixed(2)}%`;
        }
        if (
          data.value.conversionsMode === ConversionUsersMode.ABSOLUTE ||
          data.value.conversionsMode === ConversionEventsMode.ABSOLUTE
        ) {
          return `${val.toFixed(0)}`;
        }
        return `${val.toFixed(2)}`;
      };
      data.value.chart.options = {
        ...data.value.chart.options,
        title: {
          text: title,
        },
        yaxis: {
          labels: {
            formatter: getYAxisFormatter,
          },
        },
      };
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
        if (!campaign) {
          data.value.chartAds.series = [];
        } else {
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

    const getAdsTreeNode = (
      node: AdsTreeData,
      nodeKey: AdsTreeNodeType,
    ): AdsTreeData | undefined => {
      if ((node.type = nodeKey)) {
        return node;
      }
      if (node.children) {
        for (const child of node.children) {
          const res = getAdsTreeNode(child, nodeKey);
          if (res) return res;
        }
      }
    };

    const renderNodeInfo = (
      nodeRoot: AdsTreeData,
      nodeKey: AdsTreeNodeType,
    ): string | undefined => {
      const node = getAdsTreeNode(nodeRoot, nodeKey);
      if (node && node.info) {
        let html = '';
        for (const key of Object.keys(node.info)) {
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

    const getConversionsModeOptions = () => {
      switch (data.value.conversionsType) {
        case ConversionType.USERS:
          return [
            { label: 'Absolute', value: ConversionUsersMode.ABSOLUTE },
            { label: 'Conv Rate', value: ConversionUsersMode.CONV_RATE },
          ];
        case ConversionType.EVENTS:
          return [
            { label: 'Absolute', value: ConversionEventsMode.ABSOLUTE },
            { label: 'Avg per user', value: ConversionEventsMode.AVG_BY_USER },
            {
              label: 'Avg per session',
              value: ConversionEventsMode.AVG_BY_SESSION,
            },
          ];
        case ConversionType.VALUE:
          return [
            { label: 'Absolute', value: ConversionValueMode.ABSOLUTE },
            { label: 'Avg per user', value: ConversionValueMode.AVG_BY_USER },
            { label: 'Avg per conv', value: ConversionValueMode.AVG_BY_EVENT },
          ];
        default:
          throw new Error('Unsupported value: ' + data.value.conversionsType);
      }
    };

    return {
      store,
      data,
      audiences,
      uniqueCountries,
      filteredAudiences,
      onExecute,
      onProcessAudience,
      onFetchAudiencesStatus,
      onRecalculateAudiencesLog,
      onLoadConversions,
      onGetConversionsQuery,
      onOpenChart,
      formatArray,
      formatFloat,
      renderNodeInfo,
      getConversionsModeOptions,
    };
  },
});
</script>
