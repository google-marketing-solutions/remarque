<template>
  <q-page padding class="items-center justify-evenly">
    <div class="row1">
      <div class="text-h2">Audiences</div>
    </div>

    <div class="q-mt-md">
      <q-card class="stat-card" flat bordered>
        <q-card-section>
          <div class="text-h6">GA4 statistics</div>
          <div>
            <q-banner class="bg-grey-3">
              <template v-slot:avatar>
                <q-icon name="info" color="primary" />
              </template>
              Define time window and load events from Google Analytics. Then choose an app id, you'll see available events
              and countries,
              which you can use for defining audiences below.
            </q-banner>
          </div>
        </q-card-section>
        <q-card-section class="row q-col-gutter-md" style="padding-top: 0; padding-bottom: 0;">
          <div class="col-2 ">
            <q-input outlined v-model="store.days_ago_start" label="Period start (days ago)" placeholder="" hint="" />
          </div>
          <div class="col-2 ">
            <q-input outlined v-model="store.days_ago_end" label="Period end (days ago)" placeholder="" hint="" />
          </div>
        </q-card-section>

        <q-card-section class="row q-col-gutter-md" style="padding-top: 0;">
          <div class="col-md-auto ">
            <q-table title="GA4 Apps" style="height: 400px" flat bordered :rows="data.app_ids" :row-key="r => r"
              :columns="data.appid_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]"
              @row-click="onAppIdSelected" selection="single" v-model:selected="data.selectedAppId"
              :loading="data.ga_stat_loading" />
          </div>

          <div class="col">
            <q-table title="GA4 Events" style="height: 400px" flat bordered :rows="data.events" row-key="event"
              :columns="data.events_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]"
              :no-data-label="data.app_ids.length ? 'Choose an app id' : 'Load all events'"
              :loading="data.ga_stat_loading" :filter-method="filterEvents" :filter="data.eventsSearch">
              <template v-slot:top>
                <div style="width: 100%" class="row">
                  <div class="col-7">
                    <div class="q-table__title">GA4 Events</div>
                  </div>
                  <div class="col-5">
                    <q-input dense debounce="400" color="primary" v-model="data.eventsSearch">
                      <template v-slot:append>
                        <q-icon name="search" />
                      </template>
                    </q-input>
                  </div>
                </div>
              </template>
            </q-table>
          </div>

          <div class="col">
            <q-table title="GA4 Countries" style="height: 400px" flat bordered :rows="data.countries" row-key="country"
              :columns="data.countries_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]" selection="multiple"
              :no-data-label="data.app_ids.length ? 'Choose an app id' : 'Load all events'"
              :loading="data.ga_stat_loading" v-model:selected="data.selectedCountries" :filter-method="filterCountries"
              :filter="data.countriesSearch">
              <template v-slot:top>
                <div style="width: 100%" class="row">
                  <div class="col-7">
                    <div class="q-table__title">GA4 Countries</div>
                  </div>
                  <div class="col-5">
                    <q-input dense debounce="400" color="primary" v-model="data.countriesSearch">
                      <template v-slot:append>
                        <q-icon name="search" />
                      </template>
                    </q-input>
                  </div>
                </div>
              </template>

            </q-table>
          </div>
        </q-card-section>

        <q-card-actions class="q-pa-md">
          <q-btn color="primary" label="Load" icon="sync" @click="onLoad"></q-btn>
        </q-card-actions>
      </q-card>
    </div>

    <q-card class="q-mt-md audiences-card" flat bordered>
      <q-card-section>
        <div class="text-h6">Audiences</div>
        <div class="row" style="width: 100%">
          <q-banner class="col-12 bg-grey-3">
            <template v-slot:avatar>
              <q-icon name="warning" color="warning" />
            </template>
            Audience name is crucial, it's used to uniquely identify the audience.
            It will be used as a name for customer match user list and as a prefix for all tables in BigQuery with user
            ids.<br>
            As soon as you change the name an audience will be recreated, effectively loosing all accumulated data.
            If you have a lot of audiences you will probably want to use country names/codes in their names.
          </q-banner>
        </div>
      </q-card-section>
      <q-card-section>
        <q-form @reset="onAudienceFormReset" ref="audienceForm">
          <div class="row q-col-gutter-md" style="width: 100%">
            <div class="col q-gutter-md">
              <q-input filled v-model="audience.name" label="Name *" hint="Audience (a.k.a. user list) name in Google Ads"
                lazy-rules :rules="[val => val && val.length > 0 || 'Please enter a name']" />

              <q-input filled v-model="audience.app_id" label="App id *" lazy-rules :rules="[
                val => val !== null && val !== '' || 'Please enter app_id'
              ]" />

              <q-select filled v-model="audience.countries" :options="audience.allCountriesSelect" use-input use-chips
                multiple @filter="onAudienceFilterCountries" input-debounce="0" label="Countries"
                hint="Countries to include in the audience. To see available countries load your GA4 statistics"
                new-value-mode="add-unique" />

              <div>
                <label class="q-mt-md" style="margin-left: 12px; margin-right: 12px;">Mode</label>
                <q-btn-toggle class="" v-model="audience.mode" no-wrap outline
                  :toggle-color="audience.mode == 'off' ? 'red' : audience.mode == 'test' ? 'blue' : 'green'" :options="[
                    { label: 'Off', value: 'off' },
                    { label: 'Testing', value: 'test' },
                    { label: 'Always-on', value: 'prod' }
                  ]" />
              </div>
            </div>
            <div class="col q-gutter-md">
              <div>
                <q-select filled v-model="audience.events_include" :options="audience.allEventsSelect"
                  @filter="onAudienceFilterEvents" use-input use-chips multiple input-debounce="0"
                  label="Events to include" hint="GA4 events that happened for users" new-value-mode="add-unique" />
              </div>
              <div>
                <q-select filled v-model="audience.events_exclude" :options="audience.allEventsSelect"
                  @filter="onAudienceFilterEvents" use-input use-chips multiple input-debounce="0"
                  label="Events to exclude" hint="GA4 events that did NOT happen for users (app_remove always included)"
                  new-value-mode="add-unique" />
              </div>
              <div class="">
                <div class="row q-col-gutter-md">
                  <q-input class="col-3" outlined v-model="audience.days_ago_start" label="Period start" placeholder=""
                    hint="days ago" />
                  <q-input class="col-3" outlined v-model="audience.days_ago_end" label="Period end" placeholder=""
                    hint="days ago" />
                  <q-input class="col-3" outlined v-model="audience.ttl" type="number" min="1"
                    @blur="() => audience.ttl = audience.ttl < 1 ? 1 : audience.ttl" label="Time to live" placeholder=""
                    hint="Days to stay in treatment group" />
                </div>
              </div>
              <div class="row">
                <div class="col" style="display: inline; margin-left: 50px; text-align: right;" alaign="right">
                  <q-btn @click="data.showEditQueryDialog = true">Customize Query</q-btn>
                </div>
              </div>
            </div>
          </div>
          <div class="q-pa-md">
            <q-btn label="Update" @click="onAudienceFormSave" color="primary" />
            <q-btn label="Reset" type="reset" color="primary" flat class="q-ml-sm" />
            <q-btn label="Preview" @click="onAudiencePreview" flat class="q-ml-sm" />
            <q-btn label="Get Query" @click="onAudienceGetQuery" flat class="q-ml-sm" />
            <q-btn label="Power Analysis" @click="data.powerAnalysis.showDialog = true" flat class="q-ml-sm" />
          </div>
        </q-form>
      </q-card-section>
      <q-card-section>
        <div class="">
          <q-table title="Audiences" class="qtable-sticky-header" style="height: 400px" flat bordered
            :rows="data.audiences" row-key="name" :columns="data.audiences_columns" virtual-scroll
            :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]" :wrap-cells="data.audiences_wrap">
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
                <q-btn dense round flat color="grey" @click="onAudienceListEdit(props)" icon="edit"></q-btn>
                <q-btn dense round flat color="grey" @click="onAudienceListDelete(props)" icon="delete"></q-btn>
              </q-td>
            </template>
            <template v-slot:body-cell-mode="props">
              <q-td :props="props">
                <q-chip :color="props.row.mode === 'off' ? 'red' : 'green'" text-color="white" dense
                  class="text-weight-bolder" square>{{ props.row.mode === 'off' ? 'Off' : props.row.mode === 'test' ?
                    'Test ' : 'Prod' }}</q-chip>
              </q-td>
            </template>
            <template v-slot:body-cell-query="props">
              <q-td :props="props">
                <q-icon name="check" color="red" size="sm" v-if="props.row.query" />
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
          </q-table>
        </div>
      </q-card-section>
      <q-card-actions class="q-pa-md">
        <q-btn label="Save" icon="upload" size="md" @click="onAudiencesUpload" color="primary" style="width:130px" />
        <q-btn label="Reload" icon="download" size="md" @click="onAudiencesDownload" color="primary"
          style="width:130px" />
      </q-card-actions>
    </q-card>
  </q-page>

  <q-dialog v-model="data.showEditQueryDialog">
    <q-card style="width: 1200px; max-width: 80%" class="q-px-sm q-pb-md">
      <q-card-section>
        <div class="text-h6">Customize audience query</div>
      </q-card-section>
      <q-card-section>
        <div class="text-body1">A query to customize user audience. It will be executed to fetch user ids and
          attributes. It will be used as a subquery for <code>CREATE OR REPLACE TABLE `destination_table` AS</code>,
          where destination_table is the name of audience table (audience_{name})<br>
          <q-expansion-item label="Details">
            Query must return the following columns:<br>
            <ul>
              <li><code>user</code> - user id, i.e. device.advertising_id</li>
              <li><code>brand</code> - device.mobile_brand_name</li>
              <li><code>osv</code> - device.operating_system_version</li>
              <li><code>days_since_install</code> - number of days between today and last first_open event</li>
              <li><code>src</code> - traffic_source.source + "_" + traffic_source.medium</li>
              <li><code>n_sessions</code> - number of session_start events for the period</li>
            </ul>

            Inside your query your can use macros in this format:
            <code>{macro}</code>.<br>
            The following macros are available:
            <ul>
              <li><code>source_table</code> - full name of a GA4 table</li>
              <li><code>day_start</code> - start date of time window as yyyymmdd</li>
              <li><code>day_end</code> - end date of time windiw as yyyymmdd</li>
              <li><code>app_id</code> - app id from the audience definition</li>
              <li><code>countries</code> - a list of countries from the audience definition</li>
              <li><code>all_users_table</code> - fullyqualified name of the users_normalized table</li>
              <li><code>all_events_list</code> - list of all events names from the audience definition</li>
            </ul>
          </q-expansion-item>
        </div>
      </q-card-section>
      <q-card-section>
        <q-input filled type="textarea" v-model="audience.query" autogrow />
      </q-card-section>
      <q-separator />
      <q-card-actions align="right">
        <q-btn label="Close" color="primary" @click="data.showEditQueryDialog = false" />
      </q-card-actions>
    </q-card>
  </q-dialog>

  <q-dialog v-model="data.powerAnalysis.showDialog">
    <q-card style="max-width: 80%; width: 800px;" class="q-px-sm q-pb-md">
      <q-card-section>
        <div class="text-h6">Power Analysis</div>
      </q-card-section>
      <q-card-section>
        <!-- q-pa-xs q-gutter-md -->
        <div class="row q-col-gutter-md full-width">
          <q-banner class="col bg-grey-3">
            <template v-slot:avatar>
              <q-icon name="info" color="primary" />
            </template>
            Power analysis tells us the minimum amount of users in a user list for an experiment to make sense.
            To calculate power we need to calculate baseline conversion first (conversion that usually happens without any
            tests).
            To do this we'll take users (device_id's) from your GA4 data by criteria from the current audience and find
            how many of them were converted to the next "conversion window" days. Alternately you can enter the baseline
            conversion manually.
          </q-banner>
        </div>
        <div class="row q-col-gutter-md q-my-xs">
          <div class="col " style="max-width:250px">
            <q-input filled v-model="data.powerAnalysis.from" mask="####-##-##" label="Start date" clearable>
              <template v-slot:append>
                <q-icon name="event" class="cursor-pointer">
                  <q-popup-proxy ref="qStartProxy" cover transition-show="scale" transition-hide="scale">
                    <q-date v-model="data.powerAnalysis.from" mask="YYYY-MM-DD" :no-unset="true"
                      @update:model-value="$refs.qStartProxy.hide()">
                    </q-date>
                  </q-popup-proxy>
                </q-icon>
              </template>
            </q-input>
          </div>
          <div class="col " style="max-width:250px">
            <q-input filled v-model="data.powerAnalysis.to" mask="####-##-##" label="End date" clearable>
              <template v-slot:append>
                <q-icon name="event" class="cursor-pointer">
                  <q-popup-proxy ref="qEndProxy" cover transition-show="scale" transition-hide="scale">
                    <q-date v-model="data.powerAnalysis.to" mask="YYYY-MM-DD" today-btn :no-unset="true"
                      @update:model-value="$refs.qEndProxy.hide()">
                    </q-date>
                  </q-popup-proxy>
                </q-icon>
              </template>
            </q-input>
          </div>
          <q-input class="col " outlined v-model="data.powerAnalysis.conversion_window" type="number" min="1"
            label="Conversion window (days)" placeholder="" hint="Days for users to convert" />
        </div>
        <div class="row q-col-gutter-md q-my-xs">
          <q-input class="col-3" outlined v-model="data.powerAnalysis.conversion_rate" type="number" min="0"
            label="Conversion rate" placeholder="" hint="Baseline conversion"
            :readonly="!data.powerAnalysis.conversion_manual" />
          <q-toggle v-model="data.powerAnalysis.conversion_manual" label="Enter manually" />
        </div>
      </q-card-section>
      <q-card-section>
        <div class="row q-col-gutter-md">
          <div class="col">
            <div>Users in the audience: {{ data.powerAnalysis.users_audience }}</div>
            <div>Baseline conversion: {{ data.powerAnalysis.users_converted }}</div>
          </div>
          <div class="col">
            <q-btn label="Show query" color="primary" @click="onGetPowerQuery" :disable="!data.powerAnalysis.query" />
          </div>
        </div>
        <!-- <br> -->
        <!-- <q-btn label="Calculate" color="primary" @click="onGetPower" /> -->
      </q-card-section>
      <q-card-section>
        <div class="row q-col-gutter-md q-my-xs">
          <q-input class="col " outlined v-model="data.powerAnalysis.power" type="number" min="0" label="Power"
            placeholder="" hint="" />
          <q-input class="col " outlined v-model="data.powerAnalysis.uplift" type="number" min="0" max="100"
            label="Uplift" placeholder="" hint="Uplift rate (between 0-1) of conversion (desired/expected)" />
          <q-input class="col " outlined v-model="data.powerAnalysis.alpha" type="number" min="0" max="100" label="Alpha"
            placeholder="" hint="" />
          <q-input class="col " outlined v-model="data.powerAnalysis.ratio" type="number" min="0" label="ratio"
            placeholder="" hint="" />
        </div>
      </q-card-section>
      <q-card-section>
        <div>Sample size: <b>{{ data.powerAnalysis.sample_size }}</b> (calculated for t-test)</div>
        <div>Power: {{ data.powerAnalysis.new_power }} (recalculated for sample size as z-test)</div>
        <br>
        <q-btn label="Calculate" color="primary" @click="onGetPower" />
      </q-card-section>
      <q-separator />
      <q-card-actions align="right">
        <q-btn label="Close" color="primary" @click="data.powerAnalysis.showDialog = false" />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<style>
.period-card {
  padding: 0px 10px;
}

.text-pre {
  white-space: pre-wrap;
}
</style>

<script lang="ts">
import { defineComponent, ref, watch, computed } from 'vue';
import { AudienceInfo, AudienceMode, configurationStore } from 'stores/configuration';
import { getApi, postApi, postApiUi, getApiUi } from 'boot/axios';
import { useQuasar, QForm } from 'quasar';
import { formatArray } from '../helpers/utils';

export default defineComponent({
  name: 'AudiencesPage',
  components: {},
  setup: () => {
    const store = configurationStore();
    const $q = useQuasar();

    const data = ref({
      showEditQueryDialog: false,
      showPowerAnalysisDialog: false,
      powerAnalysis: {
        showDialog: false,
        // parameters:
        from: '',
        to: '',
        conversion_window: 7,
        conversion_manual: false,
        power: 0.8,
        alpha: 0.05,
        ratio: 1,
        uplift: 0.25,
        // response:
        query: '',
        users_audience: 0,
        conversion_rate: undefined,
        users_converted: 0,
        sample_size: 0,
        new_power: undefined
      },
      selectedAppId: [] as string[],
      selectedCountries: [] as any[],
      app_ids: [] as any[],
      events: [] as any[],
      countries: [] as any[],
      appid_columns: [
        { name: 'id', label: 'app_id', field: (row: any) => row }
      ],
      events_columns: [
        { name: 'event', label: 'Event name', field: 'event', sortable: true },
        { name: 'count', label: 'Event count', field: 'count', sortable: true },
      ],
      countries_columns: [
        { name: 'country', label: 'Country', field: 'country', sortable: true },
        { name: 'country_code', label: 'Code', field: 'country_code', sortable: true },
        { name: 'count', label: 'User count', field: 'count', sortable: true },
      ],
      audiences_columns: [
        { name: 'name', label: 'Name', field: 'name', sortable: true },
        { name: 'app_id', label: 'App id', field: 'app_id', sortable: true },
        { name: 'query', label: 'Custom query', field: 'query' },
        { name: 'countries', label: 'Countries', field: 'countries', sortable: true, format: formatArray },
        { name: 'events_include', label: 'Include events', field: 'events_include', sortable: true, format: formatArray },
        { name: 'events_exclude', label: 'Exclude events', field: 'events_exclude', sortable: true, format: formatArray },
        { name: 'days_ago_start', label: 'Start', field: 'days_ago_start' },
        { name: 'days_ago_end', label: 'End', field: 'days_ago_end' },
        { name: 'ttl', label: 'TTL', field: 'ttl' },
        { name: 'mode', label: 'Mode', field: 'mode' },
        { name: 'actions', label: 'Actions', field: '', align: 'center' },
      ],
      audiences_wrap: true,
      ga_stat_loading: false,
      eventsSearch: '',
      countriesSearch: '',
      audiences: computed(() => store.audiences)
    });
    let audience = ref({
      name: '',
      id: '',
      mode: 'off',
      app_id: '',
      countries: [] as string[],
      allCountries: [] as string[],
      allCountriesSelect: [] as string[],
      allEvents: [] as string[],
      allEventsSelect: [] as string[],
      events_include: [] as string[],
      events_exclude: [] as string[],
      days_ago_start: undefined,
      days_ago_end: undefined,
      user_list: '',
      query: '',
      ttl: 1
    });
    const audienceForm = ref(null as unknown as QForm);

    // Stats
    // On app_id change we update the lists of events and countries to show data for that app_id
    watch(() => data.value.selectedAppId, (newValue) => {
      if (newValue && newValue.length) {
        const app_id = newValue[0];
        data.value.events = store.stat.events[app_id];
        data.value.countries = store.stat.countries[app_id];
        if (data.value.countries && data.value.countries.length) {
          audience.value.allCountries = data.value.countries.map(r => r.country).sort();
        } else {
          audience.value.allCountries = [];
        }
        audience.value.allCountriesSelect = audience.value.allCountries;
        if (data.value.events && data.value.events.length) {
          audience.value.allEvents = data.value.events.map(r => r.event).sort();
        } else {
          audience.value.allEvents = [];
        }
        audience.value.allEventsSelect = audience.value.allEvents;
        audience.value.app_id = app_id;
        audience.value.countries = [];
      } else {
        data.value.events = [];
        data.value.countries = [];
        audience.value.allCountries = [];
        audience.value.allCountriesSelect = audience.value.allCountries;
        audience.value.allEvents = [];
        audience.value.allEventsSelect = audience.value.allEvents;
      }
    }, { deep: true });
    watch(() => data.value.selectedCountries, (newValue) => {
      console.log(newValue);
      audience.value.countries = newValue.map(r => r.country);
    });
    const onStatLoad = () => {
      const days_ago_start = store.days_ago_start
      const days_ago_end = store.days_ago_end;
      if (!days_ago_start) {
        $q.notify({
          color: 'negative',
          //position: 'top',
          message: 'Please define the period first',
          icon: 'report_problem'
        });
        return;
      }
      const loading = $q.notify('Loading stat');
      data.value.ga_stat_loading = true;
      getApi('stat', { days_ago_start, days_ago_end }, loading)
        .then((response) => {
          // we expect an object with `results` field containing an array of objects
          const results = response.data.results;
          console.log(results)

          data.value.app_ids = results.app_ids;
          data.value.events = [];
          data.value.countries = [];
          store.stat.events = results.events;
          store.stat.countries = results.countries;
          data.value.ga_stat_loading = false;
        })
        .catch((e) => {
          $q.notify({
            color: 'negative',
            message: 'Loading failed: ' + e.message,
            icon: 'report_problem'
          });
          data.value.ga_stat_loading = false;
        })
    }
    const onAppIdSelected = (evt: any, row: any, index: any) => {
      data.value.selectedAppId = [row];
    }
    const filterEvents = (rows: readonly any[], term: string) => {
      console.log(term);
      return rows.filter((row) => {
        return row['event'].toLowerCase().includes(term.toLowerCase());
      });
    }
    const filterCountries = (rows: readonly any[], term: string) => {
      return rows.filter((row) => {
        return row['country'].toLowerCase().includes(term.toLowerCase());
      })
    }
    function getAudienceFromForm(): AudienceInfo {
      const obj = {
        name: audience.value.name?.trim(),
        app_id: audience.value.app_id?.trim(),
        countries: audience.value.countries,
        events_include: audience.value.events_include,
        events_exclude: audience.value.events_exclude,
        days_ago_start: audience.value.days_ago_start || store.days_ago_start || 0,
        days_ago_end: audience.value.days_ago_end || store.days_ago_end || 0,
        user_list: audience.value.user_list,
        mode: <AudienceMode>audience.value.mode,
        query: audience.value.query,
        ttl: audience.value.ttl,
      }
      return obj;
    }
    // Audiences
    const onAudienceFormSave = () => {
      audienceForm.value.validate().then(success => {
        audience.value.name = audience.value.name.trim().replaceAll(' ', '_');
        // TODO: expand macros
        if (success) {
          const days_ago_start = audience.value.days_ago_start || store.days_ago_start || audience.value.days_ago_start;
          const days_ago_end = audience.value.days_ago_end || store.days_ago_end || audience.value.days_ago_end;
          if (!days_ago_start || !days_ago_end && days_ago_end !== 0) {
            $q.dialog({ message: 'You need to set a time period for the audience' })
            return;
          }

          // check countries - w/o countries we'll create audience for ALL users of an app,
          // so such an audience should be the only one.
          if (audience.value.countries.length == 0) {
            $q.dialog({
              title: 'Prompt',
              message: 'Your audience does not contain any country. It means that it will include all users of the app. Are you sure to proceed?',
              cancel: true,
              persistent: true
            }).onOk(data => {
              saveAudience();
            });
          } else {
            saveAudience();
          }
          // TODO: check events
        }
      });
    }
    const saveAudience = () => {
      let idx = data.value.audiences.findIndex(val => val.name === audience.value.name);
      const obj = getAudienceFromForm();
      if (idx >= 0) {
        // updating
        Object.assign(store.audiences[idx], obj);
      } else {
        // creating new
        store.audiences.push(obj);
      }
      // clear the form
      onAudienceFormReset();
    }
    const onAudienceFormReset = () => {
      audience.value.name = '';
      audience.value.app_id = '';
      audience.value.countries = [];
      audience.value.events_include = [] as string[];
      audience.value.events_exclude = [] as string[];
      audience.value.days_ago_start = undefined;
      audience.value.days_ago_end = undefined;
      audience.value.mode = 'off';
      audience.value.query = '';
      audience.value.ttl = 1;
      audienceForm.value.resetValidation();
    }
    const onAudienceFilterCountries = (val: string, doneFn: (callbackFn: () => void) => void, abortFn: () => void) => {
      doneFn(() => {
        audience.value.allCountriesSelect = audience.value.allCountries.filter(r => r.toLowerCase().startsWith(val?.toLowerCase()));
      });
    }
    const onAudienceFilterEvents = (val: string, doneFn: (callbackFn: () => void) => void, abortFn: () => void) => {
      doneFn(() => {
        audience.value.allEventsSelect = audience.value.allEvents.filter(r => r.toLowerCase().includes(val?.toLowerCase()));
      });
    }
    const onAudiencePreview = async () => {
      const obj = getAudienceFromForm();
      if (!obj.app_id) {
        $q.dialog({ message: 'Please specify an app_id' });
        return;
      }
      $q.loading.show({ message: 'Getting a preview of the audience...' });
      const loading = () => $q.loading.hide();
      try {
        let res = await postApi('audiences/preview', { audience: obj }, loading);
        console.log(res.data);
        $q.dialog({
          title: 'Audience preview',
          message: `The audience with current conditions returned ${res.data.users_count} users.\nPlease it doens't take into account TTL (users readded from previous days because of ttl>1)`
        });
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    };
    const onAudienceGetQuery = async () => {
      const obj = getAudienceFromForm();
      let res = await postApiUi('audiences/get_query', { audience: obj }, $q, 'Getting query...');
      if (!res?.data) {
        return;
      }
      console.log(res.data.query);
      $q.dialog({
        title: 'SQL Query for the audience',
        message: res.data.query,
        ok: {
          push: true
        },
        class: 'text-pre',
        fullWidth: true
      });
    };
    const onGetPower = async () => {
      if (!data.value.powerAnalysis.conversion_manual) {
        // calculate baseline conversion rate first
        const obj = getAudienceFromForm();
        let res = await postApiUi('audiences/base_conversion', {
          audience: obj,
          date_start: data.value.powerAnalysis.from,
          date_end: data.value.powerAnalysis.to,
        }, $q, 'Calculating baseline conversion...');
        if (!res?.data?.result) {
          return;
        }
        console.log(res.data);
        let result = res.data.result;
        data.value.powerAnalysis.conversion_rate = result.cr;
        data.value.powerAnalysis.users_audience = result.audience;
        data.value.powerAnalysis.users_converted = result.converted;
        data.value.powerAnalysis.query = result.query;
      }
      // calculate power
      let res = await getApiUi('audiences/power', {
        cr: data.value.powerAnalysis.conversion_rate,
        power: data.value.powerAnalysis.power,
        alpha: data.value.powerAnalysis.alpha,
        ratio: data.value.powerAnalysis.ratio,
        uplift: data.value.powerAnalysis.uplift,
      }, $q, 'Calculating power...');
      if (!res?.data) {
        return;
      }
      data.value.powerAnalysis.sample_size = res.data.sample_size;
      data.value.powerAnalysis.new_power = res.data.new_power;
    };
    const onGetPowerQuery = async () => {
      $q.dialog({
        title: 'SQL Query for calculating the baseline conversion',
        message: data.value.powerAnalysis.query,
        ok: {
          push: true
        },
        class: 'text-pre',
        fullWidth: true
      });
    };

    const onAudienceListEdit = (props: any) => {
      Object.assign(audience.value, props.row);
    }
    const onAudienceListDelete = (props: any) => {
      console.log(props);
      store.removeAudience(props.key);
    }
    const onAudiencesUpload = async () => {
      $q.loading.show({ message: 'Uploading audiences...' });
      const loading = () => $q.loading.hide();

      const audiences = data.value.audiences.map((row) => {
        return {
          name: row.name,
          app_id: row.app_id,
          countries: row.countries,
          events_include: row.events_include,
          events_exclude: row.events_exclude,
          days_ago_start: row.days_ago_start,
          days_ago_end: row.days_ago_end,
          mode: row.mode,
          query: row.query,
          ttl: row.ttl,
          // NOTE: we're not sending id, user_list
        }
      });
      try {
        let res = await postApi('audiences', { audiences }, loading);
        $q.notify({ message: 'Audiences successfully updated', icon: 'success', timeout: 1000 });
        //store.updateAudiencesStat(res.data.results);
      }
      catch (e: any) {
        $q.dialog({
          title: 'Error',
          message: e.message,
        });
      }
    }
    const onAudiencesDownload = async () => {
      $q.dialog({
        title: 'Confirm',
        message: 'Are sure to reload audiences from the server. It will overwrite any pending changes you have not uploaded',
        cancel: true,
        persistent: true
      }).onOk(async () => {
        $q.loading.show({ message: 'Loading audiences...' });
        const loading = () => $q.loading.hide();
        try {
          let res = await getApi('audiences', {}, loading);
          const audiences = res.data.results;
          store.audiences = audiences;
        }
        catch (e: any) {
          $q.dialog({
            title: 'Error',
            message: e.message,
          });
        }
      });
    }

    return {
      store,
      data,
      onLoad: onStatLoad,
      onAppIdSelected,
      filterEvents,
      filterCountries,
      audienceForm,
      audience,
      onAudienceFormSave,
      onAudienceFormReset,
      onAudiencePreview,
      onAudienceGetQuery,
      onGetPower,
      onGetPowerQuery,
      onAudienceFilterCountries,
      onAudienceFilterEvents,
      onAudienceListEdit,
      onAudienceListDelete,
      onAudiencesUpload,
      onAudiencesDownload,
      formatArray,
    };
  }
});
</script>
