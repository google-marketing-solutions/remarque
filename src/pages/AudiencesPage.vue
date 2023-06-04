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
              Define time window and load events from Google Analytics. Then choose an app, you'll see available events
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
              :loading="data.loading" />
          </div>

          <div class="col">
            <q-table title="GA4 Events" style="height: 400px" flat bordered :rows="data.events" row-key="event"
              :columns="data.events_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }" :rows-per-page-options="[0]"
              :no-data-label="data.app_ids.length ? 'Choose an app id' : 'Load all events'" :loading="data.loading"
              :filter-method="filterEvents" :filter="data.eventsSearch">
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
              :no-data-label="data.app_ids.length ? 'Choose an app id' : 'Load all events'" :loading="data.loading"
              v-model:selected="data.selectedCountries" :filter-method="filterCountries" :filter="data.countriesSearch">
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
        <div>
          <q-banner class="bg-grey-3">
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
                  label="Events to exclude" hint="GA4 events that did NOT happen for users" new-value-mode="add-unique" />
              </div>
              <div class="">
                <div class="row q-col-gutter-md">
                  <q-input class="col-3 q-gutter-md1" outlined v-model="audience.days_ago_start" label="Period start"
                    placeholder="" hint="days ago" />
                  <q-input class="col-3 q-gutter-md1" outlined v-model="audience.days_ago_end" label="Period end"
                    placeholder="" hint="days ago" />
                  <q-toggle class="col-3 q-gutter-md1" v-model="audience.active" label="Active" />
                </div>

              </div>
            </div>
          </div>
          <div class="q-pa-md">
            <q-btn label="Update" @click="onAudienceFormSave" color="primary" />
            <q-btn label="Reset" type="reset" color="primary" flat class="q-ml-sm" />
          </div>
        </q-form>
      </q-card-section>
      <q-card-section>
        <div class="">
          <q-table title="Audiences" style="height: 400px" flat bordered :rows="data.audiences" row-key="name"
            :columns="data.audiences_columns" virtual-scroll :pagination="{ rowsPerPage: 0 }"
            :rows-per-page-options="[0]">
            <template v-slot:body-cell-actions="props">
              <q-td :props="props">
                <q-btn dense round flat color="grey" @click="onAudienceListEdit(props)" icon="edit"></q-btn>
                <q-btn dense round flat color="grey" @click="onAudienceListDelete(props)" icon="delete"></q-btn>
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
</template>

<style>
.period-card {
  padding: 0px 10px;
}
</style>

<script lang="ts">
import { defineComponent, ref, watch, computed } from 'vue';
import { configurationStore } from 'stores/configuration';
import { getApi, postApi } from 'boot/axios';
import { QForm, useQuasar } from 'quasar';
import { formatArray } from '../helpers/utils';

export default defineComponent({
  name: 'AudiencesPage',
  components: {},
  setup: () => {
    const store = configurationStore();
    const $q = useQuasar();

    const data = ref({
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
        // { name: 'id', label: 'Id', field: 'id', sortable: true },
        { name: 'app_id', label: 'App id', field: 'app_id', sortable: true },
        { name: 'countries', label: 'Countries', field: 'countries', sortable: true, format: formatArray },
        { name: 'events_include', label: 'Include events', field: 'events_include', sortable: true, format: formatArray },
        { name: 'events_exclude', label: 'Exclude events', field: 'events_exclude', sortable: true, format: formatArray },
        { name: 'days_ago_start', label: 'Start', field: 'days_ago_start' },
        { name: 'days_ago_end', label: 'End', field: 'days_ago_end' },
        { name: 'active', label: 'Active', field: 'active' },
        { name: 'actions', label: 'Actions', field: '', align: 'center' },
      ],
      loading: false,
      eventsSearch: '',
      countriesSearch: '',
      audiences: computed(() => store.audiences)
    });
    let audience = ref({
      name: '',
      id: '',
      active: false,
      app_id: '',
      countries: [] as string[],
      allCountries: [] as string[],
      allCountriesSelect: [] as string[],
      allEvents: [] as string[],
      allEventsSelect: [] as string[],
      events_include: ['first_open'] as string[],
      events_exclude: ['remove'] as string[],
      days_ago_start: undefined,//store.days_ago_start,
      days_ago_end: undefined, //store.days_ago_end,
      user_list: ''
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
      data.value.loading = true;
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
          data.value.loading = false;
        })
        .catch((e) => {
          $q.notify({
            color: 'negative',
            message: 'Loading failed: ' + e.message,
            icon: 'report_problem'
          });
          data.value.loading = false;
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

    // Audiences
    const saveAudience = () => {
      let idx = data.value.audiences.findIndex(val => val.name === audience.value.name);
      const obj = {
        name: audience.value.name,
        app_id: audience.value.app_id,
        countries: audience.value.countries,
        events_include: audience.value.events_include,
        events_exclude: audience.value.events_exclude,
        days_ago_start: audience.value.days_ago_start || store.days_ago_start || 0,
        days_ago_end: audience.value.days_ago_end || store.days_ago_end || 0,
        user_list: audience.value.user_list,
        active: audience.value.active,
      }
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
          //saveAudience();
        }
      });
    }
    const onAudienceFormReset = () => {
      audience.value.name = '';
      audience.value.app_id = '';
      audience.value.countries = [];
      audience.value.events_include = ['first_open'] as string[],
        audience.value.events_exclude = ['remove'] as string[],
        audience.value.days_ago_start = undefined, //store.days_ago_start;
        audience.value.days_ago_end = undefined,//store.days_ago_end;
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
          active: row.active,
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
        const loading = $q.notify('Downloading audiences...');
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
      onAudienceFilterCountries,
      onAudienceFilterEvents,
      onAudienceListEdit,
      onAudienceListDelete,
      onAudiencesUpload,
      onAudiencesDownload
    };
  }
});
</script>
