<template>
  <q-page class="items-center justify-evenly" padding>
    <div class="row">
      <div class="text-h2">Scheduling</div>
    </div>

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
              <q-select outlined v-model="store.schedule_timezone" label="Timezone" :hide-bottom-space=true
                :options="data.timeZonesSelect" use-input @filter="onTimezoneFilter" input-debounce="0"
                hint="Name of a timezone from tz database, e.g. Europe/Moscow, America/Los_Angeles, UTC">
                <template v-slot:append>
                  <q-icon name="language" />
                </template>
              </q-select>
            </div>
          </div>
        </q-card-section>
        <q-card-actions class="q-pa-md">
          <q-btn label="Load" icon="download" size="md" @click="onScheduleLoad" color="primary" style="width:130px" />
          <q-btn label="Save" icon="save" size="md" @click="onScheduleSave" color="primary" style="width:130px" />
        </q-card-actions>
      </q-card>
    </div>
  </q-page>
</template>

<script lang="ts">
import { defineComponent, ref } from 'vue';
import { useQuasar } from 'quasar';
import { configurationStore } from 'stores/configuration';
import { getApi, postApi } from 'boot/axios';
import { timeZones } from '../helpers/timezones';

export default defineComponent({
  name: 'GoogleAdsPage',
  components: {},
  setup: () => {
    const store = configurationStore();
    const $q = useQuasar();
    const data = ref({
      timeZonesSelect: timeZones,
    });

    const onTimezoneFilter = (val: string, doneFn: (callbackFn: () => void) => void, abortFn: () => void) => {
      doneFn(() => {
        data.value.timeZonesSelect = timeZones.filter(r => r.toLowerCase().includes(val?.toLowerCase()));
      });
    }
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

    return {
      store,
      data,
      onTimezoneFilter,
      onScheduleLoad,
      onScheduleSave,
    };
  },
});
</script>
