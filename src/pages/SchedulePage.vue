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
      <div class="text-h2">Scheduling</div>
    </div>

    <div class="q-mt-md">
      <q-card class="card" flat bordered>
        <q-card-section
          class="q-col-gutter-md"
          style="padding-top: 0; padding-bottom: 30px"
        >
          <div class="text-h6">Schedule execution</div>
          <div class="row">
            <div class="col-4">
              <q-toggle
                v-model="store.scheduled"
                indeterminate-value="null"
                label="Enabled"
              />
            </div>
          </div>
          <div class="row">
            <div class="col-4">
              <q-input
                filled
                v-model="store.schedule"
                mask="time"
                :rules="['time']"
              >
                <template v-slot:append>
                  <q-icon name="access_time" class="cursor-pointer">
                    <q-popup-proxy
                      cover
                      transition-show="scale"
                      transition-hide="scale"
                    >
                      <q-time v-model="store.schedule" format24h>
                        <div class="row items-center justify-end">
                          <q-btn
                            v-close-popup
                            label="Close"
                            color="primary"
                            flat
                          />
                        </div>
                      </q-time>
                    </q-popup-proxy>
                  </q-icon>
                </template>
              </q-input>
            </div>
            <div class="col-1"></div>
            <div class="col-4">
              <q-select
                outlined
                v-model="store.schedule_timezone"
                label="Timezone"
                :hide-bottom-space="true"
                :options="data.timeZonesSelect"
                use-input
                @filter="onTimezoneFilter"
                input-debounce="0"
                hint="Name of a timezone from tz database, e.g. Europe/Moscow, America/Los_Angeles, UTC"
              >
                <template v-slot:append>
                  <q-icon name="language" />
                </template>
              </q-select>
            </div>
          </div>
          <div class="row">
            <div class="col-4">
              <q-input
                filled
                v-model="store.schedule_email"
                label="Email"
                hint="Specify an email to send notifications about execution completions"
              ></q-input>
            </div>
          </div>
        </q-card-section>
        <q-card-actions class="q-pa-md">
          <q-btn
            label="Load"
            icon="download"
            size="md"
            @click="onScheduleLoad"
            color="primary"
            style="width: 130px"
          />
          <q-btn
            label="Save"
            icon="save"
            size="md"
            @click="onScheduleSave"
            color="primary"
            style="width: 130px"
            :disable="store.scheduled === undefined"
          />
        </q-card-actions>

        <div class="row">
          <div class="col-5 q-pa-lg">
            <q-table
              title="Job runs"
              flat
              bordered
              :rows="data.runs"
              :row-key="(r) => r"
              :columns="data.runsColumns"
              virtual-scroll
              :pagination="{ rowsPerPage: 0 }"
              :rows-per-page-options="[0]"
            >
              <template v-slot:body-cell-status="props">
                <q-td :props="props">
                  <q-chip
                    :color="props.row.status === 'Failure' ? 'red' : 'green'"
                    text-color="white"
                    dense
                    class="text-weight-bolder"
                    square
                    >{{ props.row.status }}</q-chip
                  >
                </q-td>
              </template>
            </q-table>
          </div>
        </div>
      </q-card>
    </div>
  </q-page>
</template>

<script lang="ts">
import { defineComponent, ref } from 'vue';
import { useQuasar } from 'quasar';
import { useConfigurationStore } from 'stores/configuration';
import { getApiUi, postApiUi } from 'boot/axios';
import { timeZones } from '../helpers/timezones';
import { formatDate } from '../helpers/utils';

/**
 * Response type for 'schedule' endpoint.
 */
interface GetScheduleResponse {
  scheduled: boolean;
  schedule: string;
  schedule_timezone: string;
  schedule_email: string;
  runs: string[][];
}
export default defineComponent({
  name: 'GoogleAdsPage',
  components: {},
  setup: () => {
    const store = useConfigurationStore();
    const $q = useQuasar();
    const data = ref({
      timeZonesSelect: timeZones,
      runs: <{ date: string; status: string }[]>[],
      runsColumns: [
        {
          name: 'date',
          label: 'Date',
          field: 'date',
          format: (v: unknown) => formatDate(v, true),
          sortable: true,
          align: 'left',
        },
        {
          name: 'status',
          label: 'Status',
          field: 'status',
        },
      ],
    });

    const onTimezoneFilter = (
      val: string,
      doneFn: (callbackFn: () => void) => void,
    ) => {
      doneFn(() => {
        data.value.timeZonesSelect = timeZones.filter((r) =>
          r.toLowerCase().includes(val?.toLowerCase()),
        );
      });
    };
    const onScheduleLoad = async () => {
      const res = await getApiUi<GetScheduleResponse>(
        'schedule',
        {},
        'Fetching Cloud Scheduler job...',
      );
      if (!res) return;
      if (res.data) {
        store.scheduled = res.data.scheduled;
        store.schedule = res.data.schedule;
        store.schedule_timezone = res.data.schedule_timezone;
        store.schedule_email = res.data.schedule_email;
        data.value.runs = res.data.runs.map((v: string[]) => {
          return { date: v[0], status: v[1] };
        });
      } else {
        store.scheduled = false;
        store.schedule = '';
        store.schedule_timezone = '';
        store.schedule_email = '';
      }
    };
    const onScheduleSave = async () => {
      if (store.scheduled && !store.schedule) {
        $q.dialog({
          message: 'For enabling scheduled execution please specify time',
          title: 'Error',
        });
        return;
      }
      await postApiUi(
        'schedule/edit',
        {
          scheduled: store.scheduled,
          schedule: store.schedule,
          schedule_timezone: store.schedule_timezone,
          schedule_email: store.schedule_email,
        },
        'Updating Cloud Scheduler job...',
      );
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
