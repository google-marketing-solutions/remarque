<template>
  <q-dialog
    v-model="show"
    :persistent="isProcessing && !isCancelled"
    :maximized="false"
    transition-show="scale"
    transition-hide="scale"
  >
    <q-card style="min-width: 600px">
      <q-card-section class="row items-center">
        <div class="text-h6">Processing Audiences</div>
        <q-space />
        <q-btn
          icon="close"
          flat
          round
          dense
          v-close-popup
          :disable="isProcessing && !isCancelled"
        />
      </q-card-section>
      <q-card-section>
        <q-btn-toggle
          v-model="processMode"
          no-wrap
          :disable="isProcessing"
          :options="[
            { label: 'Full', value: ProcessMode.Default },
            { label: 'Only Sampling', value: ProcessMode.OnlySampling },
            { label: 'Only Uploading', value: ProcessMode.OnlyUploading },
          ]"
        />
      </q-card-section>
      <q-card-section style="max-height: 60vh" class="scroll">
        <q-list separator>
          <q-item v-for="audience in audiences" :key="audience.name">
            <q-item-section avatar style="width: 150px">
              <div class="row no-wrap items-start">
                <q-icon
                  v-if="getStatus(audience.name) === 'completed'"
                  name="check_circle"
                  color="positive"
                  size="sm"
                />
                <q-icon
                  v-else-if="getStatus(audience.name) === 'failed'"
                  name="error"
                  color="negative"
                  size="sm"
                />
                <q-circular-progress
                  v-else-if="getStatus(audience.name) === 'processing'"
                  indeterminate
                  color="primary"
                  size="sm"
                />
                <q-icon
                  v-else-if="getStatus(audience.name) === 'cancelled'"
                  name="cancel"
                  color="grey"
                  size="sm"
                />
                <q-icon
                  v-else-if="getStatus(audience.name) === 'skipped'"
                  name="block"
                  color="grey"
                  size="sm"
                />
                <q-icon v-else name="schedule" color="grey" size="sm" />
                <div class="q-ml-sm">
                  {{ capitalize(getStatus(audience.name)) }}
                  <template
                    v-if="
                      ['processing', 'completed', 'failed'].includes(
                        getStatus(audience.name),
                      )
                    "
                  >
                    <div class="text-caption text-grey">
                      {{ getCurrentDuration(audience.name) }}
                    </div>
                  </template>
                </div>
              </div>
            </q-item-section>

            <q-item-section>
              <q-item-label>{{ audience.name }}</q-item-label>
              <q-item-label caption v-if="getResult(audience.name)">
                <template v-if="typeof getResult(audience.name) === 'string'">
                  {{ getResult(audience.name) }}
                </template>
                <template v-else>
                  Test users: {{ getResult(audience.name).test_user_count
                  }}<br />
                  Control users: {{ getResult(audience.name).control_user_count
                  }}<br />
                  Uploaded users:
                  {{ getResult(audience.name).uploaded_user_count }}
                </template>
              </q-item-label>
            </q-item-section>

            <!-- Mode toggle -->
            <q-item-section side style="min-width: 200px">
              <q-btn-toggle
                v-model="audienceModes[audience.name]"
                no-wrap
                outline
                dense
                :disable="isProcessing"
                :toggle-color="
                  audienceModes[audience.name] === 'off'
                    ? 'red'
                    : audienceModes[audience.name] === 'test'
                      ? 'blue'
                      : 'green'
                "
                :options="[
                  { label: 'Off', value: 'off' },
                  { label: 'Test', value: 'test' },
                  { label: 'Prod', value: 'prod' },
                ]"
                @update:model-value="updateAudienceMode(audience.name, $event)"
              />
            </q-item-section>
          </q-item>
        </q-list>
      </q-card-section>

      <q-card-section v-if="isProcessing">
        <q-banner class="bg-grey-3">
          <template v-slot:avatar>
            <q-icon name="info" color="primary" />
          </template>
          Processing audiences... Please wait or click Cancel to stop after the
          current operation.
        </q-banner>
      </q-card-section>

      <q-card-actions align="right">
        <q-btn
          v-if="!isProcessing"
          label="Run"
          color="primary"
          :disable="!hasEnabledAudiences"
          @click="startProcessing"
        />
        <q-btn
          v-if="isProcessing"
          flat
          label="Cancel"
          color="primary"
          @click="handleCancel"
          :disable="isCancelled"
        />
        <q-btn
          flat
          label="Close"
          color="primary"
          @click="handleClose"
          :disable="isProcessing && !isCancelled"
        />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script lang="ts">
import {
  defineComponent,
  ref,
  onBeforeUnmount,
  PropType,
  watch,
  computed,
} from 'vue';
import { AudienceMode, AudienceWithLog } from 'stores/audiences';
import { AudienceProcessResult, AudiencesProcessResponse } from 'src/boot/api';
import { postApi } from 'src/boot/axios';

export interface ProcessingStatus {
  status:
    | 'pending'
    | 'processing'
    | 'completed'
    | 'failed'
    | 'cancelled'
    | 'skipped';
  result: AudienceProcessResult | null | string;
  startTime?: number; // timestamp when processing started
  endTime?: number; // timestamp when processing ended
  duration?: number; // duration in milliseconds
}

enum ProcessMode {
  Default,
  OnlySampling,
  OnlyUploading,
}
export default defineComponent({
  name: 'ProcessingDialog',

  props: {
    modelValue: {
      type: Boolean,
      required: true,
    },
    audiences: {
      type: Array as PropType<AudienceWithLog[]>,
      required: true,
    },
  },

  emits: ['update:modelValue'],

  setup(props, { emit }) {
    const processingStatus = ref<Record<string, ProcessingStatus>>({});
    const isProcessing = ref(false);
    const isCancelled = ref(false);
    const show = ref(props.modelValue);
    const audienceModes = ref<Record<string, AudienceMode>>({});
    const timer = ref<number | null>(null); // for setInterval
    const processMode = ref<ProcessMode>(ProcessMode.Default);

    // Sync show with v-model
    watch(
      () => props.modelValue,
      (val) => {
        show.value = val;
      },
    );
    watch(
      () => show.value,
      (val) => {
        emit('update:modelValue', val);
      },
    );

    watch(
      () => show.value,
      (newVal) => {
        if (newVal) {
          initializeModes();
        }
      },
    );

    onBeforeUnmount(() => {
      stopTimer();
    });

    const initializeModes = () => {
      audienceModes.value = props.audiences.reduce(
        (acc, audience) => {
          acc[audience.name] = audience.mode;
          return acc;
        },
        {} as Record<string, AudienceMode>,
      );
    };

    const hasEnabledAudiences = computed(() => {
      return Object.values(audienceModes.value).some((mode) => mode !== 'off');
    });

    const updateAudienceMode = (audienceName: string, mode: AudienceMode) => {
      audienceModes.value[audienceName] = mode;
    };

    const startProcessing = () => {
      isProcessing.value = true;
      isCancelled.value = false;
      processingStatus.value = props.audiences.reduce(
        (acc, audience) => {
          if (audienceModes.value[audience.name] !== 'off') {
            acc[audience.name] = { status: 'pending', result: null };
          } else {
            acc[audience.name] = { status: 'skipped', result: null };
          }
          return acc;
        },
        {} as Record<string, ProcessingStatus>,
      );

      processAudiences();
    };

    const processAudiences = async () => {
      startTimer();
      for (const audience of props.audiences) {
        if (audienceModes.value[audience.name] === 'off') {
          continue;
        }
        if (isCancelled.value) {
          Object.keys(processingStatus.value).forEach((name) => {
            if (processingStatus.value[name].status === 'pending') {
              processingStatus.value[name] = {
                status: 'cancelled',
                result: null,
                duration: 0,
              };
            }
          });
          break;
        }

        const startTime = Date.now();
        processingStatus.value[audience.name] = {
          status: 'processing',
          result: null,
          startTime: startTime,
          duration: 0,
        };

        try {
          const result = await postApi<AudiencesProcessResponse>(
            processMode.value === ProcessMode.OnlyUploading
              ? 'ads/upload'
              : 'process',
            {
              audience: audience.name,
              mode: audienceModes.value[audience.name],
              skip_upload:
                processMode.value === ProcessMode.OnlySampling
                  ? true
                  : undefined,
            },
          );
          const endTime = Date.now();
          const duration = endTime - startTime;

          processingStatus.value[audience.name] = {
            status: 'completed',
            result: result.data.result[audience.name],
            endTime,
            duration,
          };
        } catch (error) {
          const endTime = Date.now();
          const duration = endTime - startTime;

          processingStatus.value[audience.name] = {
            status: 'failed',
            result:
              error instanceof Error ? error.message : 'Processing failed',
            endTime,
            duration,
          };
        }
      }
      stopTimer();
      isProcessing.value = false;
    };

    const handleCancel = () => {
      isCancelled.value = true;
    };

    const handleClose = () => {
      if (
        !isProcessing.value ||
        window.confirm('Are you sure you want to close while processing?')
      ) {
        show.value = false;
      }
    };

    const getStatus = (audienceName: string): string => {
      return processingStatus.value[audienceName]?.status;
    };

    const formatDuration = (ms: number) => {
      const seconds = Math.floor(ms / 1000);
      const minutes = Math.floor(seconds / 60);
      const remainingSeconds = seconds % 60;
      return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
    };

    const getCurrentDuration = (audienceName: string) => {
      const status = processingStatus.value[audienceName];
      if (status?.duration) {
        return formatDuration(status.duration);
      }
      return '';
    };

    const getResult = (audienceName: string): any => {
      return processingStatus.value[audienceName]?.result || null;
    };

    const capitalize = (str: string): string => {
      return str ? str.charAt(0).toUpperCase() + str.slice(1) : '';
    };

    const startTimer = () => {
      timer.value = window.setInterval(() => {
        Object.keys(processingStatus.value).forEach((name) => {
          const status = processingStatus.value[name];
          if (status.status === 'processing' && status.startTime) {
            status.duration = Date.now() - status.startTime;
          }
        });
      }, 1000);
    };

    const stopTimer = () => {
      if (timer.value) {
        clearInterval(timer.value);
        timer.value = null;
      }
    };

    return {
      show,
      processMode,
      ProcessMode,
      isProcessing,
      isCancelled,
      audienceModes,
      hasEnabledAudiences,
      startProcessing,
      updateAudienceMode,
      handleCancel,
      handleClose,
      getStatus,
      getCurrentDuration,
      getResult,
      capitalize,
    };
  },
});
</script>
