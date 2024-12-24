<template>
  <q-dialog
    v-model="show"
    :persistent="isProcessing && !isStopped"
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
          :disable="isProcessing && !isStopped"
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
              <q-item-label
                caption
                v-if="getResult(audience.name)"
                v-let="{ result: getResult(audience.name) }"
              >
                <template v-if="typeof getResult(audience.name) === 'string'">
                  {{ getResult(audience.name) }}
                </template>
                <template
                  v-else-if="isAudienceProcessResult(getResult(audience.name))"
                >
                  <div>
                    Test users: {{ formatResult(audience.name, 'test') }} <br />
                    Control users: {{ formatResult(audience.name, 'control')
                    }}<br />
                    Uploaded users:
                    {{
                      (getResult(audience.name) as AudienceProcessResult)
                        .uploaded_user_count
                    }}
                  </div>
                  <div
                    class="q-mt-sm"
                    v-if="
                      (getResult(audience.name) as AudienceProcessResult)
                        .metrics
                    "
                  >
                    <q-btn
                      flat
                      dense
                      color="primary"
                      label="Metrics"
                      @click="showMetrics(audience.name)"
                    />
                    <q-btn
                      flat
                      dense
                      color="primary"
                      label="Distributions"
                      @click="showDistributions(audience.name)"
                      class="q-ml-sm"
                    />
                  </div>
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
          label="Stop"
          color="primary"
          @click="handleStop"
          :disable="isStopped"
        />
        <q-btn
          v-if="isProcessing"
          flat
          label="Cancel"
          color="primary"
          @click="handleCancel"
        />
        <q-btn
          flat
          label="Close"
          color="primary"
          @click="handleClose"
          :disable="isProcessing && !isStopped"
        />
      </q-card-actions>
    </q-card>
  </q-dialog>

  <!-- Metrics Dialog -->
  <q-dialog v-model="showMetricsDialog">
    <q-card style="width: 90vw; max-width: 1200px">
      <q-card-section class="row items-center">
        <div class="text-h6">Split Metrics</div>
        <q-space />
        <q-btn icon="close" flat round dense v-close-popup />
      </q-card-section>

      <q-card-section class="q-pa-none">
        <q-table
          :rows="metricsRows"
          :columns="metricsColumns"
          row-key="feature"
          class="full-width"
          :pagination="{ rowsPerPage: 0 }"
        >
          <template v-slot:body-cell-warnings="props">
            <q-td :props="props">
              <template v-if="props.row.warnings">
                <div
                  v-for="warning in props.row.warnings"
                  :key="warning"
                  class="text-negative"
                >
                  {{ warning }}
                </div>
              </template>
            </q-td>
          </template>
        </q-table>
      </q-card-section>
    </q-card>
  </q-dialog>

  <!-- Distributions Dialog -->
  <q-dialog v-model="showDistributionsDialog" maximized>
    <q-card>
      <q-card-section class="row items-center">
        <div class="text-h6">Feature Distributions</div>
        <q-space />
        <q-btn icon="close" flat round dense v-close-popup />
      </q-card-section>

      <q-card-section class="row q-col-gutter-md">
        <template
          v-for="dist in selectedDistributions"
          :key="dist.feature_name"
        >
          <div
            class="col-12"
            :class="dist.is_numeric ? 'col-md-6' : 'col-md-12'"
          >
            <q-card>
              <q-card-section>
                <apexchart height="350" v-bind="getDistributionChart(dist)" />
              </q-card-section>
            </q-card>
          </div>
        </template>
      </q-card-section>
    </q-card>
  </q-dialog>
</template>

<script setup lang="ts">
import { ref, onBeforeUnmount, watch, computed } from 'vue';
import { AudienceMode, AudienceWithLog } from 'stores/audiences';
import {
  AudienceProcessResult,
  AudiencesProcessResponse,
  Distributions as DistributionData,
} from 'src/boot/api';
import { postApi } from 'src/boot/axios';
import { QTableColumn } from 'quasar';

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
  abortController?: AbortController;
}

enum ProcessMode {
  Default,
  OnlySampling,
  OnlyUploading,
}

interface Props {
  modelValue: boolean;
  audiences: AudienceWithLog[];
}
const props = defineProps<Props>();

const emit = defineEmits<{
  (e: 'update:modelValue', show: boolean): void;
}>();

const processingStatus = ref<Record<string, ProcessingStatus>>({});
const isProcessing = ref(false);
const isStopped = ref(false);
const show = ref(props.modelValue);
const audienceModes = ref<Record<string, AudienceMode>>({});
const timer = ref<number | null>(null); // for setInterval
const processMode = ref<ProcessMode>(ProcessMode.Default);
const showMetricsDialog = ref(false);
const showDistributionsDialog = ref(false);
const selectedMetrics = ref<Map<string, Record<string, string>> | null>(null);
const selectedDistributions = ref<DistributionData[]>([]);

const metricsColumns: QTableColumn[] = [
  {
    name: 'feature',
    label: 'Feature',
    field: 'feature',
    align: 'left',
  },
  {
    name: 'mean_ratio',
    label: 'Mean Ratio',
    field: 'mean_ratio',
    format: (val: number | null) => (val ? (val * 100).toFixed(1) + '%' : '-'),
  },
  {
    name: 'std_ratio',
    label: 'Std Ratio',
    field: 'std_ratio',
    format: (val: number | null) => (val ? (val * 100).toFixed(1) + '%' : '-'),
  },
  {
    name: 'ks_statistic',
    label: 'KS Statistic',
    field: 'ks_statistic',
    format: (val: number | null) => val?.toFixed(3) ?? '-',
  },
  {
    name: 'p_value',
    label: 'P-Value',
    field: 'p_value',
    format: (val: number | string | null) =>
      typeof val === 'number' ? val.toFixed(3) : (val ?? '-'),
  },
  {
    name: 'warnings',
    label: 'Warnings',
    field: 'warnings',
  },
];

const metricsRows = computed(() => {
  if (!selectedMetrics.value) return [];

  return Object.entries(selectedMetrics.value).map(([feature, metrics]) => ({
    feature,
    ...metrics,
    warnings: metrics.warnings ? Object.values(metrics.warnings) : null,
  }));
});

const showMetrics = (audienceName: string) => {
  const result = getResult(audienceName) as AudienceProcessResult;
  if (result?.metrics) {
    selectedMetrics.value = result.metrics;
    showMetricsDialog.value = true;
  }
};

const showDistributions = (audienceName: string) => {
  const result = getResult(audienceName) as AudienceProcessResult;
  if (result?.distributions) {
    selectedDistributions.value = result.distributions;
    showDistributionsDialog.value = true;
  }
};

const prepareHistogramData = (values: number[], bins: number[]) => {
  const counts = new Array(bins.length - 1).fill(0);
  const total = values.length;

  values.forEach((value) => {
    for (let i = 0; i < bins.length - 1; i++) {
      if (value >= bins[i] && value < bins[i + 1]) {
        counts[i]++;
        break;
      }
    }
  });

  // Convert to percentages
  return counts.map((count) => (count / total) * 100);
};

const getNumericDistributionData = (dist: DistributionData) => {
  const allValues = [...dist.test_values!, ...dist.control_values!];
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);

  // For integer features
  if (allValues.every((v) => Number.isInteger(v))) {
    const bins = Array.from({ length: max - min + 2 }, (_, i) => min + i - 0.5);
    const categories = Array.from({ length: max - min + 1 }, (_, i) => min + i);

    const data = {
      categories,
      bins,
      test: prepareHistogramData(dist.test_values!, bins),
      control: prepareHistogramData(dist.control_values!, bins),
    };
    console.log('Prepared numeric data:', data);
    return data;
  }

  // For continuous features
  const n_bins = 30;
  const bins = Array.from(
    { length: n_bins + 1 },
    (_, i) => min + (max - min) * (i / n_bins),
  );
  const categories = bins.slice(0, -1).map((v, i) => (v + bins[i + 1]) / 2);

  return {
    categories,
    bins,
    test: prepareHistogramData(dist.test_values!, bins),
    control: prepareHistogramData(dist.control_values!, bins),
  };
};

const getDistributionChart = (dist: DistributionData) => {
  if (dist.is_numeric) {
    const data = getNumericDistributionData(dist);

    return {
      options: {
        chart: {
          type: 'line',
          zoom: { enabled: false },
        },
        stroke: {
          curve: 'stepline',
          width: 2,
        },
        title: {
          text: dist.feature_name,
          align: 'left',
        },
        xaxis: {
          categories: data.categories,
          title: {
            text: 'Value',
          },
          labels: {
            formatter: function (val: any) {
              return val?.toString() ?? '';
            },
          },
        },
        yaxis: {
          title: {
            text: 'Percentage of Users',
          },
          labels: {
            formatter: (val: number) => val.toFixed(1) + '%',
          },
        },
        tooltip: {
          x: {
            formatter: (val: number) =>
              Number.isInteger(val) ? val.toString() : val.toFixed(1),
          },
          y: {
            formatter: (val: number) => val.toFixed(1) + '%',
          },
        },
      },
      series: [
        {
          name: 'Test',
          data: data.test,
        },
        {
          name: 'Control',
          data: data.control,
        },
      ],
    };
  } else {
    // For categorical features
    return {
      options: {
        chart: {
          type: 'bar',
          height: Math.max(350, dist.categories!.length * 50),
        },
        plotOptions: {
          bar: {
            horizontal: true,
            dataLabels: {
              position: 'right',
              formatter: (val: number) => val.toFixed(1) + '%',
            },
          },
        },
        title: {
          text: dist.feature_name,
          align: 'left',
        },
        xaxis: {
          categories: dist.categories,
          title: {
            text: 'Percentage',
          },
          labels: {
            formatter: (val: number) => val.toFixed(1) + '%',
          },
        },
        yaxis: {
          labels: {
            maxWidth: 150,
          },
        },
        tooltip: {
          y: {
            formatter: (val: number) => val.toFixed(1) + '%',
          },
        },
      },
      series: [
        {
          name: 'Test',
          data: dist.test_distribution!.map((v) => v * 100),
        },
        {
          name: 'Control',
          data: dist.control_distribution!.map((v) => v * 100),
        },
      ],
    };
  }
};

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
  isStopped.value = false;
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
    if (isStopped.value) {
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
    const abortController = new AbortController();
    processingStatus.value[audience.name] = {
      status: 'processing',
      result: null,
      startTime: startTime,
      duration: 0,
      abortController,
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
            processMode.value === ProcessMode.OnlySampling ? true : undefined,
          include_distributions: true,
        },
        undefined,
        { signal: abortController.signal },
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
        result: error instanceof Error ? error.message : 'Processing failed',
        endTime,
        duration,
      };
    }
  }
  stopTimer();
  isProcessing.value = false;
};

const handleStop = () => {
  isStopped.value = true;
};

const handleCancel = async () => {
  // Find currently processing audience and cancel its request
  const processingAudience = Object.entries(processingStatus.value).find(
    ([_, status]) => status.status === 'processing',
  );

  if (processingAudience) {
    const [name, status] = processingAudience;
    status.abortController?.abort();

    processingStatus.value[name] = {
      status: 'cancelled',
      result: 'Operation cancelled',
      startTime: status.startTime,
      endTime: Date.now(),
      duration: status.startTime ? Date.now() - status.startTime : 0,
    };
  }

  // Mark remaining as cancelled
  Object.keys(processingStatus.value).forEach((name) => {
    if (processingStatus.value[name].status === 'pending') {
      processingStatus.value[name] = {
        status: 'cancelled',
        result: null,
        duration: 0,
      };
    }
  });

  isStopped.value = true; // Set stopped flag to prevent further processing
  stopTimer();
  isProcessing.value = false;
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

const getResult = (
  audienceName: string,
): string | AudienceProcessResult | null => {
  return processingStatus.value[audienceName]?.result || null;
};

const formatResult = (audienceName: string, groupName: string): string => {
  const res = getResult(audienceName);
  if (typeof res === 'string') {
    return res;
  } else if (res) {
    if (groupName === 'test') {
      return `${res.test_user_count} (${res.new_test_user_count || 0} new, ${res.total_test_user_count || 0} total)`;
    } else if (groupName === 'control') {
      return `${res.control_user_count} (${res.new_control_user_count || 0} new, ${res.total_control_user_count || 0} total)`;
    }
  }
  return '';
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

function isAudienceProcessResult(
  result: string | AudienceProcessResult | null,
): result is AudienceProcessResult {
  return result !== null && typeof result !== 'string';
}
</script>
