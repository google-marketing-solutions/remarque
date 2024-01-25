/*
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import { RouteRecordRaw } from 'vue-router';
import MainLayout from 'layouts/MainLayout.vue';
import IndexPage from 'pages/IndexPage.vue';
import ConfigurationPage from 'pages/ConfigurationPage.vue';
import AudiencesPage from 'pages/AudiencesPage.vue';
import GoogleAdsPage from 'pages/GoogleAdsPage.vue';
import SchedulePage from 'pages/SchedulePage.vue';

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    component: MainLayout,
    children: [
      {
        path: '',
        component: IndexPage,
      },
      {
        path: 'configuration',
        component: ConfigurationPage,
        meta: { title: 'Configuration' },
      },
      {
        path: 'audiences',
        component: AudiencesPage,
        meta: { title: 'Audiences' },
      },
      {
        path: 'google-ads',
        component: GoogleAdsPage,
        meta: { title: 'Google Ads' },
      },
      {
        path: 'schedule',
        component: SchedulePage,
        meta: { title: 'Scheduling' },
      },
    ],
  },

  // Always leave this as last one,
  // but you can also remove it
  {
    path: '/:catchAll(.*)*',
    component: () => import('pages/ErrorNotFound.vue'),
  },
];

export default routes;
