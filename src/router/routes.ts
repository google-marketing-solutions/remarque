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
