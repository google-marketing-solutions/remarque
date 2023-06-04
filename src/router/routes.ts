import { RouteRecordRaw } from 'vue-router';
import MainLayout from 'layouts/MainLayout.vue';
import IndexPage from 'pages/IndexPage.vue';
import ConfigurationPage from 'pages/ConfigurationPage.vue';
import DatasourcePage from 'pages/DatasourcePage.vue';
import AudiencesPage from 'pages/AudiencesPage.vue';
import GoogleAdsPage from 'pages/GoogleAdsPage.vue';

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
      },
      {
        path: 'audiences',
        component: AudiencesPage,
      },
      {
        path: 'google-ads',
        component: GoogleAdsPage,
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
