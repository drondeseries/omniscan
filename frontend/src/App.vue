<template>
  <div class="flex h-screen w-screen overflow-hidden relative bg-[#030712]">
    <Sidebar />
    
    <main class="flex-grow flex flex-col h-screen overflow-hidden z-10 bg-transparent">
      <TopBar :title="pageTitle" :uptime="statsStore.stats.uptime || '00:00:00'" />
      
      <div class="flex-grow overflow-y-auto p-4 lg:p-10">
        <router-view />
      </div>
    </main>
  </div>
</template>

<script setup>
import { computed, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { useStatsStore } from './stores/stats';
import Sidebar from './components/layout/Sidebar.vue';
import TopBar from './components/layout/TopBar.vue';

const statsStore = useStatsStore();
const route = useRoute();

const pageTitle = computed(() => {
  const titles = {
    dashboard: 'Dashboard',
    history: 'Event History',
    logs: 'Live Console',
    browser: 'File Browser',
    settings: 'Configuration Settings'
  };
  return titles[route.name] || 'Dashboard';
});

onMounted(() => {
  statsStore.fetchStats();
  setInterval(statsStore.fetchStats, 2000);
});
</script>
