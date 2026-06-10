<template>
  <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
    <StatCard label="Watching Paths" :value="statsStore.stats.watching_count" icon="fa-eye" />
    <StatCard label="Active Queue" :value="statsStore.stats.pending?.length || 0" icon="fa-hourglass-half" />
    <StatCard label="Corrupt Media" :value="statsStore.stats.corrupt_count" icon="fa-circle-exclamation" color="text-red-400" />
    <StatCard label="Missing from Server" :value="statsStore.stats.total_missing" icon="fa-file-circle-minus" color="text-yellow-400" />
  </div>
  <RecentImports v-if="statsStore.stats.recent_imports" :recentImports="statsStore.stats.recent_imports" />
  <LibraryList v-if="statsStore.stats.libraries" :libraries="statsStore.stats.libraries" />
</template>

<script setup>
import { useStatsStore } from '../stores/stats';
import StatCard from './StatCard.vue';
import RecentImports from './RecentImports.vue';
import LibraryList from './LibraryList.vue';

const statsStore = useStatsStore();
</script>
