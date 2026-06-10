<template>
  <div class="glass-card p-6 rounded-2xl">
    <div class="flex items-center justify-between mb-6 flex-wrap gap-4">
      <h4 class="text-base font-semibold text-white">Event Log history</h4>
      <div class="flex gap-2 items-center">
        <button @click="store.clearHistory" class="bg-red-500/10 text-red-400 p-2.5 rounded-xl text-xs font-bold hover:bg-red-500 hover:text-white transition-colors w-10 h-10 flex items-center justify-center border border-red-500/15" title="Clear History">
          <i class="fas fa-trash-can text-sm"></i>
        </button>
        <select v-model="filter" @change="searchHistory" class="interactive-input px-3.5 py-2 text-xs font-bold cursor-pointer">
          <option value="">All Events</option>
          <option value="Health Check">Health Checks</option>
          <option value="Scan">Plex Scans</option>
          <option value="Error">System Errors</option>
          <option value="Corrupt">Corrupted Files</option>
        </select>
        <input type="text" v-model="search" @keyup.enter="searchHistory" placeholder="Search logs..." class="interactive-input px-3.5 py-2 text-xs w-44 md:w-56 placeholder-slate-500">
      </div>
    </div>
    <div class="overflow-x-auto rounded-xl border border-white/5">
      <table class="w-full text-left text-xs text-slate-400 custom-table">
        <thead>
          <tr class="text-[9px] uppercase bg-white/2 text-slate-300 tracking-wider">
            <th class="px-5 py-3.5">Timestamp</th>
            <th class="px-5 py-3.5">Category</th>
            <th class="px-5 py-3.5">Details</th>
            <th class="px-5 py-3.5 text-right">Status</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(r, index) in store.events" :key="index" class="transition-colors">
            <td class="px-5 py-3.5 font-mono text-[10px] text-slate-400 select-all">{{ r[0] }}</td>
            <td class="px-5 py-3.5 font-bold text-slate-200">{{ r[1] }}</td>
            <td class="px-5 py-3.5 text-slate-300 font-medium whitespace-normal break-all">{{ r[2] }}</td>
            <td class="px-5 py-3.5 text-right">
              <span :class="getStatusClass(r[3])" class="text-[9px] font-extrabold uppercase tracking-widest px-2 py-0.5 rounded-full">{{ r[3] }}</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="store.more" class="p-6 text-center">
      <button @click="store.loadHistory(searchQuery, true)" class="text-cyan-400 text-xs font-bold uppercase tracking-wider hover:text-white transition-colors">
        Load Older Entries
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useHistoryStore } from '../stores/history';

const store = useHistoryStore();
const filter = ref('');
const search = ref('');

const searchQuery = computed(() => (filter.value ? filter.value + " " + search.value : search.value));

const searchHistory = () => {
  store.loadHistory(searchQuery.value, false);
};

const getStatusClass = (status) => {
  if (status === 'SUCCESS') return "text-emerald-400 bg-emerald-500/10";
  if (status === 'ERROR') return "text-red-400 bg-red-500/10";
  if (status === 'WARNING') return "text-amber-400 bg-amber-500/10";
  return "text-slate-500 bg-slate-500/10";
};

onMounted(() => {
  store.loadHistory();
});
</script>
