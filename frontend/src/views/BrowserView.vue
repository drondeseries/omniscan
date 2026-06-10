<template>
  <div class="space-y-4">
    <div class="glass-card p-4.5 rounded-2xl flex flex-col md:flex-row gap-4 justify-between items-center">
      <div class="flex items-center gap-3 w-full md:w-auto overflow-hidden">
        <span class="text-xs text-slate-500 font-bold uppercase tracking-wide">Path:</span>
        <p class="text-xs font-mono text-cyan-400 truncate max-w-[200px] md:max-w-xl">{{ store.path }}</p>
        <button @click="store.loadBrowser(store.path)" class="w-7 h-7 rounded-lg bg-white/5 text-slate-400 hover:text-white flex-shrink-0 flex items-center justify-center border border-white/5 hover:border-white/10 transition-colors">
          <i class="fas fa-rotate-right text-xs"></i>
        </button>
      </div>
      <div class="flex gap-2 w-full md:w-auto">
        <input type="text" v-model="search" @keyup.enter="doSearch" placeholder="Search path or keywords..." class="interactive-input flex-grow md:w-64 px-3.5 py-2 text-xs font-mono placeholder-slate-500">
        <button @click="doSearch" :disabled="store.loading" class="action-btn text-white px-4 py-2 rounded-xl text-xs font-bold uppercase transition-all flex items-center gap-2 justify-center">
          <i v-if="!store.loading" class="fas fa-magnifying-glass"></i>
          <i v-else class="fas fa-circle-notch fa-spin"></i>
          <span>Go</span>
        </button>
      </div>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3.5">
      <div v-if="store.items.length === 0" class="col-span-full py-20 text-center text-slate-500 font-bold">No file matching criteria found</div>
      <div v-for="item in store.items" :key="item.path" @click="item.is_dir ? store.loadBrowser(item.path) : null" class="glass-card p-4 rounded-xl flex items-center gap-4.5 cursor-pointer">
        <div class="w-9 h-9 rounded-lg bg-white/5 flex items-center justify-center border border-white/5 flex-shrink-0 text-base">
          <i :class="item.is_dir ? 'fa-folder text-cyan-400' : 'fa-file-video text-slate-400'" class="fas"></i>
        </div>
        <div class="overflow-hidden flex-grow">
          <p class="text-xs font-semibold text-white truncate" :title="item.name">{{ item.name }}</p>
          <div class="flex justify-between items-center mt-1 text-[9px] font-bold text-slate-500 uppercase tracking-wider">
            <span>{{ item.size_fmt }}</span>
            <span>{{ item.date }}</span>
          </div>
        </div>
        <button v-if="item.is_dir && !item.is_back" @click.stop="store.scanPath(item.path)" class="w-8 h-8 rounded-lg bg-white/5 hover:bg-cyan-500 hover:text-white text-slate-400 transition-colors ml-2 flex-shrink-0 flex items-center justify-center border border-white/5" title="Scan Path">
          <i class="fas fa-bolt text-[10px]"></i>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useBrowserStore } from '../stores/browser';

const store = useBrowserStore();
const search = ref('');

const doSearch = () => {
  store.loadBrowser(search.value.startsWith('/') || search.value.startsWith('\\') || search.value.includes(':/') ? search.value : store.path, search.value.startsWith('/') ? "" : search.value);
};

onMounted(() => {
  store.loadBrowser();
});
</script>
