<template>
  <div class="pb-24">
    <form @submit.prevent="save" class="space-y-6 max-w-4xl mx-auto">
      <div class="grid grid-cols-3 gap-3">
        <div @click="form.server_type = 'plex'" :class="form.server_type === 'plex' ? 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-cyan-500 font-bold text-white bg-cyan-500/5' : 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-transparent font-bold text-slate-400'">Plex</div>
        <div @click="form.server_type = 'jellyfin'" :class="form.server_type === 'jellyfin' ? 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-cyan-500 font-bold text-white bg-cyan-500/5' : 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-transparent font-bold text-slate-400'">Jellyfin</div>
        <div @click="form.server_type = 'emby'" :class="form.server_type === 'emby' ? 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-cyan-500 font-bold text-white bg-cyan-500/5' : 'glass-card p-4 rounded-xl text-center cursor-pointer border-2 border-transparent font-bold text-slate-400'">Emby</div>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div class="glass-card p-6 rounded-2xl space-y-4">
          <h3 class="font-bold text-sm text-cyan-400 border-b border-white/5 pb-2 uppercase tracking-wider flex items-center gap-2"><i class="fas fa-network-wired text-xs"></i> Connection</h3>
          
          <div v-if="form.server_type === 'plex'" class="space-y-4">
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Plex Server URL</label><input type="text" v-model="form.plex_server" class="interactive-input w-full p-3 text-xs"></div>
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Plex Token</label><input type="password" v-model="form.plex_token" class="interactive-input w-full p-3 text-xs"></div>
          </div>
          <div v-else class="space-y-4">
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Server API URL</label><input type="text" v-model="form.server_url" class="interactive-input w-full p-3 text-xs"></div>
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">API Authentication Key</label><input type="password" v-model="form.api_key" class="interactive-input w-full p-3 text-xs"></div>
          </div>
          
          <div class="pt-2">
            <button type="button" @click="testConn" class="w-full bg-cyan-500/10 hover:bg-cyan-500 text-cyan-400 hover:text-white border border-cyan-500/20 p-3 rounded-xl text-xs font-bold uppercase tracking-wider transition-all">Test Connection Link</button>
          </div>
        </div>

        <div class="glass-card p-6 rounded-2xl space-y-4">
          <div class="flex justify-between items-center border-b border-white/5 pb-2">
            <h3 class="font-bold text-sm text-cyan-400 uppercase tracking-wider flex items-center gap-2"><i class="fas fa-folder-tree text-xs"></i> Libraries</h3>
            <button type="button" @click="checkPaths" class="text-[9px] text-cyan-400 font-black uppercase tracking-wider hover:text-white">Verify Paths</button>
          </div>
          <div>
            <label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Media Library Scan Paths (comma/newline separated)</label>
            <textarea v-model="form.scan_directories" rows="3" class="interactive-input w-full p-3 text-xs font-mono"></textarea>
          </div>
          <div v-if="pathErrors.length" class="space-y-1 mt-1">
            <p v-for="err in pathErrors" :key="err" class="text-[10px] text-red-400 font-bold"><i class="fas fa-triangle-exclamation mr-1"></i> Invalid: {{ err }}</p>
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Debounce Delay (Sec)</label><input type="number" v-model="form.scan_debounce" class="interactive-input w-full p-3 text-xs"></div>
            <div><label class="block text-[9px] font-black uppercase text-slate-400 tracking-wider mb-1">Scanner Thread Workers</label><input type="number" v-model="form.scan_workers" class="interactive-input w-full p-3 text-xs"></div>
          </div>
        </div>
      </div>
      
      <div class="sticky bottom-6 flex justify-center py-4">
        <button type="submit" class="action-btn font-bold px-14 py-4 rounded-full text-white text-xs uppercase tracking-widest shadow-xl transition-all">
          Apply & Save Configurations
        </button>
      </div>
    </form>
  </div>
</template>

<script setup>
import { ref, onMounted, reactive } from 'vue';
import { useSettingsStore } from '../stores/settings';
import { useStatsStore } from '../stores/stats';

const store = useSettingsStore();
const statsStore = useStatsStore();
const pathErrors = ref([]);

const form = reactive({
  server_type: 'plex',
  plex_server: '',
  plex_token: '',
  server_url: '',
  api_key: '',
  scan_directories: '',
  scan_debounce: 0,
  scan_workers: 0,
});

const save = async () => {
  const result = await store.saveSettings(form);
  if (result.status === 'success') alert('Settings Saved');
  else alert('Error: ' + result.error);
};

const testConn = async () => {
  const result = await store.testConnection(form);
  alert(result.status === 'success' ? 'Connected' : 'Failed: ' + result.message);
};

const checkPaths = async () => {
  const result = await store.verifyPaths(form.scan_directories);
  pathErrors.value = result.results.filter(r => !r.valid).map(r => r.path);
};

onMounted(async () => {
  if (!statsStore.stats.config) await statsStore.fetchStats();
  const c = statsStore.stats.config;
  if (c) {
    Object.assign(form, c);
  }
});
</script>
