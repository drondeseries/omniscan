<template>
  <div class="h-[calc(100vh-170px)] flex flex-col">
    <div class="glass-card rounded-2xl flex-grow overflow-hidden flex flex-col border border-white/5 bg-[#070a13]/90">
      <div class="p-4 border-b border-white/5 flex justify-between items-center bg-[#07090f]/95">
        <div class="flex items-center gap-2">
          <span class="w-2 h-2 bg-emerald-500 rounded-full pulse-indicator"></span>
          <span class="text-xs font-bold font-mono text-slate-400 uppercase tracking-widest">Live Output Stream</span>
        </div>
        <button @click="store.clearLogs" class="text-[9px] uppercase font-bold text-slate-400 hover:text-white transition-colors tracking-widest">
          Clear Screen
        </button>
      </div>
      <div ref="terminal" class="flex-grow overflow-y-auto p-4.5 font-mono text-[11px] space-y-1 text-slate-300 scroll-smooth leading-relaxed">
        <div v-for="(log, index) in store.logs" :key="index" :class="log.color" class="whitespace-pre-wrap break-all font-mono text-[10px]">
          {{ log.msg }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { onMounted, ref, watch, nextTick } from 'vue';
import { useLogsStore } from '../stores/logs';

const store = useLogsStore();
const terminal = ref(null);

watch(() => store.logs, async () => {
  await nextTick();
  if (terminal.value) {
    terminal.value.scrollTop = terminal.value.scrollHeight;
  }
}, { deep: true });

onMounted(() => {
  store.connectLogs();
});
</script>
