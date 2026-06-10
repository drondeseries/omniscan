<template>
  <div class="glass-card p-6 rounded-2xl mt-6">
    <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">Recent Imports</h4>
    <div class="space-y-3">
      <div v-for="imp in recentImports" :key="imp.timestamp" class="flex justify-between items-center text-xs">
        <span class="text-white truncate max-w-sm" :title="imp.details">{{ imp.details }}</span>
        <span class="text-[9px] text-slate-500 font-mono">{{ imp.timestamp }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
defineProps({
  recentImports: Array
});
</script>
