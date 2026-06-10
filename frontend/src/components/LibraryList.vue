<template>
  <div class="glass-card p-6 rounded-2xl mt-6">
    <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider mb-4">Media Libraries</h4>
    <div class="space-y-3">
      <div v-for="lib in libraries" :key="lib.id" class="flex justify-between items-center text-xs">
        <div class="flex flex-col">
          <span class="text-white font-bold">{{ lib.title }}</span>
          <span class="text-[9px] text-slate-500 uppercase">{{ lib.type }}</span>
        </div>
        <div class="flex items-center gap-3">
          <span v-if="lib.missing > 0" class="text-yellow-400 font-bold" title="Missing files">{{ lib.missing }} missing</span>
          <span class="text-cyan-400 font-mono font-bold">{{ lib.count.toLocaleString() }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
const props = defineProps({
  libraries: Array
});
console.log("LibraryList props:", props.libraries);
</script>
