<template>
  <header class="h-16 lg:h-20 border-b border-white/5 flex items-center justify-between px-6 lg:px-10 bg-[#030712]/50 backdrop-blur-md sticky top-0 z-20">
    <h2 class="font-extrabold text-xl lg:text-2xl tracking-tight text-white">{{ title }}</h2>
    <div class="flex items-center gap-5">
      <button onclick="triggerFullScan()" class="hidden md:flex items-center gap-2 bg-[#06b6d4]/10 hover:bg-[#06b6d4] text-[#06b6d4] hover:text-white px-4.5 py-2 rounded-xl text-xs font-semibold border border-[#06b6d4]/20 transition-all">
        <i class="fas fa-bolt"></i> Run Full Scan
      </button>
      <div class="flex flex-col items-end">
        <span class="text-[8px] text-slate-500 font-bold uppercase tracking-widest">System Uptime</span>
        <span class="text-sm font-semibold font-mono text-cyan-400">{{ uptime }}</span>
      </div>
    </div>
  </header>
</template>

<script setup>
defineProps({
  title: String,
  uptime: String
});
</script>
