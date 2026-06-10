<template>
  <div class="glass-card p-6 rounded-2xl flex flex-col items-center justify-center transition-all duration-300 hover:border-cyan-500/30">
    <p class="text-slate-400 text-[10px] font-bold uppercase tracking-widest mb-2">{{ label }}</p>
    <h3 :class="['text-4xl font-extrabold leading-none', color || 'text-white']">{{ value }}</h3>
  </div>
</template>

<script setup>
defineProps({
  label: String,
  value: [String, Number],
  icon: String,
  color: String
});
</script>
