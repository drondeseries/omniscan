<template>
  <aside class="w-64 flex-shrink-0 flex flex-col bg-[#090d16]/80 border-r border-white/5 z-10 backdrop-blur-xl">
    <!-- Brand/Logo -->
    <div class="p-6 flex items-center gap-3">
      <img src="/assets/logo.png" class="w-10 h-10 rounded-xl shadow-cyan-500/20 shadow-lg object-cover" alt="Omniscan Logo">
      <div>
        <h1 class="font-extrabold text-lg tracking-tight bg-gradient-to-r from-white via-slate-100 to-slate-400 bg-clip-text text-transparent">Omniscan</h1>
        <div class="flex items-center gap-1.5 mt-0.5">
          <span class="w-2 h-2 bg-emerald-500 rounded-full pulse-indicator"></span>
          <span class="text-[9px] text-slate-400 font-bold uppercase tracking-wider">System Active</span>
        </div>
      </div>
    </div>

    <!-- Menu -->
    <nav class="mt-6 px-3 flex-grow space-y-1">
      <RouterLink v-for="item in menuItems" :key="item.id" 
                  :to="{ name: item.id }"
                  active-class="active"
                  class="nav-item w-full flex items-center gap-3.5 px-5 py-3.5 text-sm font-semibold rounded-lg">
        <i :class="['fas', item.icon, 'w-5 text-center text-base']"></i> {{ item.label }}
      </RouterLink>
    </nav>
</aside>
</template>

<script setup>
import { RouterLink } from 'vue-router';

const menuItems = [
  { id: 'dashboard', label: 'Dashboard', icon: 'fa-chart-pie' },
  { id: 'history', label: 'Event History', icon: 'fa-list-ul' },
  { id: 'logs', label: 'Live Console', icon: 'fa-terminal' },
  { id: 'browser', label: 'File Browser', icon: 'fa-folder-open' },
  { id: 'settings', label: 'Config Settings', icon: 'fa-sliders' }
];
</script>

<style scoped>
.nav-item {
  color: #94a3b8;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}
.nav-item.active {
  color: #ffffff;
  background: rgba(6, 182, 212, 0.1);
  border-left: 3px solid #06b6d4;
}
.nav-item:hover:not(.active) {
  background: rgba(255, 255, 255, 0.02);
  color: #f1f5f9;
}
.pulse-indicator {
  box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
  animation: pulse-ring 1.8s infinite;
}
@keyframes pulse-ring {
  0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.6); }
  70% { box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
  100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
}
</style>
