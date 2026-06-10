import { defineStore } from 'pinia';

export const useStatsStore = defineStore('stats', {
  state: () => ({
    stats: {},
    loading: false
  }),
  actions: {
    async fetchStats() {
      this.loading = true;
      try {
        const res = await fetch('/api/stats');
        if (res.ok) {
          this.stats = await res.json();
        }
      } catch (e) {
        console.error("Failed to fetch stats:", e);
      } finally {
        this.loading = false;
      }
    }
  }
});
