import { defineStore } from 'pinia';

export const useHistoryStore = defineStore('history', {
  state: () => ({
    events: [],
    offset: 0,
    more: true,
    loading: false
  }),
  actions: {
    async loadHistory(search = "", append = false) {
      if (this.loading || (!append && !this.more && search === "")) return;
      if (!append) {
        this.offset = 0;
        this.more = true;
        this.events = [];
      }
      this.loading = true;
      try {
        const res = await fetch(`/api/history?offset=${this.offset}&search=${encodeURIComponent(search.trim())}`);
        const data = await res.json();
        if (data.length < 50) this.more = false;
        if (append) {
          this.events.push(...data);
        } else {
          this.events = data;
        }
        this.offset += data.length;
      } catch (e) {
        console.error("Failed to fetch history:", e);
      } finally {
        this.loading = false;
      }
    },
    async clearHistory() {
      try {
        await fetch('/api/history/clear', { method: 'POST' });
        this.loadHistory("", false);
      } catch (e) {
        console.error("Failed to clear history:", e);
      }
    }
  }
});
