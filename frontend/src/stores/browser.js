import { defineStore } from 'pinia';

export const useBrowserStore = defineStore('browser', {
  state: () => ({
    path: "Root",
    items: [],
    loading: false
  }),
  actions: {
    async loadBrowser(path = "", query = "") {
      this.loading = true;
      try {
        let url = `/api/browser/list?path=${encodeURIComponent(path)}`;
        if (query) url += `&query=${encodeURIComponent(query)}`;
        const res = await fetch(url);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        this.path = data.current_path || "Root";
        this.items = data.items || [];
      } catch (e) {
        console.error("Browser lookup failed:", e);
      } finally {
        this.loading = false;
      }
    },
    async scanPath(path) {
      try {
        await fetch('/api/browser/action', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'scan', path })
        });
      } catch (e) {
        console.error("Scan path failed:", e);
      }
    }
  }
});
