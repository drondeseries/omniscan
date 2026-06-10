import { defineStore } from 'pinia';

export const useSettingsStore = defineStore('settings', {
  state: () => ({
    config: {},
    loading: false
  }),
  actions: {
    async saveSettings(settings) {
      this.loading = true;
      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(settings)
        });
        const data = await res.json();
        return data;
      } catch (e) {
        return { status: 'error', error: e.message };
      } finally {
        this.loading = false;
      }
    },
    async testConnection(settings) {
      try {
        const res = await fetch('/api/test-connection', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(settings)
        });
        return await res.json();
      } catch (e) {
        return { status: 'error', message: e.message };
      }
    },
    async testDiscord(webhookUrl) {
      try {
        const res = await fetch('/api/test-webhook', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl })
        });
        return res.ok;
      } catch (e) {
        return false;
      }
    },
    async verifyPaths(paths) {
      try {
        const res = await fetch('/api/validate-paths', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ paths })
        });
        return await res.json();
      } catch (e) {
        return { results: [] };
      }
    }
  }
});
