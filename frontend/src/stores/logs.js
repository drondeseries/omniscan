import { defineStore } from 'pinia';

export const useLogsStore = defineStore('logs', {
  state: () => ({
    logs: [],
    ws: null,
    pollingInterval: null
  }),
  actions: {
    connectLogs() {
      if (this.pollingInterval) return;
      if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) return;
      
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const url = `${proto}://${window.location.host}/ws/logs`;
      
      this.addLog(`>> Connecting to Log stream at ${url}...`, 'text-slate-500');
      
      this.ws = new WebSocket(url);
      
      this.ws.onopen = () => this.addLog(">> Connection Established", "text-cyan-400");
      this.ws.onmessage = (event) => {
        let color = "text-slate-300";
        if (event.data.includes("ERROR")) color = "text-red-400 font-bold";
        else if (event.data.includes("WARNING")) color = "text-orange-400";
        else if (event.data.includes("INFO")) color = "text-cyan-300";
        else if (event.data.includes("DEBUG")) color = "text-slate-500";
        this.addLog(event.data, color);
      };
      
      this.ws.onerror = (e) => this.addLog(">> Connection Error. Switching to polling backup...", "text-red-500");
      
      this.ws.onclose = (e) => {
        if (e.code === 1006 || e.code === 1000) {
          this.ws = null;
          this.startPolling();
        } else {
          this.addLog(`>> Connection Closed (Code: ${e.code}). Reconnecting in 5s...`, "text-orange-500");
          this.ws = null;
          setTimeout(() => this.connectLogs(), 5000);
        }
      };
    },
    async startPolling() {
      if (this.pollingInterval) return;
      this.addLog(">> Polling mode activated successfully", "text-cyan-400 font-bold");
      const poll = async () => {
        try {
          const res = await fetch('/api/logs');
          const logs = await res.json();
          this.logs = []; // Clear and replace
          logs.forEach(l => {
            let color = "text-slate-300";
            if (l.includes("ERROR")) color = "text-red-400 font-bold";
            else if (l.includes("WARNING")) color = "text-orange-400";
            else if (l.includes("INFO")) color = "text-cyan-300";
            else if (l.includes("DEBUG")) color = "text-slate-500";
            this.addLog(l, color);
          });
        } catch (e) {}
      };
      poll();
      this.pollingInterval = setInterval(poll, 2000);
    },
    addLog(msg, color) {
      const stripped = msg.replace(/[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g, '');
      this.logs.push({ msg: stripped, color });
      if (this.logs.length > 1000) this.logs.shift();
    },
    clearLogs() {
      this.logs = [];
    }
  }
});
