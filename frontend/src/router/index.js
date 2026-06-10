import { createRouter, createWebHistory } from 'vue-router'
import DashboardStats from '../components/DashboardStats.vue'
import LoginView from '../views/LoginView.vue'
import SetupView from '../views/SetupView.vue'

const routes = [
  { path: '/', name: 'dashboard', component: DashboardStats },
  { path: '/history', name: 'history', component: () => import('../views/HistoryView.vue') },
  { path: '/logs', name: 'logs', component: () => import('../views/LogsView.vue') },
  { path: '/browser', name: 'browser', component: () => import('../views/BrowserView.vue') },
  { path: '/settings', name: 'settings', component: () => import('../views/SettingsView.vue') },
  { path: '/login', name: 'login', component: LoginView },
  { path: '/setup', name: 'setup', component: SetupView }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

// Simple navigation guard
router.beforeEach(async (to, from, next) => {
  if (to.name === 'login' || to.name === 'setup') {
    next();
    return;
  }
  
  try {
    const res = await fetch('/api/stats');
    if (res.status === 401) {
        // Check if it's "Not authenticated" or "Setup required"
        const data = await res.json();
        if (data.detail === "Setup required") {
            next({ name: 'setup' });
        } else {
            next({ name: 'login' });
        }
    } else {
      next();
    }
  } catch (e) {
    next({ name: 'login' });
  }
})

export default router
