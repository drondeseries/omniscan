<template>
  <div class="p-10 text-white max-w-md mx-auto">
    <h1 class="text-2xl font-bold mb-4">Login</h1>
    <form @submit.prevent="login" class="space-y-4">
      <input type="password" v-model="password" placeholder="Password" class="interactive-input w-full p-3 text-xs" required />
      <button type="submit" class="action-btn w-full p-3 rounded-xl text-white text-xs font-bold uppercase">Login</button>
    </form>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { useRouter } from 'vue-router';

const password = ref('');
const router = useRouter();

const login = async () => {
  const res = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ password: password.value })
  });
  if (res.ok) {
    router.push('/');
  } else {
    alert('Invalid password');
  }
};
</script>
