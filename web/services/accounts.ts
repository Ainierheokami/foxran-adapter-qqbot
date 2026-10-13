import { host } from '../host'

// Served by this adapter's own routes (/api/adapters/qqbot/...).
const base = '/adapters/qqbot'

export async function loadAccounts() {
  return (await host.api.get(`${base}/accounts`)).data.accounts
}

export async function saveAccounts(accounts: unknown[]) {
  return (await host.api.put(`${base}/accounts`, { accounts })).data.accounts
}

export async function reloadAccounts() {
  return (await host.api.post(`${base}/reload`)).data.accounts
}
