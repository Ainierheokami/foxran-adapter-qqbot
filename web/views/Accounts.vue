<template>
  <AdapterSettingsScaffold
    title="QQ Bot OpenAPI"
    description="管理 QQ 开放平台应用、Gateway 订阅与 Webhook。每个账户独立鉴权，并使用自己的 Bot OpenID 判断全量群消息中的艾特。"
    :icon="MessageCircleMore"
    :accounts="accounts"
    :selected="selected"
    :busy="busy"
    :dirty="dirty"
    :error="error"
    :message="message"
    status-hint="Gateway 状态会自动刷新"
    @select="selected = $event"
    @add="addAccount"
    @duplicate="duplicateAccount"
    @remove="removeAccount"
    @save="save"
    @reload="reload"
  >
    <template #account-meta="{ account }">{{ account.app_id || account.id }} · {{ transportLabel(account.transport) }}</template>
    <template #default="{ account }">
      <div class="qq:space-y-5">
        <AdapterFormSection index="01" title="应用身份" description="对应 QQ 开放平台应用，并作为账户与会话的唯一标识。" :icon="Fingerprint">
          <label><span>显示名称</span><input v-model="account.name" placeholder="主机器人" @input="markDirty" /></label>
          <label><span>账户 ID</span><input v-model="account.id" placeholder="main" @input="markDirty" /><small>用于区分连接、Webhook 和会话。</small></label>
          <label><span>AppID</span><input v-model="account.app_id" autocomplete="off" @input="markDirty" /></label>
          <label class="setting-switch"><span><b>启用账户</b><small>保存后启动所选接入方式。</small></span><input v-model="account.enabled" type="checkbox" @change="markDirty" /></label>
          <label class="qq:md:col-span-2"><span>AppSecret</span><div class="secret-field"><input v-model="account.app_secret" :type="showSecret ? 'text' : 'password'" autocomplete="new-password" :placeholder="account.app_secret_set ? '已设置，留空保持不变' : '请输入 AppSecret'" @input="markDirty" /><button type="button" :title="showSecret ? '隐藏' : '显示'" @click="showSecret = !showSecret"><EyeOff v-if="showSecret" class="qq:h-4 qq:w-4" /><Eye v-else class="qq:h-4 qq:w-4" /></button></div></label>
        </AdapterFormSection>

        <AdapterFormSection index="02" title="接入与回调" description="选择 Gateway 或 Webhook，并配置全量群消息的机器人身份。" :icon="RadioTower">
          <label class="qq:md:col-span-2"><span>接入方式</span><select v-model="account.transport" @change="markDirty"><option value="websocket">WebSocket Gateway</option><option value="webhook">Webhook</option><option value="both">WebSocket + Webhook</option></select></label>
          <label class="qq:md:col-span-2"><span>Bot OpenID</span><input v-model="account.bot_openid" placeholder="群消息正文 <@…> 中的 OpenID" @input="markDirty" /><small>GROUP_MESSAGE_CREATE 不会单独标记艾特，适配器使用此值匹配正文中的 &lt;@OpenID&gt;。</small></label>
          <div v-if="account.transport !== 'websocket'" class="qq:md:col-span-2 qq:rounded-lg qq:border qq:bg-muted/20 qq:p-4">
            <div class="qq:text-xs qq:font-medium">Webhook 回调路径</div>
            <div class="qq:mt-2 qq:flex qq:items-center qq:gap-2 qq:rounded-md qq:border qq:bg-background qq:px-3 qq:py-2"><code class="qq:min-w-0 qq:flex-1 qq:truncate qq:text-xs">{{ account.webhook_url || `/qqbot/webhook/${account.id}` }}</code><button class="qq:text-muted-foreground qq:transition-colors qq:hover:text-foreground" @click="copyText(account.webhook_url || `/qqbot/webhook/${account.id}`)"><Copy class="qq:h-4 qq:w-4" /></button></div>
            <p class="qq:mt-2 qq:text-xs qq:text-muted-foreground">开放平台需要配置可公网访问的完整 HTTPS 地址。</p>
          </div>
        </AdapterFormSection>

        <AdapterFormSection index="03" title="事件与会话" description="控制订阅事件范围、Gateway 分片和群聊上下文策略。" :icon="BellRing">
          <label class="qq:md:col-span-2"><span>Intents 位掩码</span><input v-model.number="account.intents" type="number" @input="markDirty" /><small>1107296256 = GROUP_AND_C2C_EVENT (1&lt;&lt;25) + PUBLIC_GUILD_MESSAGES (1&lt;&lt;30)。请确保开放平台已授权对应事件。</small></label>
          <label class="setting-switch qq:md:col-span-2"><span><b>Markdown 回复</b><small>自动识别标题、列表、引用、代码和链接；QQ 拒绝时降级为纯文本。</small></span><input v-model="account.markdown_enabled" type="checkbox" @change="markDirty" /></label>
          <label class="setting-switch qq:md:col-span-2"><span><b>私聊流式回复</b><small>私聊里边生成边显示（QQ 流式消息）；群聊没有流式接口，仍整条发送。需开启 Markdown 回复。</small></span><input v-model="account.c2c_stream" type="checkbox" :disabled="!account.markdown_enabled" @change="markDirty" /></label>
          <label v-if="account.c2c_stream"><span>流式刷新间隔（毫秒）</span><input v-model.number="account.c2c_stream_interval_ms" type="number" min="300" step="100" @input="markDirty" /><small>最低 300；越小越流畅，但更容易被 QQ 限频。</small></label>
          <label v-if="account.c2c_stream" class="setting-switch"><span><b>整段回复逐步显示</b><small>等工具或任务完成后才发的回复，发送前已经写完，无法边生成边显示；开启后分几帧显示（约多 2~3 秒）。</small></span><input v-model="account.c2c_stream_reveal" type="checkbox" @change="markDirty" /></label>
          <label><span>Gateway 分片数</span><input v-model.number="account.shards" type="number" min="1" @input="markDirty" /></label>
          <label class="setting-switch"><span><b>群聊共享会话</b><small>同群成员共享上下文。</small></span><input v-model="account.use_group_as_session" type="checkbox" @change="markDirty" /></label>
        </AdapterFormSection>

        <details class="qq:rounded-xl qq:border qq:bg-muted/10">
          <summary class="qq:cursor-pointer qq:px-4 qq:py-3 qq:text-sm qq:font-medium">高级 API 与重连参数</summary>
          <div class="qq:grid qq:grid-cols-1 qq:gap-4 qq:border-t qq:p-4 qq:sm:grid-cols-2">
            <label class="qq:sm:col-span-2"><span>API Base URL</span><input v-model="account.api_base_url" @input="markDirty" /></label>
            <label class="qq:sm:col-span-2"><span>Token URL</span><input v-model="account.token_url" @input="markDirty" /></label>
            <label><span>初始重连（秒）</span><input v-model.number="account.reconnect_initial_delay" type="number" min="0.5" @input="markDirty" /></label>
            <label><span>最大重连（秒）</span><input v-model.number="account.reconnect_max_delay" type="number" min="1" @input="markDirty" /></label>
          </div>
        </details>
      </div>
    </template>
  </AdapterSettingsScaffold>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { BellRing, Copy, Eye, EyeOff, Fingerprint, MessageCircleMore, RadioTower } from 'lucide-vue-next'
import { AdapterFormSection, AdapterSettingsScaffold } from '../host'
import { loadAccounts, reloadAccounts, saveAccounts } from '../services/accounts'

type Account = Record<string, any>
const accounts = ref<Account[]>([])
const selected = ref(0)
const busy = ref(false)
const dirty = ref(false)
const showSecret = ref(false)
const error = ref('')
const message = ref('')
let timer: number | undefined
const current = computed(() => accounts.value[selected.value])
const defaults = (): Account => ({ id: `qqbot-${Date.now().toString(36)}`, name: '新 QQBot', enabled: false, app_id: '', app_secret: '', bot_openid: '', transport: 'websocket', api_base_url: 'https://api.bot.qq.com', token_url: 'https://bots.qq.com/app/getAppAccessToken', intents: 1107296256, shards: 1, use_group_as_session: true, markdown_enabled: true, c2c_stream: true, c2c_stream_interval_ms: 600, c2c_stream_reveal: true, reconnect_initial_delay: 2, reconnect_max_delay: 30 })
function markDirty() { dirty.value = true; message.value = '' }
function transportLabel(value = '') { return ({ websocket: 'WebSocket', webhook: 'Webhook', both: '双接入' } as Record<string,string>)[value] || value }
async function fetchAccounts(silent = false) { if (!silent) busy.value = true; try { const fresh = await loadAccounts(); if (!dirty.value || !silent) accounts.value = fresh; error.value = '' } catch (e: any) { error.value = e?.response?.data?.detail || '无法读取 QQBot 配置，请确认适配器已安装并启用。' } finally { busy.value = false } }
function addAccount() { accounts.value.push(defaults()); selected.value = accounts.value.length - 1; markDirty() }
function duplicateAccount() { if (!current.value) return; const copy = structuredClone(current.value); copy.id = `${copy.id}-copy`; copy.name = `${copy.name} 副本`; copy.app_secret = ''; copy.app_secret_set = false; delete copy.status; accounts.value.push(copy); selected.value = accounts.value.length - 1; markDirty() }
function removeAccount() { if (accounts.value.length <= 1 || !confirm(`删除账户“${current.value.name || current.value.id}”？`)) return; accounts.value.splice(selected.value, 1); selected.value = Math.max(0, selected.value - 1); markDirty() }
async function save() { busy.value = true; try { accounts.value = await saveAccounts(accounts.value); selected.value = Math.min(selected.value, accounts.value.length - 1); dirty.value = false; message.value = '配置已保存，连接已热重载。'; error.value = '' } catch (e: any) { error.value = e?.response?.data?.detail || '保存失败' } finally { busy.value = false } }
async function reload() { busy.value = true; try { accounts.value = await reloadAccounts(); dirty.value = false; message.value = '连接已重载。'; error.value = '' } catch (e: any) { error.value = e?.response?.data?.detail || '重载失败' } finally { busy.value = false } }
async function copyText(text: string) { await navigator.clipboard.writeText(text); message.value = '地址已复制。' }
onMounted(async () => { await fetchAccounts(); timer = window.setInterval(() => fetchAccounts(true), 5000) })
onBeforeUnmount(() => window.clearInterval(timer))
</script>
