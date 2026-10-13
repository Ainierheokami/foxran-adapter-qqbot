import './style.css'
import { MessageCircleMore } from 'lucide-vue-next'
import Accounts from './views/Accounts.vue'
import { host } from './host'

host.registerPlugin({
  id: 'qqbot',
  settingsPanels: [
    {
      id: 'qqbot',
      title: 'QQ Bot OpenAPI',
      icon: MessageCircleMore,
      group: '适配器',
      files: ['qqbot.yml', 'qqbot.yaml'],
      component: Accounts,
    },
  ],
})
