'use strict'

const https = require('https')
const http  = require('http')

const PLUGIN_UID = 'com.qua121.comment-translator'
const DEEPL_FREE_API_HOST = 'api-free.deepl.com'
const DEEPL_PRO_API_HOST  = 'api.deepl.com'
const DEEPL_API_PATH      = '/v2/translate'
const OLLAMA_HOST = 'localhost'
const OLLAMA_PORT = 11434
const OLLAMA_PATH = '/api/chat'
const MAX_TRANSLATIONS = 50
const MAX_ERRORS = 20
const MAX_DEBUG = 100
const QUEUE_CONCURRENCY = 2
const REQUEST_TIMEOUT_MS = 30000
const OLLAMA_TIMEOUT_MS       = 300000   // 通常翻訳: 5分
const OLLAMA_FIRST_TIMEOUT_MS = 3600000  // 初回モデルロード: 1時間

const LANG_NAME_FOR_PROMPT = {
  'JA':    'Japanese',
  'EN-US': 'English',
  'EN-GB': 'English',
  'ZH':    'Chinese',
  'KO':    'Korean',
  'FR':    'French',
  'DE':    'German',
  'ES':    'Spanish',
}

const SOURCE_LANG_FOR_PROMPT = {
  'JA':    { name: 'Japanese', code: 'JA' },
  'KO':    { name: 'Korean',   code: 'KO' },
  'ZH':    { name: 'Chinese',  code: 'ZH' },
  'OTHER': { name: 'English',  code: 'EN' }, // OTHERは英語として扱う（最頻ケース）
}

const LANG_PATTERNS = {
  JA: /[\u3040-\u309F\u30A0-\u30FF]/g,
  KO: /[\uAC00-\uD7AF\u1100-\u11FF\u3130-\u318F]/g,
  ZH: /[\u4E00-\u9FFF\u3400-\u4DBF\u{20000}-\u{2A6DF}]/gu,
}

/** 言語判定に必要な最低スクリプト文字数。1文字の混入による誤判定を防ぐ */
const SCRIPT_MIN_COUNT = 2
/** 全文字中の該当スクリプト占有率の閾値。英語主体コメントの誤判定を抑制 */
const SCRIPT_RATIO_THRESHOLD = 0.25

function detectLang(text) {
  const letters = text.replace(/[\s\p{P}\p{S}\p{N}]/gu, '').length
  // shouldSkipを通過しても絵文字のみ等でlettersが0になるケースへの防御
  if (letters === 0) return 'OTHER'

  const jaCount = (text.match(LANG_PATTERNS.JA) || []).length
  if (jaCount >= SCRIPT_MIN_COUNT && jaCount / letters >= SCRIPT_RATIO_THRESHOLD) return 'JA'

  const koCount = (text.match(LANG_PATTERNS.KO) || []).length
  if (koCount >= SCRIPT_MIN_COUNT && koCount / letters >= SCRIPT_RATIO_THRESHOLD) return 'KO'

  const zhCount = (text.match(LANG_PATTERNS.ZH) || []).length
  if (zhCount >= SCRIPT_MIN_COUNT && zhCount / letters >= SCRIPT_RATIO_THRESHOLD) return 'ZH'

  return 'OTHER'
}

function shouldSkip(text) {
  const stripped = text
    .replace(/https?:\/\/\S+/g, '')
    .replace(/[\u{1F000}-\u{1FFFF}]/gu, '')
    .replace(/[\u2600-\u27BF]/g, '')
    .replace(/@\S+/g, '')
    .replace(/#\S+/g, '')
    .trim()
  if (stripped.length <= 1) return true
  if (/^[0-9\s\p{P}\p{S}]+$/u.test(stripped)) return true
  return false
}

class AsyncQueue {
  constructor(concurrency) {
    this.concurrency = concurrency
    this.running = 0
    this.queue = []
  }

  add(task) {
    return new Promise((resolve, reject) => {
      this.queue.push({ task, resolve, reject })
      this._run()
    })
  }

  clear() {
    const pending = this.queue.splice(0)
    for (const { reject } of pending) {
      reject({ code: 'CANCELLED', message: 'Queue cleared' })
    }
  }

  _run() {
    while (this.running < this.concurrency && this.queue.length > 0) {
      const { task, resolve, reject } = this.queue.shift()
      this.running++
      task()
        .then(resolve)
        .catch(reject)
        .finally(() => {
          this.running--
          this._run()
        })
    }
  }
}

const CACHE_MAX_SIZE = 100

class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize
    this.cache = new Map()
  }

  get(key) {
    if (!this.cache.has(key)) return undefined
    const value = this.cache.get(key)
    this.cache.delete(key)
    this.cache.set(key, value)
    return value
  }

  set(key, value) {
    if (this.cache.has(key)) this.cache.delete(key)
    this.cache.set(key, value)
    if (this.cache.size > this.maxSize) {
      const oldest = this.cache.keys().next().value
      this.cache.delete(oldest)
    }
  }

  clear() {
    this.cache.clear()
  }
}

function isDeepLFreeKey(apiKey) {
  return apiKey.endsWith(':fx')
}

function callDeepLAPI(text, apiKey, targetLang = 'JA') {
  return new Promise((resolve, reject) => {
    const body = new URLSearchParams({ text, target_lang: targetLang }).toString()
    const hostname = isDeepLFreeKey(apiKey) ? DEEPL_FREE_API_HOST : DEEPL_PRO_API_HOST

    const options = {
      hostname,
      path: DEEPL_API_PATH,
      method: 'POST',
      headers: {
        'Authorization': `DeepL-Auth-Key ${apiKey}`,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      },
    }

    const req = https.request(options, (res) => {
      let data = ''
      res.on('data', (chunk) => { data += chunk })
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            const json = JSON.parse(data)
            resolve(json.translations[0].text)
          } catch (e) {
            reject({ code: 'PARSE_ERROR', message: e.message })
          }
        } else {
          reject({ code: res.statusCode, message: data })
        }
      })
    })

    req.on('error', (e) => reject({ code: 'NETWORK_ERROR', message: e.message }))

    req.setTimeout(REQUEST_TIMEOUT_MS, () => {
      req.destroy()
      reject({ code: 'TIMEOUT', message: 'Request timed out' })
    })

    req.write(body)
    req.end()
  })
}

function cleanLLMOutput(text) {
  const cleaned = text
    // LLM制御トークン除去（Qwen, Gemma, Llama等）
    .replace(/<\|[^|]*\|>/g, '')
    .replace(/<\/?(?:think|im_start|im_end|endoftext|pad|s|\/s)\b[^>]*>/gi, '')
    // thinkingブロックの中身ごと除去
    .replace(/<think>[\s\S]*?<\/think>/gi, '')
    // チャットロールラベル以降を切り捨て（user/assistant/systemの再出現）
    .replace(/\b(?:user|assistant|system)\s+[\s\S]*$/i, '')
    // 注釈・補足説明を除去（「（注：...）」「(Note: ...)」「※...」等）
    .replace(/\s*[（(]\s*(?:注|Note|注釈|補足|Note:|注：)[\s\S]*$/i, '')
    // 「Wait,」「Okay,」「Let me」等の思考フレーズ以降を切り捨て
    .replace(/\s*(?:Wait,|Okay,|Let me|Actually,|I'll|I think|Looking at|So I)[\s\S]*$/i, '')
    // ラベルプレフィックス除去
    .replace(/^(?:translation|翻訳|訳|output|result|here is|here's)[^:：]*[:：]\s*/i, '')
    // 引用符除去
    .replace(/^["'`「」『』]+|["'`「」『』]+$/g, '')
    .trim()
  // 後処理で空になった場合は元テキストを返す（ラベルだけ返す異常応答への対処）
  return cleaned || text.trim()
}

function callOllamaAPI(text, model, targetLang, sourceLang = 'OTHER', timeoutMs = OLLAMA_TIMEOUT_MS) {
  const targetName = LANG_NAME_FOR_PROMPT[targetLang] || targetLang
  const targetCode = targetLang.split('-')[0] // 'EN-US' → 'EN'

  const modelLower = model.toLowerCase()
  const isTranslateGemma = modelLower.startsWith('translategemma') && sourceLang !== 'OTHER'
  const isQwen = modelLower.startsWith('qwen')
  let systemPrompt
  if (isTranslateGemma) {
    const src = SOURCE_LANG_FOR_PROMPT[sourceLang]
    systemPrompt =
      `You are a professional ${src.name} (${src.code}) to ${targetName} (${targetCode}) translator.`
  } else {
    systemPrompt =
      `You are a translator. Translate the user's message to ${targetName}. ` +
      'Output ONLY the translated text. No explanations, no notes, no labels, no quotation marks, no parenthetical comments.'
  }
  // Qwen3系のthinking（推論）モードを無効化
  if (isQwen) {
    systemPrompt = '/no_think\n' + systemPrompt
  }

  const body = JSON.stringify({
    model,
    stream: false,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user',   content: text },
    ],
  })

  return new Promise((resolve, reject) => {
    const options = {
      hostname: OLLAMA_HOST,
      port: OLLAMA_PORT,
      path: OLLAMA_PATH,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }

    const req = http.request(options, (res) => {
      let data = ''
      res.on('data', (chunk) => { data += chunk })
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            const json = JSON.parse(data)
            const raw = json.message?.content
            if (!raw) return reject({ code: 'OLLAMA_EMPTY', message: 'Empty response from Ollama' })
            resolve(cleanLLMOutput(raw))
          } catch (e) {
            reject({ code: 'PARSE_ERROR', message: e.message })
          }
        } else if (res.statusCode === 404) {
          reject({ code: 'OLLAMA_MODEL_NOT_FOUND', message: data })
        } else {
          reject({ code: 'OLLAMA_ERROR', message: `HTTP ${res.statusCode}: ${data}` })
        }
      })
    })

    req.on('error', (e) => reject({ code: 'OLLAMA_CONNECT_ERROR', message: e.message }))

    req.setTimeout(timeoutMs, () => {
      req.destroy()
      reject({ code: 'TIMEOUT', message: 'Request timed out' })
    })

    req.write(body)
    req.end()
  })
}

const ERROR_CATALOG = {
  API_KEY_EMPTY: {
    cause: 'APIキーが入力されていません',
    solution: '設定ページからDeepL APIキーを入力してください',
    link: 'https://www.deepl.com/account/summary',
  },
  403: {
    cause: 'APIキーが無効、またはキー種別とエンドポイントが一致しません',
    solution: 'Freeキーは末尾が :fx です。ProキーをFree APIで使用していないか確認してください',
    link: 'https://www.deepl.com/account/summary',
  },
  456: {
    cause: '今月の無料枠（50万文字）を超えました',
    solution: 'DeepL Proへのアップグレードを検討してください',
    link: 'https://www.deepl.com/pro',
  },
  429: {
    cause: 'リクエストが集中しています',
    solution: 'しばらく待つと自動回復します',
    link: null,
  },
  NETWORK_ERROR: {
    cause: 'DeepLサーバーに接続できません',
    solution: 'インターネット接続を確認してください',
    link: null,
  },
  TIMEOUT: {
    cause: '翻訳リクエストがタイムアウトしました',
    solution: '次のコメントから再開します',
    link: null,
  },
  PARSE_ERROR: {
    cause: 'APIレスポンスの解析に失敗しました',
    solution: 'デバッグログを確認してください',
    link: null,
  },
  OLLAMA_CONNECT_ERROR: {
    cause: 'Ollamaに接続できません',
    solution: 'Ollamaが起動しているか確認してください（ollama serve）',
    link: null,
  },
  OLLAMA_EMPTY: {
    cause: 'Ollamaからの応答が空でした',
    solution: 'モデルが正しく動作しているか確認してください',
    link: null,
  },
  OLLAMA_MODEL_NOT_FOUND: {
    cause: '指定したOllamaモデルが見つかりません',
    solution: '設定のモデル名を確認してください（例: translategemma:4b, qwen3.5:9b）',
    link: null,
  },
  OLLAMA_ERROR: {
    cause: 'Ollamaでエラーが発生しました',
    solution: 'デバッグログを確認してください',
    link: null,
  },
}

function categorizeError(code) {
  return ERROR_CATALOG[code] ?? {
    cause: `不明なエラー (${code})`,
    solution: 'デバッグログを確認してください',
    link: null,
  }
}

const plugin = {
  name: 'コメント翻訳',
  uid: PLUGIN_UID,
  version: '0.3.1',
  author: 'qua121',
  url: `http://localhost:11180/plugins/${PLUGIN_UID}/index.html`,
  permissions: ['filter.comment'],

  defaultState: {
    apiKey: '',
    engine: 'deepl',
    targetLang: 'JA',
    ollamaModel: 'translategemma:4b',
    translations: [],
    errors: [],
    debugLog: [],
  },

  _store: null,
  _queue: null,
  _ollamaQueue: null,
  _translationCache: null,
  _destroyed: false,
  _ollamaFirstRequest: true,
  _commentStructureLogged: false,
  _stateVersion: 0,

  init({ dir, store }, initialData) {
    this._store = store
    this._queue = new AsyncQueue(QUEUE_CONCURRENCY)
    this._ollamaQueue = new AsyncQueue(1)
    this._translationCache = new LRUCache(CACHE_MAX_SIZE)
    this._destroyed = false
    this._ollamaFirstRequest = true
    this._log('INFO', `plugin initialized (v${this.version})`)
    this._log('INFO', `plugin dir: ${dir}`)
    this._log('INFO', `engine: ${store.get('engine')} / targetLang: ${store.get('targetLang')}`)

    if (store.get('engine') !== 'ollama' && !store.get('apiKey')) {
      this._log('INFO', 'DeepL APIキーが未設定です。設定ページから入力してください。')
    }
  },

  destroy() {
    this._log('INFO', 'plugin destroyed')
    this._destroyed = true
    if (this._queue) this._queue.clear()
    if (this._ollamaQueue) this._ollamaQueue.clear()
    this._store = null
  },

  filterComment(comment, service, userData) {
    if (!this._commentStructureLogged) {
      this._log('DEBUG', `[structure check] ${JSON.stringify(comment)}`)
      this._commentStructureLogged = true
    }

    const text =
      comment?.data?.comment ??
      comment?.data?.text ??
      comment?.comment ??
      ''

    const name =
      comment?.data?.displayName ??
      comment?.data?.name ??
      comment?.name ??
      'unknown'

    const id =
      comment?.id ??
      comment?.data?.id ??
      `${Date.now()}-${Math.random().toString(36).slice(2)}`

    if (!text || text.trim() === '' || shouldSkip(text)) {
      return comment
    }

    const targetLang = (this._store?.get('targetLang') || 'JA').split('-')[0]
    const lang = detectLang(text)

    if (lang === targetLang || (lang === 'OTHER' && targetLang === 'EN')) {
      this._log('DEBUG', `skip (${lang}=target): "${text.slice(0, 40)}"`)
      return comment
    }
    this._log('INFO', `queued (${lang}): "${text.slice(0, 40)}"`)

    this._translateAsync({ id, name, lang, text }).catch((e) => {
      this._log('ERROR', `_translateAsync unexpected error: ${e?.message ?? e}`)
    })

    return comment
  },

  async _translateAsync({ id, name, lang, text }) {
    if (this._destroyed) return
    const store = this._store
    if (!store) return

    const engine = store.get('engine') || 'deepl'
    const targetLang = store.get('targetLang') || 'JA'

    if (engine === 'deepl') {
      const apiKey = store.get('apiKey')
      if (!apiKey) {
        this._log('DEBUG', `翻訳スキップ: APIキー未設定 (text="${text.slice(0, 20)}")`)
        return
      }
    }

    const cached = this._translationCache?.get(text)
    if (cached) {
      this._log('INFO', `cache hit: "${text.slice(0, 20)}"`)
      const entry = {
        id, name, lang, original: text, translated: cached,
        timestamp: new Date().toISOString(), status: 'ok',
      }
      const list = store.get('translations') || []
      list.unshift(entry)
      if (list.length > MAX_TRANSLATIONS) list.length = MAX_TRANSLATIONS
      store.set('translations', list)
      this._stateVersion++
      return
    }

    try {
      let translated
      if (engine === 'ollama') {
        const model = store.get('ollamaModel') || 'translategemma:4b'
        const isFirst = this._ollamaFirstRequest
        this._ollamaFirstRequest = false
        const timeout = isFirst ? OLLAMA_FIRST_TIMEOUT_MS : OLLAMA_TIMEOUT_MS
        if (isFirst) {
          this._log('INFO', `ollama first request (${model}), timeout=${timeout / 1000}s: "${text.slice(0, 20)}"`)
        } else {
          this._log('INFO', `ollama (${model}): "${text.slice(0, 20)}"`)
        }
        try {
          translated = await this._ollamaQueue.add(() =>
            callOllamaAPI(text, model, targetLang, lang, timeout)
          )
        } catch (retryErr) {
          if (retryErr?.code === 'OLLAMA_EMPTY') {
            this._log('INFO', `ollama empty response, retrying: "${text.slice(0, 20)}"`)
            translated = await this._ollamaQueue.add(() =>
              callOllamaAPI(text, model, targetLang, lang, timeout)
            )
          } else {
            throw retryErr
          }
        }
      } else {
        const apiKey = store.get('apiKey')
        translated = await this._queue.add(() =>
          callDeepLAPI(text, apiKey, targetLang)
        )
      }

      this._log('INFO', `ok: "${text.slice(0, 20)}" → "${translated.slice(0, 20)}"`)
      if (this._translationCache) this._translationCache.set(text, translated)

      const entry = {
        id,
        name,
        lang,
        original: text,
        translated,
        timestamp: new Date().toISOString(),
        status: 'ok',
      }

      const list = store.get('translations') || []
      list.unshift(entry)
      if (list.length > MAX_TRANSLATIONS) list.length = MAX_TRANSLATIONS
      store.set('translations', list)
      this._stateVersion++

    } catch (err) {
      if (err?.code === 'CANCELLED') return
      const code = err?.code ?? 'UNKNOWN'
      const info = categorizeError(code)
      this._pushError(code, `${info.cause} — "${text.slice(0, 20)}" [raw: ${err?.message ?? JSON.stringify(err)}]`)
    }
  },

  _log(level, message) {
    if (!this._store) {
      console.info(`[${level}] ${message}`)
      return
    }
    const entry = { level, message, timestamp: new Date().toISOString() }
    const list = this._store.get('debugLog') || []
    list.unshift(entry)
    if (list.length > MAX_DEBUG) list.length = MAX_DEBUG
    this._store.set('debugLog', list)
    console.info(`[${level}] ${message}`)
  },

  _pushError(code, message) {
    if (!this._store) return
    const info = categorizeError(code)
    const entry = {
      code,
      message,
      cause: info.cause,
      solution: info.solution,
      link: info.link,
      timestamp: new Date().toISOString(),
    }
    const list = this._store.get('errors') || []
    list.unshift(entry)
    if (list.length > MAX_ERRORS) list.length = MAX_ERRORS
    this._store.set('errors', list)
    this._stateVersion++
    this._log('ERROR', `[${code}] ${message}`)
  },

  async request(req) {
    const store = this._store
    if (!store) return { code: 500, response: { error: 'store not initialized' } }

    switch (req.method) {
      case 'GET': {
        const type = req.params?.type

        if (type === 'translations') {
          return { code: 200, response: { translations: store.get('translations') || [] } }
        }
        if (type === 'errors') {
          return { code: 200, response: { errors: store.get('errors') || [] } }
        }
        if (type === 'debug') {
          return { code: 200, response: { debugLog: store.get('debugLog') || [] } }
        }
        if (type === 'ollama_test') {
          return new Promise((resolve) => {
            const ollamaReq = http.get(
              { hostname: OLLAMA_HOST, port: OLLAMA_PORT, path: '/api/tags', timeout: 5000 },
              (res) => {
                let data = ''
                res.on('data', (chunk) => { data += chunk })
                res.on('end', () => {
                  if (res.statusCode === 200) {
                    try {
                      const json = JSON.parse(data)
                      resolve({ code: 200, response: { ok: true, models: json.models || [] } })
                    } catch (e) {
                      resolve({ code: 200, response: { ok: false, error: 'レスポンス解析エラー' } })
                    }
                  } else {
                    resolve({ code: 200, response: { ok: false, error: `HTTP ${res.statusCode}` } })
                  }
                })
              }
            )
            ollamaReq.on('error', (e) => {
              resolve({ code: 200, response: { ok: false, error: `接続失敗: ${e.message}` } })
            })
            ollamaReq.on('timeout', () => {
              ollamaReq.destroy()
              resolve({ code: 200, response: { ok: false, error: 'タイムアウト' } })
            })
          })
        }
        if (type === 'settings') {
          return {
            code: 200,
            response: {
              apiKeySet: !!store.get('apiKey'),
              engine: store.get('engine'),
              targetLang: store.get('targetLang'),
              ollamaModel: store.get('ollamaModel') || 'translategemma:4b',
            },
          }
        }

        const clientVersion = req.params?.version !== undefined
          ? parseInt(req.params.version, 10)
          : -1
        if (clientVersion === this._stateVersion) {
          return { code: 200, response: { changed: false, version: this._stateVersion } }
        }
        return {
          code: 200,
          response: {
            translations: store.get('translations') || [],
            errors: store.get('errors') || [],
            debugLog: store.get('debugLog') || [],
            version: this._stateVersion,
          },
        }
      }

      case 'POST': {
        const body = req.body || {}
        if (body.apiKey !== undefined) {
          store.set('apiKey', body.apiKey)
          this._commentStructureLogged = false
        }
        if (body.engine !== undefined && (body.engine === 'deepl' || body.engine === 'ollama')) {
          store.set('engine', body.engine)
          if (this._translationCache) this._translationCache.clear()
        }
        if (body.targetLang !== undefined) {
          store.set('targetLang', body.targetLang)
          if (this._translationCache) this._translationCache.clear()
        }
        if (body.ollamaModel !== undefined) {
          store.set('ollamaModel', body.ollamaModel)
          if (this._translationCache) this._translationCache.clear()
        }

        const logBody = { ...body, apiKey: body.apiKey !== undefined ? '***' : undefined }
        this._log('INFO', `settings updated: ${JSON.stringify(logBody)}`)

        if (body.apiKey) {
          const errors = store.get('errors') || []
          const hadApiKeyError = errors.some((e) => e.code === 'API_KEY_EMPTY')
          const filtered = errors.filter((e) => e.code !== 'API_KEY_EMPTY')
          if (hadApiKeyError) {
            filtered.unshift({
              code: 'RESOLVED',
              message: 'APIキーが設定されました',
              cause: '解決済み',
              solution: 'APIキーのエラーは解消されました',
              link: null,
              timestamp: new Date().toISOString(),
            })
          }
          store.set('errors', filtered)
          this._stateVersion++
        }

        return { code: 200, response: { ok: true } }
      }

      case 'DELETE': {
        const target = req.params?.target
        if (target === 'translations') {
          store.set('translations', [])
          this._stateVersion++
          this._log('INFO', 'translations cleared')
          return { code: 200, response: { ok: true } }
        }
        if (target === 'errors') {
          store.set('errors', [])
          this._stateVersion++
          this._log('INFO', 'errors cleared')
          return { code: 200, response: { ok: true } }
        }
        if (target === 'debug') {
          store.set('debugLog', [])
          this._stateVersion++
          this._log('INFO', 'debug log cleared')
          return { code: 200, response: { ok: true } }
        }
        return { code: 400, response: { error: 'unknown target. use: translations | errors | debug' } }
      }

      default:
        return { code: 405, response: { error: 'method not allowed' } }
    }
  },
}

module.exports = plugin
