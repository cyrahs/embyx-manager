// Page smoke of the unmodified React front end against the Rust server (WP4).
//
// The server binary runs from a temporary directory with the built front end beside it
// (`static/`, where `serve` looks for it), on a scratch database, with an API token and a
// stand-in Emby. A headless browser then signs in, opens every page, reads and saves the
// Emby settings, runs its connection test and reloads a deep link. The monitor and playlists
// pages (WP9) must load their data without an API error: the dashboard shows the four
// pipeline cards and a refused trigger, the subscriptions page the feed seeded through the
// API, the playlists page its empty list. Finally the server gets SIGTERM and must shut
// down cleanly; the scheduler it started must stop with it.
//
// Environment: SMOKE_BINARY (the embyx-manager binary), SMOKE_STATIC (the front end's
// build), SMOKE_DATABASE_URL (a scratch database), SMOKE_SCREENSHOTS (optional directory).
// Run through tools/smoke/run.sh, which prepares all three.

import { spawn } from 'node:child_process'
import { cpSync, mkdtempSync, rmSync, mkdirSync } from 'node:fs'
import { createServer } from 'node:http'
import { createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { chromium } from 'playwright'

const TOKEN = 'smoke-token'
const EMBY_KEY = 'smoke-key'
// API prefixes whose routes arrive with a later work package; their pages may get 404 until then.
const NOT_YET_PORTED = ['/api/merge/'] // WP14

const PAGES = [
  ['/dashboard', '监控看板'],
  ['/fill-actor', '补全演员'],
  ['/subscriptions', '订阅'],
  ['/playlists', '列表'],
  ['/merge', '合并'],
  ['/settings', '设置'],
]

function required(name) {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is not set (run tools/smoke/run.sh)`)
  return value
}

function check(condition, message) {
  if (!condition) throw new Error(`smoke check failed: ${message}`)
  console.log(`ok: ${message}`)
}

async function freePort() {
  const server = createNetServer()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address()
  await new Promise((resolve) => server.close(resolve))
  return port
}

/** A stand-in Emby: `/emby/System/Info` for the right key, 401 otherwise. */
async function startEmby() {
  const seen = []
  const server = createServer((request, response) => {
    seen.push({ method: request.method, url: request.url, token: request.headers['x-emby-token'] ?? null })
    if (request.url !== '/emby/System/Info') {
      response.writeHead(404).end()
    } else if (request.headers['x-emby-token'] !== EMBY_KEY) {
      response.writeHead(401).end()
    } else {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ ServerName: 'embyx-smoke', Version: '4.10.0.40', Id: 'smoke' }))
    }
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  return { server, seen, address: `http://127.0.0.1:${server.address().port}` }
}

function startServer(binary, port, databaseUrl) {
  const output = { stdout: '', stderr: '' }
  const child = spawn(binary, ['serve'], {
    env: {
      PATH: process.env.PATH ?? '/usr/bin:/bin',
      EMBYX_MANAGER_DATABASE_URL: databaseUrl,
      EMBYX_MANAGER_HOST: '127.0.0.1',
      EMBYX_MANAGER_PORT: String(port),
      EMBYX_MANAGER_API_TOKEN: TOKEN,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  child.stdout.on('data', (chunk) => (output.stdout += chunk))
  child.stderr.on('data', (chunk) => (output.stderr += chunk))
  const exited = new Promise((resolve) => child.on('exit', (code, signal) => resolve({ code, signal })))
  return { child, output, exited }
}

async function waitForHealth(base, exited) {
  let gone = false
  void exited.then(() => (gone = true))
  for (let attempt = 0; attempt < 300; attempt += 1) {
    if (gone) throw new Error('the server exited during startup')
    try {
      const response = await fetch(`${base}/api/health`)
      if (response.status === 200) return response.json()
    } catch {
      // not listening yet
    }
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error('the server did not become healthy')
}

async function api(base, method, path, body) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers: { authorization: `Bearer ${TOKEN}`, 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  return { status: response.status, body: await response.json() }
}

async function main() {
  const binary = required('SMOKE_BINARY')
  const staticDir = required('SMOKE_STATIC')
  const databaseUrl = required('SMOKE_DATABASE_URL')
  const screenshots = process.env.SMOKE_SCREENSHOTS

  // `serve` finds the front end next to its executable.
  const home = mkdtempSync(join(tmpdir(), 'embyx-smoke-'))
  cpSync(binary, join(home, 'embyx-manager'))
  cpSync(staticDir, join(home, 'static'), { recursive: true })

  const emby = await startEmby()
  const port = await freePort()
  const base = `http://127.0.0.1:${port}`
  const server = startServer(join(home, 'embyx-manager'), port, databaseUrl)
  let browser
  try {
    const health = await waitForHealth(base, server.exited)
    check(health.status === 'ok' && health.auth_required === true, 'health is ok and asks for a token')

    const seeded = await api(base, 'PUT', '/api/config/emby', { values: { address: emby.address, api_key: EMBY_KEY } })
    check(seeded.status === 200 && seeded.body.version === 1, 'the Emby section is seeded through the API')

    const categories = await api(base, 'PUT', '/api/config/rss', {
      values: { categories: [{ label: 'rank', task_dir_path: '/115/rank' }] },
    })
    check(categories.status === 200, 'an RSS category is seeded through the API')
    const feed = await api(base, 'POST', '/api/monitor/subscriptions', {
      kind: 'rss',
      category: 'rank',
      url: 'https://feeds.invalid/smoke.xml',
      name: 'Smoke feed',
    })
    check(feed.status === 201 && feed.body.kind === 'rss', 'a feed subscription is created through the API')
    const status = await api(base, 'GET', '/api/monitor/status')
    check(
      status.status === 200 && status.body.map((item) => item.pipeline).join() === 'rss,archive,mapping,playlists',
      'the monitor reports every pipeline',
    )

    const asset = await fetch(`${base}/assets/missing.js`)
    check(asset.status === 404, 'a missing asset is a 404, not the SPA page')

    browser = await chromium.launch()
    const page = await browser.newPage()
    const pageErrors = []
    page.on('pageerror', (error) => pageErrors.push(String(error)))

    await page.goto(`${base}/`)
    await page.waitForURL(`${base}/dashboard`)
    const tokenField = page.getByLabel('API Token')
    await tokenField.waitFor()
    check(true, 'the root redirects to the dashboard behind the login screen')

    await tokenField.fill('wrong-token')
    await tokenField.press('Enter')
    await page.getByText('API Token 不正确').waitFor()
    check(true, 'a wrong token is refused by the login probe')

    // From here on every API call a page makes must be answered: a 404 or 405 is a route
    // the server lacks, a 5xx a handler that failed.
    const apiFailures = []
    page.on('response', (response) => {
      const url = new URL(response.url())
      const pending = NOT_YET_PORTED.some((prefix) => url.pathname.startsWith(prefix))
      if (
        url.pathname.startsWith('/api/') &&
        !pending &&
        (response.status() >= 500 || [404, 405].includes(response.status()))
      ) {
        apiFailures.push(`${response.request().method()} ${url.pathname}${url.search} ${response.status()}`)
      }
    })

    await tokenField.fill(TOKEN)
    await tokenField.press('Enter')
    await page.getByText('服务正常').waitFor()
    check(true, 'the right token opens the app and the health chip is green')

    const cards = page.locator('article.pipeline-card')
    await cards.nth(3).waitFor()
    check((await cards.count()) === 4, 'the dashboard shows the four pipeline cards')
    const rssCard = page.locator('article[aria-label="RSS 磁力摄取"]')
    await rssCard.getByText('未配置', { exact: true }).waitFor()
    await rssCard.getByText('尚未配置 CloudDrive').waitFor()
    check(true, 'without CloudDrive the RSS pipeline reads as not configured, with the reason')

    for (const [path, label] of PAGES) {
      await page.getByRole('navigation', { name: '页面导航' }).locator(`a[href="${path}"]`).click()
      await page.waitForURL(`${base}${path}`)
      await page.waitForFunction((title) => document.title === title, `${label} · Embyx Manager`)
      await page.waitForLoadState('networkidle')
      check(true, `${path} renders (${await page.title()})`)
      if (path === '/subscriptions') {
        await page.getByRole('tab', { name: /榜单/ }).click()
        await page.getByText('https://feeds.invalid/smoke.xml').waitFor()
        check(true, 'the subscriptions page lists the seeded feed')
      } else if (path === '/fill-actor') {
        await page.getByText('补全演员尚未配置').waitFor()
        check(true, "the fill-actor page reads the health block's unconfigured state")
      } else if (path === '/playlists') {
        await page.getByText('还没有列表').waitFor()
        check(true, 'the playlists page shows its empty list')
      }
    }
    check(apiFailures.length === 0, `every page's API calls are answered${apiFailures.length ? `: ${apiFailures.join('; ')}` : ''}`)

    const panel = page.locator('section[aria-labelledby="settings-emby"]')
    await panel.getByText('v1').waitFor()
    check((await page.locator('#emby-address').inputValue()) === emby.address, 'settings show the stored Emby address')

    await panel.getByRole('button', { name: '测试连接' }).click()
    await panel.getByText('embyx-smoke 4.10.0.40').waitFor()
    check(
      emby.seen.some((call) => call.url === '/emby/System/Info' && call.token === EMBY_KEY),
      'the connection test reached Emby with the stored key',
    )

    await page.locator('#emby-address').fill(`${emby.address}/`)
    await panel.getByRole('button', { name: '保存' }).click()
    await panel.getByText('已保存并生效。').waitFor()
    await panel.getByText('v2').waitFor()
    const stored = await api(base, 'GET', '/api/config/emby')
    check(stored.body.version === 2 && stored.body.values.address === emby.address, 'saving normalizes and stores the address')
    check(stored.body.values.api_key === '' && stored.body.secrets.api_key === true, 'the key stays stored and masked')

    await page.reload()
    await page.locator('section[aria-labelledby="settings-emby"]').getByText('v2').waitFor()
    check(true, 'a reloaded deep link is served the app and shows the saved section')

    if (screenshots) {
      mkdirSync(screenshots, { recursive: true })
      await page.screenshot({ path: join(screenshots, 'settings.png'), fullPage: true })
    }
    check(pageErrors.length === 0, `no uncaught page errors${pageErrors.length ? `: ${pageErrors.join('; ')}` : ''}`)
  } finally {
    await browser?.close()
    emby.server.close()
    server.child.kill('SIGTERM')
    const { code } = await server.exited
    rmSync(home, { recursive: true, force: true })
    if (process.exitCode === undefined || process.exitCode === 0) {
      check(code === 0, `the server exits 0 on SIGTERM (got ${code})`)
      check(server.output.stderr.includes('Application shutdown complete.'), 'the lifespans shut down')
      check(server.output.stdout.includes('"GET /api/health HTTP/1.1" 200 OK'), 'the access log is written')
    } else {
      console.error(server.output.stderr)
    }
  }
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
