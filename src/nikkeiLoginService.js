import { existsSync } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-chromium";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const sessionDir = path.join(projectRoot, "data");
const storageStatePath = path.join(sessionDir, "nikkei-storage-state.json");

const DEFAULT_BROWSER_PATHS = [
  process.env.PLAYWRIGHT_BROWSER_PATH,
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
].filter(Boolean);

function getLoginId() {
  return process.env.NIKKEI_LOGIN_ID || process.env.NIKKEI_XTECH_LOGIN_ID || "";
}

function getLoginPassword() {
  return process.env.NIKKEI_LOGIN_PASSWORD || process.env.NIKKEI_XTECH_LOGIN_PASSWORD || "";
}

function hasCredentials() {
  return Boolean(getLoginId() && getLoginPassword());
}

function getTargetUrls() {
  return [...new Set([
    process.env.NIKKEI_LOGIN_URL || "https://www.nikkei.com/",
    process.env.NIKKEI_XTECH_LOGIN_URL || "https://xtech.nikkei.com/"
  ])];
}

async function ensureSessionDir() {
  await fs.mkdir(sessionDir, { recursive: true });
}

async function fileExists(targetPath) {
  try {
    await fs.access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function getBrowserExecutablePath() {
  const explicitPath = DEFAULT_BROWSER_PATHS.find((candidate) => candidate && existsSync(candidate));
  if (explicitPath) {
    return explicitPath;
  }

  try {
    const bundledPath = chromium.executablePath();
    return bundledPath && existsSync(bundledPath) ? bundledPath : "";
  } catch {
    return "";
  }
}

function getLoginAvailability() {
  if (!hasCredentials()) {
    return {
      available: false,
      reason: "missing-credentials",
      details: "NIKKEI_LOGIN_ID / NIKKEI_LOGIN_PASSWORD が未設定です。"
    };
  }

  const executablePath = getBrowserExecutablePath();
  if (!executablePath) {
    return {
      available: false,
      reason: "missing-browser",
      details: "Playwright 用ブラウザがサーバー環境に見つかりません。Zeabur では自動再ログインは使えず、Cookie 事前設定が必要です。"
    };
  }

  return {
    available: true,
    reason: "available",
    details: "",
    executablePath
  };
}

async function loadStorageState() {
  if (!(await fileExists(storageStatePath))) {
    return null;
  }
  return JSON.parse(await fs.readFile(storageStatePath, "utf8"));
}

function filterRelevantCookies(cookies = []) {
  return cookies.filter((cookie) => {
    const domain = cookie.domain || "";
    return domain.includes("nikkei.com") || domain.includes("xtech.nikkei.com");
  });
}

function cookiesToHeader(cookies = []) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  return cookies
    .filter((cookie) => !cookie.expires || cookie.expires === -1 || cookie.expires > nowSeconds)
    .map((cookie) => `${cookie.name}=${cookie.value}`)
    .join("; ");
}

function summarizeCookieDomains(cookies = []) {
  const summary = { nikkei: 0, xtech: 0 };

  for (const cookie of cookies) {
    const domain = cookie.domain || "";
    if (domain.includes("xtech.nikkei.com")) {
      summary.xtech += 1;
    } else if (domain.includes("nikkei.com")) {
      summary.nikkei += 1;
    }
  }

  return summary;
}

export async function getSavedNikkeiCookieHeader() {
  const state = await loadStorageState();
  if (!state) {
    return "";
  }
  return cookiesToHeader(filterRelevantCookies(state.cookies || []));
}

export async function saveStorageState(state) {
  await ensureSessionDir();
  await fs.writeFile(storageStatePath, JSON.stringify(state, null, 2), "utf8");
}

export async function getNikkeiLoginStatus() {
  const state = await loadStorageState();
  const cookies = filterRelevantCookies(state?.cookies || []);
  const activeCookies = cookiesToHeader(cookies);
  const cookieDomains = summarizeCookieDomains(cookies);
  const loginAvailability = getLoginAvailability();

  return {
    hasCredentials: hasCredentials(),
    browserExecutablePath: getBrowserExecutablePath() || "",
    hasSavedSession: Boolean(state),
    savedCookieCount: cookies.length,
    sessionUsable: Boolean(activeCookies),
    storageStatePath,
    targetUrls: getTargetUrls(),
    cookieDomains,
    loginAvailable: loginAvailability.available,
    loginReason: loginAvailability.reason,
    loginDetails: loginAvailability.details
  };
}

async function launchBrowser() {
  const executablePath = getBrowserExecutablePath();
  if (!executablePath) {
    throw new Error("Playwright 用のブラウザ実行ファイルが見つかりません。PLAYWRIGHT_BROWSER_PATH を設定してください。");
  }

  return chromium.launch({
    headless: process.env.NIKKEI_HEADLESS !== "false" && process.env.NIKKEI_XTECH_HEADLESS !== "false",
    executablePath
  });
}

function loginSelectors() {
  return {
    loginButton:
      process.env.NIKKEI_LOGIN_BUTTON_SELECTOR ||
      process.env.NIKKEI_XTECH_LOGIN_BUTTON_SELECTOR ||
      'a[href*="login"], button[href*="login"], .login, .btn-login',
    loginId:
      process.env.NIKKEI_LOGIN_ID_SELECTOR ||
      process.env.NIKKEI_XTECH_LOGIN_ID_SELECTOR ||
      'input[type="email"], input[name="mail"], input[name="userId"], input[name="loginId"], input[type="text"]',
    password:
      process.env.NIKKEI_LOGIN_PASSWORD_SELECTOR ||
      process.env.NIKKEI_XTECH_LOGIN_PASSWORD_SELECTOR ||
      'input[type="password"], input[name="password"]',
    submit:
      process.env.NIKKEI_LOGIN_SUBMIT_SELECTOR ||
      process.env.NIKKEI_XTECH_LOGIN_SUBMIT_SELECTOR ||
      'button[type="submit"], input[type="submit"], .btn-login',
    success:
      process.env.NIKKEI_LOGIN_SUCCESS_SELECTOR ||
      process.env.NIKKEI_XTECH_LOGIN_SUCCESS_SELECTOR ||
      'a[href*="logout"], button[href*="logout"], .user, .account'
  };
}

async function performLogin(page, url, selectors) {
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

  const loginButton = page.locator(selectors.loginButton).first();
  if (await loginButton.count()) {
    await loginButton.click({ timeout: 10000 }).catch(() => {});
  }

  await page.locator(selectors.loginId).first().fill(getLoginId(), { timeout: 15000 });
  await page.locator(selectors.password).first().fill(getLoginPassword(), { timeout: 15000 });
  await page.locator(selectors.submit).first().click({ timeout: 15000 });
  await page.waitForLoadState("networkidle", { timeout: 60000 }).catch(() => {});
  await page.locator(selectors.success).first().waitFor({ timeout: 15000 }).catch(() => {});
}

export async function loginToNikkeiAndPersistSession({ force = false } = {}) {
  const availability = getLoginAvailability();
  if (!availability.available) {
    return { ok: false, reason: availability.reason, details: availability.details };
  }

  if (!force) {
    const existingHeader = await getSavedNikkeiCookieHeader();
    if (existingHeader) {
      return { ok: true, reused: true };
    }
  }

  let browser;

  try {
    browser = await launchBrowser();
    const context = await browser.newContext();
    const page = await context.newPage();
    const selectors = loginSelectors();
    const attemptedUrls = [];

    for (const url of getTargetUrls()) {
      attemptedUrls.push(url);
      await performLogin(page, url, selectors);
    }

    const state = await context.storageState();
    await saveStorageState(state);
    await browser.close();
    return { ok: true, reused: false, attemptedUrls };
  } catch (error) {
    return {
      ok: false,
      reason: "playwright-login-failed",
      details: error instanceof Error ? error.message : String(error)
    };
  } finally {
    await browser?.close().catch(() => {});
  }
}
