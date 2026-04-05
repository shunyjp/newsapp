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
const loginAttemptPath = path.join(sessionDir, "nikkei-login-attempt.json");
const debugLogPath = path.join(sessionDir, "nikkei-playwright-debug.txt");
const OTP_SESSION_TTL_MS = Number.parseInt(process.env.NIKKEI_OTP_SESSION_TTL_MS || `${10 * 60 * 1000}`, 10);
let lastLoginAttempt = null;
let pendingOtpSession = null;

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
      details: "NIKKEI_LOGIN_ID / NIKKEI_LOGIN_PASSWORD is not configured."
    };
  }

  const executablePath = getBrowserExecutablePath();
  if (!executablePath) {
    return {
      available: false,
      reason: "missing-browser",
      details: "No Playwright browser was found on the server."
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

async function loadLastLoginAttempt() {
  if (lastLoginAttempt) {
    return lastLoginAttempt;
  }
  if (!(await fileExists(loginAttemptPath))) {
    return null;
  }
  try {
    lastLoginAttempt = JSON.parse(await fs.readFile(loginAttemptPath, "utf8"));
    return lastLoginAttempt;
  } catch {
    return null;
  }
}

async function saveLastLoginAttempt(attempt) {
  lastLoginAttempt = attempt;
  await ensureSessionDir();
  await fs.writeFile(loginAttemptPath, JSON.stringify(attempt, null, 2), "utf8");
}

async function appendDebugLog(label, payload = {}) {
  const lines = [
    `[${new Date().toISOString()}] ${label}`,
    `pid: ${process.pid}`,
    `uptimeSeconds: ${Math.round(process.uptime())}`,
    ...Object.entries(payload).map(([key, value]) => {
      const rendered = typeof value === "string" ? value : JSON.stringify(value);
      return `${key}: ${rendered}`;
    }),
    ""
  ];

  await ensureSessionDir();
  await fs.appendFile(debugLogPath, `${lines.join("\n")}\n`, "utf8");
}

function otpSelectors() {
  return {
    otpInput: [
      'input[autocomplete="one-time-code"]',
      'input[name*="otp" i]',
      'input[name*="code" i]',
      'input[name*="challenge" i]',
      'input[id*="otp" i]',
      'input[id*="code" i]',
      'input[id*="challenge" i]',
      'input[inputmode="numeric"]',
      'input[maxlength="6"]'
    ].join(", "),
    otpSubmit:
      process.env.NIKKEI_OTP_SUBMIT_SELECTOR ||
      'button[type="submit"], input[type="submit"], button, [role="button"], button:has-text("Verify"), button:has-text("Submit"), button:has-text("Login"), button:has-text("確認"), button:has-text("送信"), button:has-text("認証"), button:has-text("続行"), button:has-text("次へ")'
  };
}

function isOtpChallengePage(url = "", title = "") {
  return url.includes("/login/challenge") || title.includes("ワンタイムパスワード");
}

async function waitForOtpTransition(page) {
  const startUrl = page.url();

  await Promise.race([
    page.waitForURL((nextUrl) => nextUrl !== startUrl, { timeout: 15000 }).catch(() => {}),
    page.waitForLoadState("domcontentloaded", { timeout: 15000 }).catch(() => {}),
    page.waitForTimeout(5000)
  ]);
}

async function clearPendingOtpSession() {
  if (!pendingOtpSession) {
    await appendDebugLog("otpSession:clear-skip", { reason: "no-pending-session" });
    return;
  }

  const session = pendingOtpSession;
  await appendDebugLog("otpSession:clear", {
    createdAt: new Date(session.createdAt).toISOString(),
    ageMs: Date.now() - session.createdAt,
    pageUrl: session.page?.url?.() || ""
  });
  pendingOtpSession = null;
  await session.page?.close().catch(() => {});
  await session.context?.close().catch(() => {});
  await session.browser?.close().catch(() => {});
}

function getPendingOtpSession() {
  if (!pendingOtpSession) {
    void appendDebugLog("otpSession:get-miss", { reason: "null" });
    return null;
  }

  if (Date.now() - pendingOtpSession.createdAt > OTP_SESSION_TTL_MS) {
    void appendDebugLog("otpSession:get-expired", {
      createdAt: new Date(pendingOtpSession.createdAt).toISOString(),
      ageMs: Date.now() - pendingOtpSession.createdAt,
      ttlMs: OTP_SESSION_TTL_MS
    });
    void clearPendingOtpSession();
    return null;
  }

  void appendDebugLog("otpSession:get-hit", {
    createdAt: new Date(pendingOtpSession.createdAt).toISOString(),
    ageMs: Date.now() - pendingOtpSession.createdAt,
    pageUrl: pendingOtpSession.page?.url?.() || ""
  });
  return pendingOtpSession;
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
  const savedAttempt = await loadLastLoginAttempt();
  const cookies = filterRelevantCookies(state?.cookies || []);
  const activeCookies = cookiesToHeader(cookies);
  const cookieDomains = summarizeCookieDomains(cookies);
  const loginAvailability = getLoginAvailability();
  const activeOtpSession = getPendingOtpSession();

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
    loginDetails: loginAvailability.details || savedAttempt?.details || "",
    debugLogPath,
    lastLoginAttempt: savedAttempt,
    otpPending: Boolean(activeOtpSession),
    otpExpiresAt: activeOtpSession ? new Date(activeOtpSession.createdAt + OTP_SESSION_TTL_MS).toISOString() : ""
  };
}

async function launchBrowser() {
  const executablePath = getBrowserExecutablePath();
  if (!executablePath) {
    throw new Error("No Playwright browser executable was found. Set PLAYWRIGHT_BROWSER_PATH if needed.");
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
      [
        'input[type="email"]',
        'input[name="mail"]',
        'input[name="email"]',
        'input[name="userId"]',
        'input[name="loginId"]',
        'input[id*="mail"]',
        'input[id*="email"]',
        'input[autocomplete="username"]',
        'input[inputmode="email"]',
        'input[type="text"]'
      ].join(", "),
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

async function locateFirst(locatorFactory, page) {
  for (const frame of page.frames()) {
    const locator = locatorFactory(frame);
    const count = await locator.count().catch(() => 0);
    if (count > 0) {
      return { frame, locator, count };
    }
  }

  return { frame: page.mainFrame(), locator: locatorFactory(page.mainFrame()), count: 0 };
}

async function waitForFirst(locatorFactory, page, { timeout = 10000, interval = 250 } = {}) {
  const deadline = Date.now() + timeout;

  while (Date.now() < deadline) {
    const match = await locateFirst(locatorFactory, page);
    if (match.count > 0) {
      return match;
    }
    await page.waitForTimeout(interval).catch(() => {});
  }

  return locateFirst(locatorFactory, page);
}

async function buildFrameSummary(page, selectors) {
  const summary = [];
  for (const frame of page.frames()) {
    summary.push({
      url: frame.url(),
      loginId: await frame.locator(selectors.loginId).count().catch(() => 0),
      password: await frame.locator(selectors.password).count().catch(() => 0),
      submit: await frame.locator(selectors.submit).count().catch(() => 0),
      success: await frame.locator(selectors.success).count().catch(() => 0)
    });
  }
  return summary;
}

async function locateOtpInputs(page) {
  const selectors = otpSelectors();
  return locateFirst((frame) => frame.locator(selectors.otpInput), page);
}

async function buildOtpFrameSummary(page) {
  const selectors = otpSelectors();
  const summary = [];
  for (const frame of page.frames()) {
    const buttons = await frame.locator("button, input[type='submit'], input[type='button'], [role='button']").evaluateAll((elements) =>
      elements.slice(0, 8).map((element) => {
        const text = "value" in element && typeof element.value === "string" && element.value
          ? element.value
          : element.textContent || "";
        return text.replace(/\s+/g, " ").trim().slice(0, 80);
      }).filter(Boolean)
    ).catch(() => []);
    summary.push({
      url: frame.url(),
      otpInput: await frame.locator(selectors.otpInput).count().catch(() => 0),
      otpSubmit: await frame.locator(selectors.otpSubmit).count().catch(() => 0),
      buttons
    });
  }
  return summary;
}

async function dispatchInputEvents(locator) {
  await locator.evaluate((element) => {
    for (const eventName of ["input", "change", "keyup", "blur"]) {
      element.dispatchEvent(new Event(eventName, { bubbles: true }));
    }
  }).catch(() => {});
}

async function submitOtpForm(otpMatch) {
  const frame = otpMatch.frame;
  const locator = otpMatch.locator;
  const inputCount = otpMatch.count;
  const targetInput = inputCount === 1 ? locator.first() : locator.nth(inputCount - 1);
  const submitLocator = frame.locator(otpSelectors().otpSubmit);
  const submitCount = await submitLocator.count().catch(() => 0);

  for (let index = 0; index < submitCount; index += 1) {
    const candidate = submitLocator.nth(index);
    const disabled = await candidate.evaluate((element) => {
      if (!(element instanceof HTMLElement)) {
        return true;
      }
      const ariaDisabled = element.getAttribute("aria-disabled");
      return element.hasAttribute("disabled") || ariaDisabled === "true";
    }).catch(() => true);
    if (disabled) {
      continue;
    }

    await candidate.click({ timeout: 5000 }).catch(() => {});
    await frame.page().waitForTimeout(750).catch(() => {});
    const remainingOtp = await locateOtpInputs(frame.page());
    if (!remainingOtp.count || remainingOtp.frame !== frame) {
      return { count: submitCount, clicked: index + 1 };
    }
  }

  await targetInput.press("Enter").catch(() => {});
  await frame.page().waitForTimeout(750).catch(() => {});

  await frame.locator("form").evaluateAll((forms) => {
    for (const form of forms) {
      if (form instanceof HTMLFormElement) {
        if (typeof form.requestSubmit === "function") {
          form.requestSubmit();
        } else {
          form.submit();
        }
      }
    }
  }).catch(() => {});

  return { count: submitCount, clicked: 0 };
}

async function performLogin(page, url, selectors) {
  await appendDebugLog("performLogin:start", { url });
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

  const loginButtonMatch = await locateFirst((frame) => frame.locator(selectors.loginButton), page);
  const loginButtonCount = loginButtonMatch.count;
  const loginButton = loginButtonMatch.locator.first();
  if (loginButtonCount) {
    await loginButton.click({ timeout: 10000 }).catch(() => {});
    await page.waitForLoadState("domcontentloaded", { timeout: 10000 }).catch(() => {});
  }

  const loginIdMatch = await waitForFirst((frame) => frame.locator(selectors.loginId), page, { timeout: 12000 });
  const submitMatch = await waitForFirst((frame) => frame.locator(selectors.submit), page, { timeout: 12000 });
  const passwordMatchBefore = await waitForFirst((frame) => frame.locator(selectors.password), page, { timeout: 4000 });
  const successMatchBefore = await waitForFirst((frame) => frame.locator(selectors.success), page, { timeout: 4000 });

  const loginIdCount = loginIdMatch.count;
  const passwordCountBefore = passwordMatchBefore.count;
  const submitCount = submitMatch.count;
  const successCountBefore = successMatchBefore.count;

  if (!loginIdCount || !submitCount) {
    await appendDebugLog("performLogin:selector-check", {
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCountBefore,
        submit: submitCount,
        success: successCountBefore
      },
      frames: await buildFrameSummary(page, selectors)
    });
    throw new Error(JSON.stringify({
      stage: "selector-check",
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCountBefore,
        submit: submitCount,
        success: successCountBefore
      },
      frames: await buildFrameSummary(page, selectors)
    }));
  }

  await loginIdMatch.locator.first().fill(getLoginId(), { timeout: 15000 });
  await submitMatch.locator.first().click({ timeout: 15000 });
  await page.waitForLoadState("domcontentloaded", { timeout: 10000 }).catch(() => {});

  const passwordMatch = await waitForFirst((frame) => frame.locator(selectors.password), page, { timeout: 12000 });
  const successMatch = await waitForFirst((frame) => frame.locator(selectors.success), page, { timeout: 4000 });
  const passwordCount = passwordMatch.count;
  const successCount = successMatch.count;

  if (!passwordCount) {
    await appendDebugLog("performLogin:password-check", {
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCount,
        submit: submitCount,
        success: successCount
      },
      frames: await buildFrameSummary(page, selectors)
    });
    throw new Error(JSON.stringify({
      stage: "password-check",
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCount,
        submit: submitCount,
        success: successCount
      },
      frames: await buildFrameSummary(page, selectors)
    }));
  }

  const submitAfterPasswordMatch = await waitForFirst((frame) => frame.locator(selectors.submit), page, { timeout: 12000 });
  await passwordMatch.locator.first().fill(getLoginPassword(), { timeout: 15000 });
  await submitAfterPasswordMatch.locator.first().click({ timeout: 15000 });
  await page.waitForLoadState("domcontentloaded", { timeout: 15000 }).catch(() => {});
  await page.waitForLoadState("networkidle", { timeout: 60000 }).catch(() => {});

  const otpMatch = await locateOtpInputs(page);
  const currentUrl = page.url();
  const currentTitle = await page.title().catch(() => "");
  const challengeInputMatch = await waitForFirst((frame) => frame.locator(selectors.loginId), page, { timeout: 2000 });
  if (otpMatch.count > 0 || (isOtpChallengePage(currentUrl, currentTitle) && challengeInputMatch.count > 0)) {
    await appendDebugLog("performLogin:otp-required", {
      url,
      currentUrl,
      title: currentTitle,
      counts: {
        otp: otpMatch.count,
        challengeInputs: challengeInputMatch.count
      },
      otpFrames: await buildOtpFrameSummary(page)
    });
    throw new Error(JSON.stringify({
      stage: "otp-required",
      url,
      currentUrl,
      title: currentTitle,
      counts: {
        otp: otpMatch.count,
        challengeInputs: challengeInputMatch.count
      },
      frames: await buildFrameSummary(page, selectors)
    }));
  }

  await successMatch.locator.first().waitFor({ timeout: 15000 }).catch(async () => {
    await appendDebugLog("performLogin:success-check", {
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCount,
        submit: await submitAfterPasswordMatch.locator.count().catch(() => 0),
        success: await successMatch.locator.count().catch(() => 0)
      },
      frames: await buildFrameSummary(page, selectors),
      otpFrames: await buildOtpFrameSummary(page)
    });
    throw new Error(JSON.stringify({
      stage: "success-check",
      url,
      currentUrl: page.url(),
      title: await page.title().catch(() => ""),
      counts: {
        loginButton: loginButtonCount,
        loginId: loginIdCount,
        password: passwordCount,
        submit: await submitAfterPasswordMatch.locator.count().catch(() => 0),
        success: await successMatch.locator.count().catch(() => 0)
      },
      frames: await buildFrameSummary(page, selectors)
    }));
  });
}

export async function loginToNikkeiAndPersistSession({ force = false } = {}) {
  const availability = getLoginAvailability();
  if (!availability.available) {
    return { ok: false, reason: availability.reason, details: availability.details };
  }

  await clearPendingOtpSession();

  if (!force) {
    const existingHeader = await getSavedNikkeiCookieHeader();
    if (existingHeader) {
      await saveLastLoginAttempt({ ok: true, reused: true, details: "saved-session-reused" });
      return { ok: true, reused: true };
    }
  }

  let browser;
  let context;
  let page;

  try {
    await appendDebugLog("loginToNikkei:start", { force, targetUrls: getTargetUrls() });
    browser = await launchBrowser();
    context = await browser.newContext();
    page = await context.newPage();
    const selectors = loginSelectors();
    const attemptedUrls = [];

    for (const url of getTargetUrls()) {
      attemptedUrls.push(url);
      await performLogin(page, url, selectors);
    }

    const state = await context.storageState();
    await saveStorageState(state);
    await appendDebugLog("loginToNikkei:success", {
      attemptedUrls,
      savedCookieCount: Array.isArray(state.cookies) ? state.cookies.length : 0
    });
    await browser.close();
    await saveLastLoginAttempt({ ok: true, reused: false, details: "login-succeeded", attemptedUrls });
    return { ok: true, reused: false, attemptedUrls };
  } catch (error) {
    const details = error instanceof Error ? error.message : String(error);
    let parsedDetails = null;
    try {
      parsedDetails = JSON.parse(details);
    } catch {
      parsedDetails = null;
    }

    const isOtpStage =
      parsedDetails?.stage === "otp-required" ||
      (parsedDetails?.stage === "success-check" && isOtpChallengePage(parsedDetails?.currentUrl || "", parsedDetails?.title || ""));

    if (isOtpStage) {
      await appendDebugLog("loginToNikkei:otp-session-pending", {
        details: parsedDetails,
        currentUrl: parsedDetails?.currentUrl || "",
        title: parsedDetails?.title || ""
      });
      await appendDebugLog("otpSession:set", {
        createdAt: new Date().toISOString(),
        currentUrl: parsedDetails?.currentUrl || "",
        title: parsedDetails?.title || ""
      });
      pendingOtpSession = {
        browser,
        context,
        page,
        createdAt: Date.now()
      };
      const otpAttempt = {
        ok: false,
        reason: "otp-required",
        details: "One-time password required. Enter the code from the email.",
        otpPending: true,
        currentUrl: parsedDetails.currentUrl || "",
        title: parsedDetails.title || ""
      };
      await saveLastLoginAttempt(otpAttempt);
      return otpAttempt;
    }

    const failure = {
      ok: false,
      reason: "playwright-login-failed",
      details
    };
    await appendDebugLog("loginToNikkei:failure", {
      details,
      parsedDetails
    });
    await saveLastLoginAttempt(failure);
    return {
      ok: false,
      reason: failure.reason,
      details: failure.details
    };
  } finally {
    if (!pendingOtpSession || pendingOtpSession.browser !== browser) {
      await page?.close().catch(() => {});
      await context?.close().catch(() => {});
      await browser?.close().catch(() => {});
    }
  }
}

export async function submitNikkeiOtpAndPersistSession(code) {
  const session = getPendingOtpSession();
  if (!session) {
    await appendDebugLog("submitOtp:missing-session", {
      reason: "otp-session-missing",
      codeLength: String(code || "").replace(/\s+/g, "").trim().length,
      lastLoginAttempt: await loadLastLoginAttempt()
    });
    return {
      ok: false,
      reason: "otp-session-missing",
      details: "No pending OTP login session was found. Start login again."
    };
  }

  const normalizedCode = String(code || "").replace(/\s+/g, "").trim();
  if (!normalizedCode) {
    await appendDebugLog("submitOtp:missing-code", {
      reason: "otp-code-missing"
    });
    return {
      ok: false,
      reason: "otp-code-missing",
      details: "Enter the one-time password."
    };
  }

  try {
    await appendDebugLog("submitOtp:start", {
      codeLength: normalizedCode.length,
      currentUrl: session.page.url(),
      title: await session.page.title().catch(() => ""),
      otpFrames: await buildOtpFrameSummary(session.page)
    });
    const otpMatch = await locateOtpInputs(session.page);
    if (!otpMatch.count) {
      await appendDebugLog("submitOtp:otp-input-not-found", {
        currentUrl: session.page.url(),
        title: await session.page.title().catch(() => ""),
        otpFrames: await buildOtpFrameSummary(session.page)
      });
      throw new Error(JSON.stringify({
        stage: "otp-submit",
        reason: "otp_input_not_found",
        currentUrl: session.page.url(),
        title: await session.page.title().catch(() => ""),
        frames: await buildOtpFrameSummary(session.page)
      }));
    }

    const inputCount = otpMatch.count;
    if (inputCount === 1) {
      const input = otpMatch.locator.first();
      await input.click({ timeout: 15000 }).catch(() => {});
      await input.fill("", { timeout: 15000 }).catch(() => {});
      await input.type(normalizedCode, { delay: 80, timeout: 15000 }).catch(async () => {
        await input.fill(normalizedCode, { timeout: 15000 });
      });
      await dispatchInputEvents(input);
    } else {
      const digits = normalizedCode.split("");
      for (let index = 0; index < Math.min(inputCount, digits.length); index += 1) {
        const input = otpMatch.locator.nth(index);
        await input.click({ timeout: 15000 }).catch(() => {});
        await input.fill("", { timeout: 15000 }).catch(() => {});
        await input.type(digits[index], { delay: 60, timeout: 15000 }).catch(async () => {
          await input.fill(digits[index], { timeout: 15000 });
        });
        await dispatchInputEvents(input);
      }
    }

    const otpSubmitMatch = await submitOtpForm(otpMatch);
    await appendDebugLog("submitOtp:after-submit", {
      currentUrl: session.page.url(),
      title: await session.page.title().catch(() => ""),
      submitCount: otpSubmitMatch.count,
      clickedSubmitIndex: otpSubmitMatch.clicked,
      otpFrames: await buildOtpFrameSummary(session.page)
    });

    await waitForOtpTransition(session.page);

    const remainingOtp = await locateOtpInputs(session.page);
    if (remainingOtp.count > 0) {
      await appendDebugLog("submitOtp:otp-still-visible", {
        currentUrl: session.page.url(),
        title: await session.page.title().catch(() => ""),
        submitCount: otpSubmitMatch.count,
        clickedSubmitIndex: otpSubmitMatch.clicked,
        otpFrames: await buildOtpFrameSummary(session.page)
      });
      throw new Error(JSON.stringify({
        stage: "otp-submit",
        reason: "otp_input_still_visible",
        currentUrl: session.page.url(),
        title: await session.page.title().catch(() => ""),
        submitCount: otpSubmitMatch.count,
        frames: await buildOtpFrameSummary(session.page)
      }));
    }

    const state = await session.context.storageState();
    await saveStorageState(state);
    await appendDebugLog("submitOtp:success", {
      currentUrl: session.page.url(),
      title: await session.page.title().catch(() => ""),
      savedCookieCount: Array.isArray(state.cookies) ? state.cookies.length : 0
    });
    await saveLastLoginAttempt({
      ok: true,
      reused: false,
      details: "otp-login-succeeded",
      attemptedUrls: getTargetUrls()
    });
    await clearPendingOtpSession();
    return { ok: true, reused: false };
  } catch (error) {
    const details = error instanceof Error ? error.message : String(error);
    const failure = {
      ok: false,
      reason: "otp-submit-failed",
      details
    };
    await appendDebugLog("submitOtp:failure", { details });
    await saveLastLoginAttempt(failure);
    return failure;
  }
}
