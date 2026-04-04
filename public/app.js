const form = document.querySelector("#digest-form");
const topicOptions = document.querySelector("#topic-options");
const viewpointOptions = document.querySelector("#viewpoint-options");
const statusNode = document.querySelector("#status");
const resultsNode = document.querySelector("#results");
const summaryNode = document.querySelector("#summary");
const articlesNode = document.querySelector("#articles");
const downloadsNode = document.querySelector("#downloads");
const submitButton = document.querySelector("#submit-button");
const importTopicNode = document.querySelector("#import-topic");
const bookmarkletLink = document.querySelector("#bookmarklet-link");
const importsNode = document.querySelector("#imports");
const nikkeiStatusNode = document.querySelector("#nikkei-status");
const nikkeiReloginButton = document.querySelector("#nikkei-relogin");
const nikkeiOtpPanel = document.querySelector("#nikkei-otp-panel");
const nikkeiOtpCodeNode = document.querySelector("#nikkei-otp-code");
const nikkeiOtpSubmitButton = document.querySelector("#nikkei-otp-submit");

function renderOption(container, option, name, checked = true, description = "") {
  const label = document.createElement("label");
  label.className = "option-card";

  const input = document.createElement("input");
  input.type = "checkbox";
  input.name = name;
  input.value = option.id;
  input.checked = checked;

  const text = document.createElement("span");
  text.innerHTML = `<strong>${option.label}</strong>${description ? `<span class="option-meta">${description}</span>` : ""}`;
  label.append(input, text);
  container.append(label);
}

function collectChecked(name) {
  return Array.from(document.querySelectorAll(`input[name="${name}"]:checked`)).map((node) => node.value);
}

function renderDownloads(files) {
  downloadsNode.innerHTML = "";

  [
    ["text", "テキストを開く"],
    ["word", "Wordを開く"],
    ["audio", "音声を開く"],
    ["notebooklm", "NotebookLM用原文#��開く"]
  ].forEach(([key, label]) => {
    if (!files[key]) {
      return;
    }

    const link = document.createElement("a");
    link.href = files[key];
    link.textContent = label;
    link.target = "_blank";
    downloadsNode.append(link);
  });
}

function renderArticles(articles) {
  articlesNode.innerHTML = "";

  articles.forEach((article) => {
    const card = document.createElement("article");
    card.className = "article-card";
    card.innerHTML = `
      <p class="eyebrow">${article.topicLabel}</p>
      <h3>${article.title}</h3>
      <p>${article.contentSnippet || "概要は取得できませんでした。"}</p>
      <p>${article.source || "Source unknown"} / ${article.pubDate || "Date unknown"} / trust ${article.trustScore ?? 0}</p>
      <a href="${article.link}" target="_blank" rel="noreferrer">記事を見る</a>
    `;
    articlesNode.append(card);
  });
}

async function deleteImportedArticle(link) {
  const response = await fetch("/api/imports", {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ link })
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || data.details || "delete failed");
  }
}

function renderImportedArticles(articles) {
  importsNode.innerHTML = "";

  if (!articles.length) {
    const empty = document.createElement("p");
    empty.className = "status";
    empty.textContent = "まだ取り込み済み記事はありません。";
    importsNode.append(empty);
    return;
  }

  articles.forEach((article) => {
    const card = document.createElement("article");
    card.className = "article-card";
    card.innerHTML = `
      <p class="eyebrow">${article.topicLabel} / imported</p>
      <h3>${article.title}</h3>
      <p>${article.contentSnippet || "本文はありません。"}</p>
      <p>${article.source || "Source unknown"} / ${article.importedAt || article.pubDate || "Date unknown"}</p>
      <div class="card-actions">
        <a href="${article.link}" target="_blank" rel="noreferrer">記事を開く</a>
        <button type="button" class="danger-button" data-delete-link="${article.link}">この取り込み記事を削除</button>
      </div>
    `;
    importsNode.append(card);
  });

  importsNode.querySelectorAll("[data-delete-link]").forEach((button) => {
    button.addEventListener("click", async () => {
      const link = button.getAttribute("data-delete-link");
      button.disabled = true;

      try {
        await deleteImportedArticle(link);
        statusNode.textContent = "取り込み記事を削除しました。";
        await refreshImports();
      } catch (error) {
        statusNode.textContent = `削除に失敗しました: ${error.message}`;
        button.disabled = false;
      }
    });
  });
}

async function refreshImports() {
  const response = await fetch("/api/imports");
  const data = await response.json();
  renderImportedArticles(data.articles || []);
}

function buildBookmarklet() {
  const selectedTopic = importTopicNode.value || "sap";
  const payloadUrl = `${window.location.origin}/api/imports`;
  const script =
    `(function(){const topicId=${JSON.stringify(selectedTopic)};` +
    "const pick=(sels)=>sels.map((s)=>document.querySelector(s)).find(Boolean);" +
    "const body=pick(['article','main','[role=\"main\"]','.container','#content'])||document.body;" +
    "const text=(body.innerText||'').replace(/\\s+/g,' ').trim().slice(0,4000);" +
    "const title=document.title||'Untitled';" +
    `fetch(${JSON.stringify(payloadUrl)},{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:location.href,title,content:text,topicId})})` +
    ".then(async(r)=>{const data=await r.json();if(!r.ok)throw new Error(data.error||'import failed');alert('記事を取り込みました');})" +
    ".catch((e)=>alert('取り込みに失敗しました: '+e.message));})();";

  bookmarkletLink.href = `javascript:${script}`;
}

function renderNikkeiStatus(status) {
  const browser = status.browserExecutablePath
    ? `ブラウザ: ${status.browserExecutablePath}`
    : "ブラウザ: 未検出";
  const cookieDomains = status.cookieDomains
    ? `cookie: 日経新聞 ${status.cookieDomains.nikkei || 0} / xTECH ${status.cookieDomains.xtech || 0}`
    : null;
  const targets = Array.isArray(status.targetUrls) && status.targetUrls.length
    ? `対象: ${status.targetUrls.join(" , ")}`
    : null;

  const lines = [
    status.hasCredentials ? "資格情報: 設定済み" : "資格情報: 未設定",
    status.hasSavedSession ? "保存済みセッション: あり" : "保存済みセッション: なし",
    status.sessionUsable
      ? `セッション状態: 利用可能 (${status.savedCookieCount} cookies)`
      : "セッション状態: 利用不可",
    cookieDomains,
    browser,
    targets
  ].filter(Boolean);

  nikkeiStatusNode.textContent = lines.join(" / ");
  if (status.loginDetails) {
    nikkeiStatusNode.textContent += ` / diagnostic: ${status.loginDetails}`;
  }
  nikkeiReloginButton.disabled = status.loginAvailable === false;
  nikkeiOtpPanel.hidden = !status.otpPending;
}

async function refreshNikkeiStatus() {
  const response = await fetch("/api/auth/nikkei/status");
  const status = await response.json();
  renderNikkeiStatus(status);
}

async function forceNikkeiLogin() {
  nikkeiReloginButton.disabled = true;
  nikkeiReloginButton.textContent = "再ログイン中...";

  try {
    const response = await fetch("/api/auth/nikkei/login", { method: "POST" });
    const data = await response.json();
    if (data.otpRequired) {
      renderNikkeiStatus(data.status);
      statusNode.textContent = "ワンタイムパスワードをメールで確認して入力してください。";
      nikkeiOtpCodeNode.focus();
      return;
    }
    if (!response.ok) {
      throw new Error(data.details || data.error || "login failed");
    }

    renderNikkeiStatus(data.status);
    statusNode.textContent = "日経グループサイト向けのセッションを更新しました。";
  } catch (error) {
    statusNode.textContent = `日経グループサイト再ログインに失敗しました: ${error.message}`;
  } finally {
    nikkeiReloginButton.disabled = false;
    nikkeiReloginButton.textContent = "Playwrightで再ログイン";
  }
}

async function submitNikkeiOtp() {
  nikkeiOtpSubmitButton.disabled = true;

  try {
    const response = await fetch("/api/auth/nikkei/otp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: nikkeiOtpCodeNode.value })
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.details || data.error || "otp failed");
    }

    renderNikkeiStatus(data.status);
    nikkeiOtpCodeNode.value = "";
    statusNode.textContent = "ワンタイムパスワード認証が完了しました。";
  } catch (error) {
    statusNode.textContent = `ワンタイムパスワード認証に失敗しました: ${error.message}`;
  } finally {
    nikkeiOtpSubmitButton.disabled = false;
  }
}

async function bootstrap() {
  const response = await fetch("/api/options");
  const data = await response.json();

  data.topics.forEach((topic) => {
    renderOption(topicOptions, topic, "topicIds", true);

    const option = document.createElement("option");
    option.value = topic.id;
    option.textContent = topic.label;
    importTopicNode.append(option);
  });

  data.viewpoints.forEach((viewpoint) => {
    renderOption(viewpointOptions, viewpoint, "viewpointIds", true, viewpoint.hint);
  });

  if (data.authenticatedSourceStatus?.sessionUsable) {
    statusNode.textContent = "日経グループサイトの保存済みセッションを利用できます。";
  }

  renderNikkeiStatus(data.authenticatedSourceStatus);
  buildBookmarklet();
  await refreshImports();
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const topicIds = collectChecked("topicIds");
  const viewpointIds = collectChecked("viewpointIds");
  const outputFormats = collectChecked("outputFormats");

  statusNode.textContent = "ニュースを生成中です。少しだけお待ちください...";
  submitButton.disabled = true;

  try {
    const response = await fetch("/api/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ topicIds, viewpointIds, outputFormats })
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || data.details || "Unknown error");
    }

    resultsNode.classList.remove("hidden");
    summaryNode.textContent = data.summary;
    renderArticles(data.articles);
    renderDownloads(data.files);
    statusNode.textContent = `${data.articleCount}件の記事をもとに要約を生成しました。取り込み済み記事 ${data.importedArticleCount} 件を含みます。`;
    await refreshImports();
    await refreshNikkeiStatus();
  } catch (error) {
    statusNode.textContent = `生成に失敗しました: ${error.message}`;
  } finally {
    submitButton.disabled = false;
  }
});

importTopicNode.addEventListener("change", buildBookmarklet);
nikkeiReloginButton.addEventListener("click", forceNikkeiLogin);
nikkeiOtpSubmitButton.addEventListener("click", submitNikkeiOtp);

bootstrap().catch((error) => {
  statusNode.textContent = `初期化に失敗しました: ${error.message}`;
});
