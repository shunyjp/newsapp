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
    ["text", "繝・く繧ｹ繝医ｒ髢九￥"],
    ["word", "Word繧帝幕縺・],
    ["audio", "髻ｳ螢ｰ繧帝幕縺・],
    ["notebooklm", "NotebookLM逕ｨ蜴滓枚繧帝幕縺・]
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
      <p>${article.contentSnippet || "讎りｦ√・蜿門ｾ励〒縺阪∪縺帙ｓ縺ｧ縺励◆縲・}</p>
      <p>${article.source || "Source unknown"} / ${article.pubDate || "Date unknown"} / trust ${article.trustScore ?? 0}</p>
      <a href="${article.link}" target="_blank" rel="noreferrer">險倅ｺ九ｒ隕九ｋ</a>
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
    empty.textContent = "縺ｾ縺蜿悶ｊ霎ｼ縺ｿ貂医∩險倅ｺ九・縺ゅｊ縺ｾ縺帙ｓ縲・;
    importsNode.append(empty);
    return;
  }

  articles.forEach((article) => {
    const card = document.createElement("article");
    card.className = "article-card";
    card.innerHTML = `
      <p class="eyebrow">${article.topicLabel} / imported</p>
      <h3>${article.title}</h3>
      <p>${article.contentSnippet || "譛ｬ譁・・縺ゅｊ縺ｾ縺帙ｓ縲・}</p>
      <p>${article.source || "Source unknown"} / ${article.importedAt || article.pubDate || "Date unknown"}</p>
      <div class="card-actions">
        <a href="${article.link}" target="_blank" rel="noreferrer">險倅ｺ九ｒ髢九￥</a>
        <button type="button" class="danger-button" data-delete-link="${article.link}">縺薙・蜿悶ｊ霎ｼ縺ｿ險倅ｺ九ｒ蜑企勁</button>
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
        statusNode.textContent = "蜿悶ｊ霎ｼ縺ｿ險倅ｺ九ｒ蜑企勁縺励∪縺励◆縲・;
        await refreshImports();
      } catch (error) {
        statusNode.textContent = `蜑企勁縺ｫ螟ｱ謨励＠縺ｾ縺励◆: ${error.message}`;
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
    ".then(async(r)=>{const data=await r.json();if(!r.ok)throw new Error(data.error||'import failed');alert('險倅ｺ九ｒ蜿悶ｊ霎ｼ縺ｿ縺ｾ縺励◆');})" +
    ".catch((e)=>alert('蜿悶ｊ霎ｼ縺ｿ縺ｫ螟ｱ謨励＠縺ｾ縺励◆: '+e.message));})();";

  bookmarkletLink.href = `javascript:${script}`;
}

function renderNikkeiStatus(status) {
  const browser = status.browserExecutablePath
    ? `繝悶Λ繧ｦ繧ｶ: ${status.browserExecutablePath}`
    : "繝悶Λ繧ｦ繧ｶ: 譛ｪ讀懷・";
  const cookieDomains = status.cookieDomains
    ? `cookie: 譌･邨梧眠閨・${status.cookieDomains.nikkei || 0} / xTECH ${status.cookieDomains.xtech || 0}`
    : null;
  const targets = Array.isArray(status.targetUrls) && status.targetUrls.length
    ? `蟇ｾ雎｡: ${status.targetUrls.join(" , ")}`
    : null;

  const lines = [
    status.hasCredentials ? "雉・ｼ諠・ｱ: 險ｭ螳壽ｸ医∩" : "雉・ｼ諠・ｱ: 譛ｪ險ｭ螳・,
    status.hasSavedSession ? "菫晏ｭ俶ｸ医∩繧ｻ繝・す繝ｧ繝ｳ: 縺ゅｊ" : "菫晏ｭ俶ｸ医∩繧ｻ繝・す繝ｧ繝ｳ: 縺ｪ縺・,
    status.sessionUsable
      ? `繧ｻ繝・す繝ｧ繝ｳ迥ｶ諷・ 蛻ｩ逕ｨ蜿ｯ閭ｽ (${status.savedCookieCount} cookies)`
      : "繧ｻ繝・す繝ｧ繝ｳ迥ｶ諷・ 蛻ｩ逕ｨ荳榊庄",
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
  nikkeiReloginButton.textContent = "蜀阪Ο繧ｰ繧､繝ｳ荳ｭ...";

  try {
    const response = await fetch("/api/auth/nikkei/login", { method: "POST" });
    const data = await response.json();
    if (data.otpRequired) {
      renderNikkeiStatus(data.status);
      statusNode.textContent = "繝ｯ繝ｳ繧ｿ繧､繝繝代せ繝ｯ繝ｼ繝峨ｒ繝｡繝ｼ繝ｫ縺ｧ遒ｺ隱阪＠縺ｦ蜈･蜉帙＠縺ｦ縺上□縺輔＞縲・;
      nikkeiOtpCodeNode.focus();
      return;
    }
    if (!response.ok) {
      throw new Error(data.details || data.error || "login failed");
    }

    renderNikkeiStatus(data.status);
    statusNode.textContent = "譌･邨後げ繝ｫ繝ｼ繝励し繧､繝亥髄縺代・繧ｻ繝・す繝ｧ繝ｳ繧呈峩譁ｰ縺励∪縺励◆縲・;
  } catch (error) {
    statusNode.textContent = `譌･邨後げ繝ｫ繝ｼ繝励し繧､繝亥・繝ｭ繧ｰ繧､繝ｳ縺ｫ螟ｱ謨励＠縺ｾ縺励◆: ${error.message}`;
  } finally {
    nikkeiReloginButton.disabled = false;
    nikkeiReloginButton.textContent = "Playwright縺ｧ蜀阪Ο繧ｰ繧､繝ｳ";
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
    statusNode.textContent = "繝ｯ繝ｳ繧ｿ繧､繝繝代せ繝ｯ繝ｼ繝芽ｪ崎ｨｼ縺悟ｮ御ｺ・＠縺ｾ縺励◆縲・;
  } catch (error) {
    statusNode.textContent = `繝ｯ繝ｳ繧ｿ繧､繝繝代せ繝ｯ繝ｼ繝芽ｪ崎ｨｼ縺ｫ螟ｱ謨励＠縺ｾ縺励◆: ${error.message}`;
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
    statusNode.textContent = "譌･邨後げ繝ｫ繝ｼ繝励し繧､繝医・菫晏ｭ俶ｸ医∩繧ｻ繝・す繝ｧ繝ｳ繧貞茜逕ｨ縺ｧ縺阪∪縺吶・;
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

  statusNode.textContent = "繝九Η繝ｼ繧ｹ繧堤函謌蝉ｸｭ縺ｧ縺吶ょｰ代＠縺縺代♀蠕・■縺上□縺輔＞...";
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
    statusNode.textContent = `${data.articleCount}莉ｶ縺ｮ險倅ｺ九ｒ繧ゅ→縺ｫ隕∫ｴ・ｒ逕滓・縺励∪縺励◆縲ょ叙繧願ｾｼ縺ｿ貂医∩險倅ｺ・${data.importedArticleCount} 莉ｶ繧貞性縺ｿ縺ｾ縺吶Ａ;
    await refreshImports();
    await refreshNikkeiStatus();
  } catch (error) {
    statusNode.textContent = `逕滓・縺ｫ螟ｱ謨励＠縺ｾ縺励◆: ${error.message}`;
  } finally {
    submitButton.disabled = false;
  }
});

importTopicNode.addEventListener("change", buildBookmarklet);
nikkeiReloginButton.addEventListener("click", forceNikkeiLogin);
nikkeiOtpSubmitButton.addEventListener("click", submitNikkeiOtp);

bootstrap().catch((error) => {
  statusNode.textContent = `蛻晄悄蛹悶↓螟ｱ謨励＠縺ｾ縺励◆: ${error.message}`;
});
