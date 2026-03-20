export const TOPIC_DEFINITIONS = [
  {
    id: "ai",
    label: "AI",
    queries: ["AI", "generative AI", "AI agent", "LLM enterprise"],
    trustedDomains: [
      "openai.com",
      "anthropic.com",
      "deepmind.google",
      "microsoft.com",
      "aws.amazon.com",
      "infoq.com",
      "xtech.nikkei.com",
      "nikkei.com",
      "financial.nikkei.com",
      "bizgate.nikkei.com"
    ],
    curatedFeeds: []
  },
  {
    id: "sap",
    label: "SAP",
    queries: ["SAP", "SAP S/4HANA", "SAP BTP", "Rise with SAP", "Clean Core SAP"],
    trustedDomains: [
      "news.sap.com",
      "community.sap.com",
      "blogs.sap.com",
      "xtech.nikkei.com",
      "nikkei.com",
      "financial.nikkei.com",
      "bizgate.nikkei.com",
      "cio.de",
      "computerweekly.com"
    ],
    curatedFeeds: [
      { label: "SAP News Center", url: "https://news.sap.com/feed/", trust: 5, maxAgeDays: 45 },
      { label: "SAP News Center Press Releases", url: "https://news.sap.com/germany/type/pressemitteilung/feed/", trust: 5, maxAgeDays: 45 },
      { label: "SAP Community ERP Q&A", url: "https://community.sap.com/khhcw49343/rss/board?board.id=erp-questions", trust: 2, maxAgeDays: 14 },
      {
        label: "SAP Community Enterprise Architecture Knowledge Base",
        url: "https://community.sap.com/khhcw49343/rss/board?board.id=Enterprise-Architecturetkb-board",
        trust: 4,
        maxAgeDays: 30
      }
    ]
  },
  {
    id: "core-systems",
    label: "基幹システム",
    queries: ["基幹システム", "ERP modernization", "legacy modernization", "mission critical system", "ERP migration"],
    trustedDomains: [
      "xtech.nikkei.com",
      "nikkei.com",
      "financial.nikkei.com",
      "bizgate.nikkei.com",
      "cio.de",
      "computerweekly.com",
      "infoq.com",
      "oracle.com",
      "microsoft.com",
      "aws.amazon.com"
    ],
    curatedFeeds: [
      {
        label: "SAP Community Enterprise Architecture Knowledge Base",
        url: "https://community.sap.com/khhcw49343/rss/board?board.id=Enterprise-Architecturetkb-board",
        trust: 4,
        maxAgeDays: 30
      }
    ]
  }
];

export const VIEWPOINT_DEFINITIONS = [
  {
    id: "technology",
    label: "技術動向",
    hint: "新技術、アーキテクチャ、セキュリティ、製品アップデートを重視"
  },
  {
    id: "business",
    label: "ビジネス動向",
    hint: "市場、投資、提携、M&A、企業戦略や業績を重視"
  },
  {
    id: "cases",
    label: "企業事例",
    hint: "導入事例、PoC、本番展開、移行プロジェクトの実例を重視"
  },
  {
    id: "learning",
    label: "学習コンテンツ",
    hint: "解説記事、チュートリアル、勉強会、資格学習を重視"
  }
];

export const AUTHENTICATED_DOMAIN_SETTINGS = {
  "xtech.nikkei.com": {
    label: "Nikkei xTECH",
    cookieEnvKey: "NIKKEI_XTECH_COOKIE",
    trustBonus: 5
  },
  "nikkei.com": {
    label: "日本経済新聞",
    cookieEnvKey: "NIKKEI_COOKIE",
    trustBonus: 4
  },
  "financial.nikkei.com": {
    label: "NIKKEI Financial",
    cookieEnvKey: "NIKKEI_COOKIE",
    trustBonus: 4
  },
  "bizgate.nikkei.com": {
    label: "NIKKEI BizGate",
    cookieEnvKey: "NIKKEI_COOKIE",
    trustBonus: 4
  }
};

export const DEFAULT_LANGUAGE = "ja";
