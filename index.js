// Friendle Discord Bot with metrics map & name mapping
// ----------------------------------------------------
// This Node.js Discord bot builds daily puzzles and POSTs them to a Cloudflare Worker.
// It now also computes a name map and metrics map per guild and sends them to
// a /metadata endpoint on the Worker for frontend validation.
//
// Features:
// - Slash commands: /optin, /optout, /force_generate (admin only), /play
// - Scheduled daily job (CRON) to generate puzzles
// - Message scraping from yesterday across guild text channels
// - Building payloads for 4 mini-games: friendle_daily, quotele, mediale, statle
// - Adds solution_user_name and solution_metrics to puzzles
// - Computes nameMap (userID → displayName) and metricsMap (per-user activity profile)
// - Sends nameMap and metricsMap to Worker (/metadata endpoint)
// - POSTs puzzles to Worker with HMAC-SHA256 signature header

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import axios from 'axios';
import cron from 'node-cron';
import dotenv from 'dotenv';
import { Client, GatewayIntentBits, Partials, Routes } from 'discord.js';
import { REST } from '@discordjs/rest';

dotenv.config();

// ------------------------------------------------------------------
// Configuration
// ------------------------------------------------------------------
const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const CLIENT_ID = process.env.CLIENT_ID;
const GUILD_ID = process.env.GUILD_ID || null; // optional for testing
const WEBSITE_ENDPOINT = process.env.WEBSITE_ENDPOINT; // e.g. https://friendle-api.example.workers.dev
const FRONTEND_URL = process.env.FRONTEND_URL || process.env.WEBSITE_URL || null; // e.g. https://friendle.example.com
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || 'change-me';
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 1 * * *'; // default 01:00 UTC
const MIN_QUOTE_LENGTH = Number(process.env.MIN_QUOTE_LENGTH) || 40;

if (!DISCORD_TOKEN || !WEBSITE_ENDPOINT) {
  console.error('Please set DISCORD_TOKEN and WEBSITE_ENDPOINT in your environment.');
  process.exit(1);
}
if (!FRONTEND_URL) {
  console.warn('FRONTEND_URL is not set. /play will not be available.');
}

// ------------------------------------------------------------------
// Simple storage (opt-in users & last-run timestamp)
// ------------------------------------------------------------------
const STORAGE_PATH = path.join(process.cwd(), 'bot_storage.json');
let storage = { optInUsers: [], lastRun: null, lastRunByGuild: {} };
if (fs.existsSync(STORAGE_PATH)) {
  try {
    storage = JSON.parse(fs.readFileSync(STORAGE_PATH, 'utf8'));
    if (!storage.lastRunByGuild) storage.lastRunByGuild = {};
    if (!storage.optInUsers) storage.optInUsers = [];
  } catch (e) {
    console.warn('Could not parse storage file, using defaults.');
  }
}
function saveStorage() {
  fs.writeFileSync(STORAGE_PATH, JSON.stringify(storage, null, 2));
}

// ------------------------------------------------------------------
// Discord client setup
// ------------------------------------------------------------------
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers
  ],
  partials: [Partials.Channel, Partials.Message, Partials.Reaction]
});

// Register slash commands (only once on startup)
const commands = [
  {
    name: 'play',
    description: 'Get a Friendle play link for this server'
  },
  { name: 'optin', description: 'Opt in to Friendle puzzles (allow your public activity to be used)' },
  { name: 'optout', description: 'Opt out of Friendle puzzles' },
  { name: 'force_generate', description: 'Force puzzle generation now (admin only)' }
];

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
  try {
    if (GUILD_ID) {
      console.log('Registering commands to guild', GUILD_ID);
      await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), { body: commands });
    } else {
      console.log('Registering global commands (may take up to an hour)');
      await rest.put(Routes.applicationCommands(CLIENT_ID), { body: commands });
    }
  } catch (err) {
    console.error('Error registering commands', err);
  }
}

// ------------------------------------------------------------------
// Utility functions
// ------------------------------------------------------------------
function hmacSign(payloadJson) {
  return crypto.createHmac('sha256', WEBHOOK_SECRET).update(payloadJson).digest('hex');
}

function anonymizeText(text) {
  return text
    .replace(/<@!?\d+>/g, '[mention]')
    .replace(/@\w+/g, '[mention]')
    .replace(/\s+/g, ' ')
    .trim();
}

function scrambleWords(text) {
  const words = text.split(/(\s+)/).filter(Boolean);
  const pureWords = words.filter(w => !/\s+/.test(w));
  for (let i = pureWords.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [pureWords[i], pureWords[j]] = [pureWords[j], pureWords[i]];
  }
  return pureWords.join(' ');
}

function bucketTime(date) {
  const hour = date.getUTCHours();
  if (hour >= 5 && hour < 12) return 'Morning';
  if (hour >= 12 && hour < 17) return 'Afternoon';
  if (hour >= 17 && hour < 22) return 'Evening';
  return 'Night';
}

function accountAgeRange(createdAt) {
  const years = (Date.now() - createdAt.getTime()) / (1000 * 60 * 60 * 24 * 365);
  if (years < 1) return 'Less than 1 year';
  if (years < 2) return '1–2 years';
  if (years < 4) return '2–4 years';
  return '4+ years';
}

function topNonCommonWord(messages) {
  const stop = new Set([
    'the','be','to','of','and','a','in','that','have','i','it','for','not','on','with','he','as','you','do','at',
    'this','but','his','by','from','they','we','say','her','she','or','an','will','my','one','all','would','there',
    'their','what','so','up','out','if','about','who','get','which','go','me','when','make','can','like','time','no',
    'just','him','know','take','people','into','year','your','good','some','could','them','see','other','than','then',
    'now','look','only','come','its','over','think','also','back','after','use','two','how','our','work','first','well',
    'way','even','new','want','because','any','these','give','day','most','us','lol','yeah','yay','nah','nahhh','hello','hi'
  ]);
  const freq = {};
  for (const m of messages) {
    const words = m.content.toLowerCase().replace(/[^a-z0-9' ]+/g, ' ').split(/\s+/).filter(Boolean);
    for (const w of words) {
      if (!stop.has(w) && w.length > 2) freq[w] = (freq[w] || 0) + 1;
    }
  }
  const entries = Object.entries(freq).sort((a, b) => b[1] - a[1]);
  return entries[0] ? entries[0][0] : null;
}

function getDisplayName(member) {
  // Use nickname, globalName, or username (prioritised)
  return member.nickname || member.user.globalName || member.user.username;
}

function buildPlayUrl(guildId) {
  if (!FRONTEND_URL) return null;
  try {
    const url = new URL(FRONTEND_URL);
    let base = url.origin + url.pathname;
    if (!base.endsWith('/')) base += '/';
    const params = new URLSearchParams({ guild: guildId });
    return `${base}#/play?${params.toString()}`;
  } catch (err) {
    return null;
  }
}

function getDayRangeUtc(offsetDays = 0) {
  const now = new Date();
  const todayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
  const start = new Date(todayStart.getTime() + offsetDays * 24 * 60 * 60 * 1000);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000 - 1);
  const dateLabel = start.toISOString().slice(0, 10);
  return { start, end, dateLabel };
}

// ------------------------------------------------------------------
// Message scraping (same as before)
// ------------------------------------------------------------------
async function fetchMessagesInRange(channel, startDate, endDate) {
  if (!channel || !channel.isTextBased()) return [];
  const msgs = [];
  let lastId = null;
  const startTs = startDate.getTime();
  const endTs = endDate.getTime();
  try {
    let options = { limit: 100 };
    let fetching = true;
    while (fetching) {
      if (lastId) options.before = lastId;
      const fetched = await channel.messages.fetch(options);
      if (!fetched || fetched.size === 0) break;
      for (const m of fetched.values()) {
        const t = m.createdTimestamp;
        if (t < startTs) {
          fetching = false;
          break;
        }
        if (t <= endTs && t >= startTs) msgs.push(m);
      }
      lastId = fetched.last().id;
      if (fetched.size < 100) break;
      await new Promise(r => setTimeout(r, 300));
    }
  } catch (err) {
    // ignore channels the bot can't read
  }
  return msgs;
}

async function collectMessagesForRange(channels, start, end) {
  const allMessages = [];
  const messagesByUser = new Map();

  for (const [, channel] of channels) {
    const msgs = await fetchMessagesInRange(channel, start, end);
    if (!msgs || msgs.length === 0) continue;

    allMessages.push(...msgs);
    for (const m of msgs) {
      if (!messagesByUser.has(m.author.id)) messagesByUser.set(m.author.id, []);
      messagesByUser.get(m.author.id).push(m);
    }

    await new Promise(r => setTimeout(r, 150));
  }

  return { allMessages, messagesByUser };
}

async function collectRecentMessages(channels, perChannelLimit = 50, totalLimit = 500) {
  const allMessages = [];

  for (const [, channel] of channels) {
    try {
      const fetched = await channel.messages.fetch({ limit: perChannelLimit });
      if (!fetched || fetched.size === 0) continue;
      allMessages.push(...fetched.values());
      await new Promise(r => setTimeout(r, 150));
    } catch (err) {
      // ignore channels the bot can't read
    }
  }

  allMessages.sort((a, b) => b.createdTimestamp - a.createdTimestamp);
  const trimmed = totalLimit ? allMessages.slice(0, totalLimit) : allMessages;
  const messagesByUser = new Map();
  for (const m of trimmed) {
    if (!messagesByUser.has(m.author.id)) messagesByUser.set(m.author.id, []);
    messagesByUser.get(m.author.id).push(m);
  }

  const dateLabel = trimmed.length
    ? new Date(trimmed[0].createdTimestamp).toISOString().slice(0, 10)
    : null;

  return { allMessages: trimmed, messagesByUser, dateLabel };
}

// ------------------------------------------------------------------
// Puzzle builders
// ------------------------------------------------------------------
async function buildFriendlePayload(guild, messagesByUser, membersMap, dateLabel) {
  const candidateEntries = Array.from(messagesByUser.entries())
    .filter(([uid, msgs]) => storage.optInUsers.includes(uid) && msgs.length > 0);
  if (candidateEntries.length === 0) return null;
  const [userId, msgs] = candidateEntries[Math.floor(Math.random() * candidateEntries.length)];
  const member = membersMap.get(userId);
  const messageCount = msgs.length;
  const topWord = topNonCommonWord(msgs) || null;
  const hours = msgs.map(m => new Date(m.createdTimestamp).getUTCHours());
  const minHour = Math.min(...hours);
  const maxHour = Math.max(...hours);
  const activeWindow = `${bucketTime(new Date(minHour * 3600 * 1000))} — ${bucketTime(new Date(maxHour * 3600 * 1000))}`;
  const mentions = msgs.reduce((acc, m) => acc + (m.mentions?.users?.size || 0), 0);
  const firstMsg = msgs.reduce((a, b) => (a.createdTimestamp < b.createdTimestamp ? a : b));
  return {
    game: 'friendle_daily',
    date: dateLabel,
    solution_user_id: userId,
    clues: {
      messages_yesterday: `${Math.max(1, messageCount)} messages`,
      top_word: topWord,
      active_window: activeWindow,
      mentions: mentions,
      first_message_bucket: bucketTime(new Date(firstMsg.createdTimestamp)),
      account_age_range: member ? accountAgeRange(new Date(member.user.createdTimestamp)) : null
    }
  };
}

function pickRandomMessage(messages) {
  if (!messages || messages.length === 0) return null;
  return messages[Math.floor(Math.random() * messages.length)];
}

async function buildQuotelePayload(guild, allMessages, dateLabel) {
  const longMsgs = allMessages
    .filter(m => m.content && m.content.length >= MIN_QUOTE_LENGTH)
    // reject messages that look like URLs
    .filter(m => !/(https?:\/\/\S+|www\.\S+)/i.test(m.content));

  if (longMsgs.length === 0) return null;

  const msg = pickRandomMessage(longMsgs);

  const originalNorm = normalizeQuoteForHash(msg.content);
  if (!originalNorm || originalNorm.length < 10) return null;

  const scrambled = scrambleWords(anonymizeText(msg.content));

  return {
    game: 'quotele',
    date: dateLabel,
    solution_user_id: msg.author.id,

    // show only scrambled quote
    quote_scrambled: scrambled,

    // store hash so frontend can validate typed quote
    quote_hash: sha256Hex(originalNorm),

    meta: {
      message_span: 1,
      time_bucket: bucketTime(new Date(msg.createdTimestamp)),
      channel_category: msg.channel.parent ? msg.channel.parent.name : null,
      // Optional: length bucket to prevent brute forcing short quotes
      min_chars: originalNorm.length
    }
  };
}

function normalizeQuoteForHash(text) {
  return anonymizeText(text)
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\p{L}\p{N}\s']/gu, '')  // keep letters/numbers/spaces/apostrophe
    .trim();
}

// Node built-in crypto SHA-256 hex
function sha256Hex(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

async function buildMedialePayload(guild, allMessages, dateLabel) {
  const mediaMsgs = allMessages
    .filter(m => m.attachments && m.attachments.size > 0)
    .filter(m => {
      const a = m.attachments.first();
      if (!a) return false;
      const ext = a.name ? a.name.split('.').pop().toLowerCase() : '';
      return ['png', 'jpg', 'jpeg', 'gif', 'webp'].includes(ext) || (a.contentType && a.contentType.startsWith('image'));
    });
  if (mediaMsgs.length === 0) return null;
  const msg = pickRandomMessage(mediaMsgs);
  const attachment = msg.attachments.first();
  return {
    game: 'mediale',
    date: dateLabel,
    solution_user_id: msg.author.id,
    media: {
      url: attachment.url,
      file_name: attachment.name || null,
      size: attachment.size || null,
      // you can add keywords now if you wish:
      keywords: (attachment.name || '')
        .split(/[._-]/)
        .filter(part => part && /^[a-zA-Z]{2,}/.test(part))
        .map(part => part.toLowerCase())
    },
    meta: {
      time_bucket: bucketTime(new Date(msg.createdTimestamp)),
      channel_category: msg.channel.parent ? msg.channel.parent.name : null
    }
  };
}

async function buildStatlePayload(guild, allMessages, dateLabel) {
  if (allMessages.length === 0) return null;
  const byUser = new Map();
  for (const m of allMessages) {
    if (!byUser.has(m.author.id)) byUser.set(m.author.id, []);
    byUser.get(m.author.id).push(m);
  }
  let longest = { id: null, length: 0 };
  for (const m of allMessages) {
    if (m.content && m.content.length > longest.length) {
      longest = { id: m.author.id, length: m.content.length };
    }
  }
  const wordUserMap = {};
  for (const [uid, msgs] of byUser) {
    for (const m of msgs) {
      const words = m.content
        ? m.content.toLowerCase().replace(/[^a-z0-9' ]+/g, ' ').split(/\s+/).filter(Boolean)
        : [];
      for (const w of words) {
        if (!wordUserMap[w]) wordUserMap[w] = new Set();
        wordUserMap[w].add(uid);
      }
    }
  }
  const rareWords = Object.entries(wordUserMap)
    .filter(([w, set]) => set.size === 1 && w.length > 3 && !w.match(/\d+/))
    .map(([w]) => w);
  const rareWord = rareWords.length ? rareWords[Math.floor(Math.random() * rareWords.length)] : null;
  let chosenUser = null;
  let stats = {};
  if (rareWord) {
    const userSet = Array.from(wordUserMap[rareWord]);
    chosenUser = userSet[0];
    stats = {
      unique_word: rareWord,
      messages: byUser.get(chosenUser).length,
      reactions_received: byUser.get(chosenUser).reduce((a, m) => a + (m.reactions?.cache?.size || 0), 0)
    };
  } else if (longest.id) {
    chosenUser = longest.id;
    stats = {
      messages: byUser.get(chosenUser).length,
      longest_message_length: longest.length,
      reactions_received: byUser.get(chosenUser).reduce((a, m) => a + (m.reactions?.cache?.size || 0), 0)
    };
  } else {
    const arr = Array.from(byUser.keys());
    chosenUser = arr[Math.floor(Math.random() * arr.length)];
    stats = { messages: byUser.get(chosenUser).length };
  }
  return {
    game: 'statle',
    date: dateLabel,
    solution_user_id: chosenUser,
    stats
  };
}

// ------------------------------------------------------------------
// Main generation flow with metadata posting
// ------------------------------------------------------------------
async function generateDailyPuzzles() {
  console.log('Starting daily puzzle generation...');

  for (const [guildId, guild] of client.guilds.cache) {
    try {
      console.log('Processing guild', guildId);
      await generatePuzzlesForGuild(guild);
    } catch (err) {
      console.error('Error processing guild', guildId, err.message);
    }
  }

  storage.lastRun = new Date().toISOString();
  saveStorage();
  console.log('Daily generation finished.');
}

// ------------------------------------------------------------------
// POST puzzles to Worker
// ------------------------------------------------------------------
async function postPuzzleToWebsite(guildId, puzzle) {
  try {
    const payload = { guild_id: guildId, puzzle };
    const payloadJson = JSON.stringify(payload);
    const signature = hmacSign(payloadJson);
    const res = await axios.post(`${WEBSITE_ENDPOINT}/ingest`, payloadJson, {
      headers: {
        'X-Signature': signature,
        'Content-Type': 'application/json'
      },
      timeout: 10000,
      transformRequest: [data => data],
      validateStatus: () => true
    });
    if (res.status >= 200 && res.status < 300) {
      console.log('Posted puzzle', puzzle.game);
    } else {
      console.error('Worker rejected puzzle', puzzle.game, res.status, res.data);
    }
  } catch (err) {
    console.error('Failed posting puzzle', puzzle.game, err.message);
  }
}

async function generatePuzzlesForGuild(guild) {
  const guildId = guild.id;
  await guild.members.fetch();
  const membersMap = new Map(guild.members.cache.map(m => [m.user.id, m]));

  const optedInMembers = storage.optInUsers.filter(uid => membersMap.has(uid));
  if (optedInMembers.length === 0) {
    console.log('No opted-in members for guild', guildId);
    return { puzzles: [], generated: false, reason: 'no_opted_in' };
  }

  const channels = guild.channels.cache.filter(
    c => c.isTextBased() && c.viewable && !c.nsfw && c.type === 0 // 0 = GUILD_TEXT
  );

  const yesterdayRange = getDayRangeUtc(-1);
  const todayRange = getDayRangeUtc(0);
  const ranges = [yesterdayRange, todayRange];

  let allMessages = [];
  let messagesByUser = new Map();
  let dateLabel = yesterdayRange.dateLabel;

  for (const range of ranges) {
    const result = await collectMessagesForRange(channels, range.start, range.end);
    if (result.allMessages.length > 0) {
      allMessages = result.allMessages;
      messagesByUser = result.messagesByUser;
      dateLabel = range.dateLabel;
      break;
    }
  }

  if (allMessages.length === 0) {
    const fallback = await collectRecentMessages(channels);
    allMessages = fallback.allMessages;
    messagesByUser = fallback.messagesByUser;
    if (fallback.dateLabel) dateLabel = fallback.dateLabel;
  }

  if (allMessages.length === 0) {
    console.log('No recent messages available for guild', guildId);
    return { puzzles: [], generated: false, reason: 'no_messages' };
  }

  // Compute nameMap and metricsMap for this guild/date
  const nameMap = {};
  const metricsMap = {};
  for (const [uid, member] of membersMap) {
    nameMap[uid] = getDisplayName(member);
    const msgs = messagesByUser.get(uid) || [];
    const messageCount = msgs.length;
    let activeWindow, mentions, firstMessageBucket;
    if (messageCount > 0) {
      const hours = msgs.map(m => new Date(m.createdTimestamp).getUTCHours());
      const minHour = Math.min(...hours);
      const maxHour = Math.max(...hours);
      const bucketRange = `${bucketTime(new Date(minHour * 3600 * 1000))} — ${bucketTime(new Date(maxHour * 3600 * 1000))}`;
      activeWindow = bucketRange;
      mentions = msgs.reduce((acc, m) => acc + (m.mentions?.users?.size || 0), 0);
      const firstMsg = msgs.reduce((a, b) => (a.createdTimestamp < b.createdTimestamp ? a : b));
      firstMessageBucket = bucketTime(new Date(firstMsg.createdTimestamp));
    } else {
      activeWindow = 'Not active';
      mentions = 0;
      firstMessageBucket = null;
    }
    const topWord = messageCount > 0 ? (topNonCommonWord(msgs) || null) : null;
    const ageStr = member ? accountAgeRange(new Date(member.user.createdTimestamp)) : null;
    metricsMap[uid] = {
      messageCount,
      topWord,
      activeWindow,
      mentions,
      firstMessageBucket,
      accountAgeRange: ageStr
    };
  }

  // Build puzzles
  const friendle = await buildFriendlePayload(guild, messagesByUser, membersMap, dateLabel);
  const quotele = await buildQuotelePayload(guild, allMessages, dateLabel);
  const mediale = await buildMedialePayload(guild, allMessages, dateLabel);
  const statle = await buildStatlePayload(guild, allMessages, dateLabel);

  // Attach solution display names and metrics
  if (friendle) {
    friendle.solution_user_name = nameMap[friendle.solution_user_id] || null;
    friendle.solution_metrics = metricsMap[friendle.solution_user_id] || null;
  }
  if (quotele) quotele.solution_user_name = nameMap[quotele.solution_user_id] || null;
  if (mediale) mediale.solution_user_name = nameMap[mediale.solution_user_id] || null;
  if (statle) statle.solution_user_name = nameMap[statle.solution_user_id] || null;

  // Post puzzles individually
  const puzzles = [friendle, quotele, mediale, statle].filter(Boolean);
  for (const p of puzzles) {
    await postPuzzleToWebsite(guildId, p);
  }

  // Post metadata (names and metrics) once per guild/day
  const metadataPayload = {
    guild_id: guildId,
    date: dateLabel,
    names: nameMap,
    metrics: metricsMap
  };
  const metadataJson = JSON.stringify(metadataPayload);
  const metadataSig = hmacSign(metadataJson);
  await axios.post(`${WEBSITE_ENDPOINT}/metadata`, metadataJson, {
    headers: {
      'X-Signature': metadataSig,
      'Content-Type': 'application/json'
    },
    timeout: 10000,
    transformRequest: [data => data],
    validateStatus: () => true
  });

  storage.lastRunByGuild[guildId] = dateLabel;
  saveStorage();

  console.log(`Puzzles for ${guildId}:`, puzzles.map(p => p.game));
  return { puzzles, generated: true };
}

function getRecentDateLabelsUtc() {
  const yesterday = getDayRangeUtc(-1).dateLabel;
  const today = getDayRangeUtc(0).dateLabel;
  return { yesterday, today };
}

// ------------------------------------------------------------------
// Interaction handlers
// ------------------------------------------------------------------
client.on('interactionCreate', async interaction => {
  try {
    if (!interaction.isChatInputCommand()) return;
    const { commandName } = interaction;
    if (commandName === 'optin') {
      const uid = interaction.user.id;
      if (!storage.optInUsers.includes(uid)) storage.optInUsers.push(uid);
      saveStorage();
      await interaction.reply({
        content: 'You are now opted in to Friendle puzzles. You can opt out with /optout',
        ephemeral: true
      });
    } else if (commandName === 'play') {
      if (!interaction.guildId) {
        await interaction.reply({
          content: 'This command can only be used inside a server.',
          ephemeral: true
        });
        return;
      }
      const { yesterday, today } = getRecentDateLabelsUtc();
      const lastGenerated = storage.lastRunByGuild?.[interaction.guildId] || null;
      if (lastGenerated !== yesterday && lastGenerated !== today) {
        const guild = client.guilds.cache.get(interaction.guildId);
        if (!guild) {
          await interaction.reply({
            content: 'Could not find this server in the bot cache. Please try again in a moment.',
            ephemeral: true
          });
          return;
        }
        await interaction.deferReply();
        const result = await generatePuzzlesForGuild(guild);
        if (result.reason === 'no_opted_in') {
          await interaction.editReply('No one in this server has opted in yet. Use /optin to get started.');
          return;
        }
      }
      const playUrl = buildPlayUrl(interaction.guildId);
      if (!playUrl) {
        await interaction.reply({
          content: 'Play link is not configured. Ask the admin to set FRONTEND_URL for the bot.',
          ephemeral: true
        });
        return;
      }
      const response = `Play Friendle for this server: ${playUrl}`;
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply(response);
      } else {
        await interaction.reply({ content: response });
      }
    } else if (commandName === 'optout') {
      const uid = interaction.user.id;
      storage.optInUsers = storage.optInUsers.filter(u => u !== uid);
      saveStorage();
      await interaction.reply({
        content: 'You are now opted out of Friendle puzzles.',
        ephemeral: true
      });
    } else if (commandName === 'force_generate') {
      if (!interaction.memberPermissions || !interaction.memberPermissions.has('ManageGuild')) {
        await interaction.reply({
          content: 'You must be a guild admin to use this.',
          ephemeral: true
        });
        return;
      }
      await interaction.reply({ content: 'Forcing generation now...', ephemeral: true });
      await generateDailyPuzzles();
    }
  } catch (err) {
    console.error('interaction handler error', err);
  }
});

// ------------------------------------------------------------------
// Startup
// ------------------------------------------------------------------
client.once('ready', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await registerCommands();
  cron.schedule(CRON_SCHEDULE, async () => {
    console.log('CRON triggered at', new Date().toISOString());
    await generateDailyPuzzles();
  });
  console.log('Scheduled daily generation:', CRON_SCHEDULE);
});

client.login(DISCORD_TOKEN);

// Expose manual trigger when running directly with node index.js --run-now
if (process.argv.includes('--run-now')) {
  client.on('ready', () => {
    generateDailyPuzzles();
  });
}
