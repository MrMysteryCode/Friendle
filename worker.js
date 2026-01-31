/**
 * Friendle Cloudflare Worker
 * --------------------------------------------------
 * Routes:
 *   POST /ingest        store puzzle per guild/date/game (signed)
 *   POST /metadata      store nameMap and metricsMap for a guild/date (signed)
 *   GET  /puzzles       fetch puzzles + names + metrics + allowed usernames
 *   GET  /stats         fetch live-ish counters (KV) (public)
 *   POST /stats/event   increment counters (public; optionally add a simple key)
 *   GET  /health        quick status check
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname.replace(/^\/+|\/+$/g, "");

    // ---- CORS ----
    const requestOrigin = request.headers.get("Origin") || "";
    const allowedOrigin = env.ALLOWED_ORIGIN || "*";
    const originToReturn = allowedOrigin === "*" ? (requestOrigin || "*") : allowedOrigin;
    const corsHeaders = {
      "Access-Control-Allow-Origin": originToReturn,
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type,X-Signature,Authorization",
      Vary: "Origin",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    const kv = env.PUZZLES_KV;
    if (!kv) {
      return json({ error: "KV binding not found. Expected env.PUZZLES_KV." }, 500, corsHeaders);
    }

    // ------------------------------
    // GET /health
    // ------------------------------
    if (pathname === "health" && request.method === "GET") {
      const hasSecret = !!env.WEBHOOK_SECRET;
      return json(
        {
          ok: true,
          has_kv_binding: true,
          has_webhook_secret: hasSecret,
          allowed_origin: env.ALLOWED_ORIGIN || "*",
        },
        200,
        corsHeaders
      );
    }

    // ------------------------------
    // GET /stats  (Option A: KV counters)
    // ------------------------------
    // Supports:
    //   /stats                              -> global totals
    //   /stats?guild_id=123                 -> per guild totals
    //   /stats?guild_id=123&date=YYYY-MM-DD -> per guild + date
    //   /stats?latest=1&guild_id=123        -> use latest_date for guild if available
    if (pathname === "stats" && request.method === "GET") {
      const guildId = url.searchParams.get("guild_id") || "global";
      const wantsLatest = url.searchParams.get("latest") === "1";

      let date = url.searchParams.get("date") || null;
      if (wantsLatest && guildId !== "global") {
        const latest = await kv.get(`guild:${guildId}:latest_date`);
        if (latest) date = latest;
      }

      const scopeKey = date
        ? `stats:guild:${guildId}:date:${date}`
        : `stats:guild:${guildId}:total`;

      const stats = await readStats(kv, scopeKey);

      return json(
        {
          scope: date ? "guild_day" : "guild_total",
          guild_id: guildId === "global" ? null : guildId,
          date,
          ...stats,
          // Back-compat for your UI code:
          guessed_correctly: stats.guessed_correctly,
          played_all: stats.completed_games,
        },
        200,
        corsHeaders
      );
    }

    // ------------------------------
    // POST /stats/event (Option A increment)
    // ------------------------------
    // Body:
    // {
    //   "type": "view" | "guess" | "guess_correct" | "game_complete",
    //   "guild_id": "123" (optional, default "global"),
    //   "date": "YYYY-MM-DD" (optional),
    //   "latest": true (optional; if true and no date, uses guild latest_date)
    // }
    //
    // Returns updated counters.
    //
    // NOTE: This is "public" by default (anyone can call it).
    // If you want basic protection, set env.STATS_WRITE_KEY and require header Authorization: Bearer <key>.
    if (pathname === "stats/event" && request.method === "POST") {
      // Optional simple write protection:
      const writeKey = env.STATS_WRITE_KEY || "";
      if (writeKey) {
        const auth = request.headers.get("Authorization") || "";
        const expected = `Bearer ${writeKey}`;
        if (auth !== expected) {
          return json({ error: "Unauthorized" }, 401, corsHeaders);
        }
      }

      let payload;
      try {
        payload = await request.json();
      } catch {
        return json({ error: "Invalid JSON" }, 400, corsHeaders);
      }

      const rawType = String(payload.type || "").toLowerCase();
      const type = rawType === "game_completed" ? "game_complete" : rawType;
      if (!["view", "guess", "guess_correct", "game_complete"].includes(type)) {
        return json(
          { error: "Invalid type. Use view | guess | guess_correct | game_complete" },
          400,
          corsHeaders
        );
      }

      const guildId = payload.guild_id ? String(payload.guild_id) : "global";

      let date = payload.date ? String(payload.date) : null;
      const wantsLatest = payload.latest === true || payload.latest === 1 || payload.latest === "1";
      if (!date && wantsLatest && guildId !== "global") {
        const latest = await kv.get(`guild:${guildId}:latest_date`);
        if (latest) date = latest;
      }

      // We maintain BOTH:
      //   - a total scope (per guild)
      //   - a daily scope (per guild+date) if date is provided
      const totalKey = `stats:guild:${guildId}:total`;
      const dayKey = date ? `stats:guild:${guildId}:date:${date}` : null;

      await bumpStat(kv, totalKey, type);
      if (dayKey) await bumpStat(kv, dayKey, type);

      const totalStats = await readStats(kv, totalKey);
      const dayStats = dayKey ? await readStats(kv, dayKey) : null;

      return json(
        {
          ok: true,
          guild_id: guildId === "global" ? null : guildId,
          date,
          total: totalStats,
          day: dayStats,
        },
        200,
        corsHeaders
      );
    }

    // ------------------------------
    // POST /ingest (signed)
    // ------------------------------
    if (pathname === "ingest" && request.method === "POST") {
      const verified = await verifySignedJson(request, env, corsHeaders);
      if (!verified.ok) return verified.res;

      const { payload } = verified;
      const guildId = payload.guild_id;
      const puzzle = payload.puzzle;

      if (!guildId || !puzzle || !puzzle.date || !puzzle.game) {
        return json(
          { error: "Missing required fields: guild_id, puzzle.date, puzzle.game" },
          400,
          corsHeaders
        );
      }

      const key = `guild:${guildId}:date:${puzzle.date}:game:${puzzle.game}`;
      await kv.put(key, JSON.stringify(puzzle), { expirationTtl: 60 * 60 * 24 * 365 });

      await kv.put(`guild:${guildId}:latest_date`, puzzle.date);
      await kv.put(`guild:${guildId}:latest_game:${puzzle.game}`, JSON.stringify(puzzle), {
        expirationTtl: 60 * 60 * 24 * 365,
      });

      return json({ ok: true, stored: key }, 200, corsHeaders);
    }

    // ------------------------------
    // POST /metadata (signed)
    // ------------------------------
    if (pathname === "metadata" && request.method === "POST") {
      const verified = await verifySignedJson(request, env, corsHeaders);
      if (!verified.ok) return verified.res;

      const { payload } = verified;
      const guildId = payload.guild_id;
      const date = payload.date;

      if (!guildId || !date) {
        return json({ error: "Missing guild_id or date" }, 400, corsHeaders);
      }

      if (payload.names) {
        await kv.put(`guild:${guildId}:date:${date}:names`, JSON.stringify(payload.names), {
          expirationTtl: 60 * 60 * 24 * 365,
        });
      }
      if (payload.metrics) {
        await kv.put(`guild:${guildId}:date:${date}:metrics`, JSON.stringify(payload.metrics), {
          expirationTtl: 60 * 60 * 24 * 365,
        });
      }
      if (Array.isArray(payload.allowed_usernames)) {
        await kv.put(
          `guild:${guildId}:date:${date}:allowed_usernames`,
          JSON.stringify(payload.allowed_usernames.filter(Boolean)),
          { expirationTtl: 60 * 60 * 24 * 365 }
        );
      }

      return json({ ok: true }, 200, corsHeaders);
    }

    // ------------------------------
    // GET /puzzles
    // ------------------------------
    if (pathname === "puzzles" && request.method === "GET") {
      const guildId = url.searchParams.get("guild_id");
      if (!guildId) {
        return json({ error: "Missing guild_id" }, 400, corsHeaders);
      }
      const wantsLatest = url.searchParams.get("latest") === "1";
      let date = url.searchParams.get("date");
      const game = url.searchParams.get("game");

      if (wantsLatest) {
        date = await kv.get(`guild:${guildId}:latest_date`);
      }
      if (!date) {
        return json({ error: "Missing date (or no latest stored yet)" }, 400, corsHeaders);
      }

      const namesJson = await kv.get(`guild:${guildId}:date:${date}:names`);
      const metricsJson = await kv.get(`guild:${guildId}:date:${date}:metrics`);
      const allowedJson = await kv.get(`guild:${guildId}:date:${date}:allowed_usernames`);
      const names = namesJson ? JSON.parse(namesJson) : {};
      const metrics = metricsJson ? JSON.parse(metricsJson) : {};
      const allowedUsernames = allowedJson ? JSON.parse(allowedJson) : [];

      if (game) {
        const key = `guild:${guildId}:date:${date}:game:${game}`;
        const val = await kv.get(key);
        if (!val) return json({ error: "Not found", key }, 404, corsHeaders);

        const puzzle = JSON.parse(val);
        attachSolutionInfo(puzzle, names, metrics);

        return json(
          {
            guild_id: guildId,
            date,
            puzzles: { [game]: puzzle },
            names,
            metrics,
            allowed_usernames: allowedUsernames,
          },
          200,
          corsHeaders
        );
      }

      const gameKeys = ["friendle_daily", "quotele", "mediale", "statle"];
      const puzzles = {};
      for (const g of gameKeys) {
        const key = `guild:${guildId}:date:${date}:game:${g}`;
        const val = await kv.get(key);
        puzzles[g] = val ? JSON.parse(val) : null;
        if (puzzles[g]) attachSolutionInfo(puzzles[g], names, metrics);
      }

      return json(
        { guild_id: guildId, date, puzzles, names, metrics, allowed_usernames: allowedUsernames },
        200,
        corsHeaders
      );
    }

    return json({ error: "Not found", route: pathname, method: request.method }, 404, corsHeaders);
  },
};

// ------------ Existing helpers ------------
function attachSolutionInfo(puzzle, names, metrics) {
  if (!puzzle || !puzzle.solution_user_id) return;
  puzzle.solution_user_name = names[puzzle.solution_user_id] || null;
  if (puzzle.game === "friendle_daily") {
    puzzle.solution_metrics = metrics[puzzle.solution_user_id] || null;
  }
}

function json(obj, status, corsHeaders) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

async function verifySignedJson(request, env, corsHeaders) {
  const signature = request.headers.get("X-Signature");
  if (!signature) return { ok: false, res: json({ error: "Missing X-Signature" }, 401, corsHeaders) };

  const bodyText = await request.text();
  const secret = env.WEBHOOK_SECRET;
  if (!secret) {
    return { ok: false, res: json({ error: "Missing WEBHOOK_SECRET in Worker secrets" }, 500, corsHeaders) };
  }

  const expectedSig = await hmacSha256Hex(secret, bodyText);
  if (!timingSafeEqualHex(signature, expectedSig)) {
    return { ok: false, res: json({ error: "Invalid signature" }, 403, corsHeaders) };
  }

  let payload;
  try {
    payload = JSON.parse(bodyText);
  } catch {
    return { ok: false, res: json({ error: "Invalid JSON" }, 400, corsHeaders) };
  }

  return { ok: true, payload };
}

async function hmacSha256Hex(secret, bodyText) {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sigBuf = await crypto.subtle.sign("HMAC", key, enc.encode(bodyText));
  return [...new Uint8Array(sigBuf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function timingSafeEqualHex(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

// ------------ NEW helpers for stats ------------
async function readStats(kv, scopeKey) {
  const [views, guesses, correct, active, completed] = await Promise.all([
    kv.get(`${scopeKey}:views`),
    kv.get(`${scopeKey}:guesses_total`),
    kv.get(`${scopeKey}:guessed_correctly`),
    kv.get(`${scopeKey}:active_players`),
    kv.get(`${scopeKey}:completed_games`),
  ]);
  return {
    views: Number(views || 0),
    guesses_total: Number(guesses || 0),
    guessed_correctly: Number(correct || 0),
    active_players: Number(active || 0),
    completed_games: Number(completed || 0),
  };
}

async function bumpStat(kv, scopeKey, type) {
  // KV lacks atomic increment; we do read+write. This is acceptable for small scale.
  // If you expect heavy traffic, switch to Durable Objects.
  const keyFor = (t) => {
    if (t === "view") return `${scopeKey}:views`;
    if (t === "guess") return `${scopeKey}:guesses_total`;
    if (t === "guess_correct") return `${scopeKey}:guessed_correctly`;
    if (t === "game_complete") return `${scopeKey}:completed_games`;
    return null;
  };

  const key = keyFor(type);
  if (!key) return;

  const currentRaw = await kv.get(key);
  const current = Number(currentRaw || 0);
  await kv.put(key, String(current + 1), { expirationTtl: 60 * 60 * 24 * 365 });
}
