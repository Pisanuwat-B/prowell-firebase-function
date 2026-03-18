const REGION = "asia-southeast1";
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { getFunctions } = require("firebase-admin/functions");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { onRequest } = require("firebase-functions/v2/https");
const { Timestamp } = require("firebase-admin/firestore");

// v1 compat used specifically for auth.user().onDelete —
// v2 auth triggers require Identity Platform; v1 works with standard Firebase Auth.
const functionsV1 = require("firebase-functions/v1");

const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const axios = require('axios');

admin.initializeApp();

// Triggered automatically when Firebase Auth deletes a user.
// Recursively deletes the user's Firestore document and ALL subcollections:
//   daily_logs, addresses, payment_methods, health_history, weekly_reports, fcm_tokens
exports.onUserDeleted = functionsV1.region(REGION).auth.user().onDelete(async (user) => {
  const uid = user.uid;
  const userRef = admin.firestore().collection('users').doc(uid);

  try {
    await admin.firestore().recursiveDelete(userRef);
    logger.info(`All Firestore data deleted for user: ${uid}`);
  } catch (err) {
    logger.error(`Failed to delete Firestore data for user: ${uid}`, err);
    throw err;
  }
});

// MANUAL TRIGGER for testing
exports.manualDailyScan = onRequest({ region: REGION }, async (req, res) => {
  logger.info("Manual trigger started");
	logger.info("Project ID:", process.env.GCLOUD_PROJECT);
  
  // We simply call the same logic as your scheduler
  // You might want to wrap your boss logic in a named function to reuse it perfectly
  try {
    const db = admin.firestore();
    const now = Timestamp.now();
    
    const usersSnap = await db.collection('users')
      .where('consent.consent_ai_processing', '==', true)
      .where('consent.consent_product_recommendation', '==', true)
      .where('next_weekly_report_at', '<=', now)
      .limit(100) 
      .get();

    logger.info(`Test found ${usersSnap.size} users`);
    
    // Log exactly which IDs were picked up
    usersSnap.docs.forEach(doc => logger.info(`Picked up user: ${doc.id}`));

    const queue = getFunctions().taskQueue(`locations/${REGION}/functions/processWeeklyReport`);
    const promises = usersSnap.docs.map(doc => queue.enqueue({ userId: doc.id }));
    await Promise.all(promises);

    res.status(200).send(`Enqueued ${usersSnap.size} users. Check logs for worker progress.`);
  } catch (err) {
    logger.error("Manual trigger failed", err);
    res.status(500).send(err.message);
  }

});

exports.dailyUserScan = onSchedule({
	region: REGION,
  schedule: "*/10 3-6 * * *", 
  timeZone: "Asia/Bangkok",
}, async (event) => {
	logger.info("Scheduler started");
  const db = admin.firestore();
  const now = Timestamp.now();
  
  // Refined Query: Check both consent fields and the schedule
  const usersSnap = await db.collection('users')
    .where('consent.consent_ai_processing', '==', true)
    .where('consent.consent_product_recommendation', '==', true)
    .where('next_weekly_report_at', '<=', now)
    .limit(100) 
    .get();

  if (usersSnap.empty) {
    logger.info("No eligible users with consent found.");
    return;
  }

  const queue = getFunctions().taskQueue(`locations/${REGION}/functions/processWeeklyReport`);

  const promises = usersSnap.docs.map(doc => {
    return queue.enqueue({ userId: doc.id });
  });

  await Promise.all(promises);
  logger.info(`Enqueued ${usersSnap.size} tasks with full consent.`);
});

async function getLast14DailyLogs(userRef) {
  const fourteenDaysAgo = Timestamp.fromDate(
    new Date(Date.now() - 14 * 24 * 60 * 60 * 1000)
  );

  const snap = await userRef
    .collection('daily_logs')
    .where('log_date', '>=', fourteenDaysAgo)
    .get();

  return snap.docs.map(doc => doc.data());
}

function getCurrentWeekLogs(dailyLogs) {
  const now = new Date();
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(now.getDate() - 7);

  return dailyLogs.filter(log => {
    const logDate = log.log_date.toDate();
    return logDate >= sevenDaysAgo && logDate <= now;
  });
}

function countDistinctLogDays(dailyLogs) {
  const days = new Set();

  dailyLogs.forEach(log => {
    const d = log.log_date.toDate().toISOString().slice(0, 10);
    days.add(d);
  });

  return days.size;
}

exports.processWeeklyReport = onTaskDispatched(
  {
		region: REGION,
    timeoutSeconds: 300, // Give it 5 minutes per user to be safe
    memory: "1GiB",
    retryConfig: { maxAttempts: 3 },
    rateLimits: { maxConcurrentDispatches: 10 } // Only 10 calls at once
  },
  async (request) => {
    const userId = request.data.userId; // Data passed from the Dispatcher
    const userRef = admin.firestore().collection('users').doc(userId);
    const userSnap = await userRef.get();
    
    if (!userSnap.exists) return;

    const dailyLogs = await getLast14DailyLogs(userRef);
    const currentWeekLogs = getCurrentWeekLogs(dailyLogs);
    const distinctDays = countDistinctLogDays(currentWeekLogs);

    const now = new Date();
    const reportWeekEnd = new Date(now);
    const reportWeekStart = new Date(now);
    reportWeekStart.setDate(reportWeekStart.getDate() - 7);
    let reportType;

    const hasInitial = userSnap.data().has_received_initial_report === true;

    if (!hasInitial) {
      reportType = "initial";
    } else {
      reportType = "weekly";
    }

    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + 7);
    const startTime = new Date().toISOString(); // Record start of the task
    
    logger.info(`[Task Start] User: ${userId} with log count: ${dailyLogs.length} at ${startTime}`);

    if (!hasInitial) {
      await userRef.update({
        next_weekly_report_at: Timestamp.fromDate(nextDate),
      });
    } else {
      // Weekly report: require real coverage
      if (distinctDays < 3) {
        await userRef.update({
          next_weekly_report_at: Timestamp.fromDate(nextDate),
        });
			  logger.info(`User ${userId} has insufficient time coverage: ${distinctDays}`);
        return;
      }
    }

    const userData = normalizeFirestoreValue(userSnap.data());
    userData.daily_logs = normalizeFirestoreValue(dailyLogs);
    userData.health_history = await fetchSubcollection(userRef, "health_history");

    const aiUserPayload = buildAIUserPayload(userData, reportType);

    const aiResult = await generateWeeklyReport(aiUserPayload);

    const aiReqEnd = new Date().toISOString();
    logger.info(`[AI Response] User: ${userId} received at ${aiReqEnd}`);

    const batch = admin.firestore().batch();
    const reportId = reportWeekStart.toISOString().slice(0, 10);
    const reportRef = userRef.collection("weekly_reports").doc(reportId);
    batch.set(reportRef, {
      ...aiResult,
      created_at: Timestamp.fromDate(now),
      report_week_start: Timestamp.fromDate(reportWeekStart),
      report_week_end: Timestamp.fromDate(reportWeekEnd),
      model_version: "prowell-v1",
      notification_read: false,
      report_type: reportType,
    });

    batch.update(userRef, {
      next_weekly_report_at: Timestamp.fromDate(nextDate),
    });

    if (!hasInitial) {
      batch.update(userRef, {
        has_received_initial_report: true,
      });
    }
    
    await batch.commit();

    await sendWeeklyReportNotification(userId, reportType);
  }
);

async function fetchSubcollection(userRef, name) {
  const snap = await userRef.collection(name).get();
  return snap.docs.map(doc =>
    normalizeFirestoreValue({
      id: doc.id,
      ...doc.data(),
    })
  );
}

function normalizeFirestoreValue(value) {
  if (value === null || value === undefined) return value;

  // Firestore Timestamp
  if (value instanceof admin.firestore.Timestamp) {
    return value.toDate().toISOString();
  }

  // Firestore DocumentReference
  if (value.path && value.id) {
    return value.path;
  }

  // Array
  if (Array.isArray(value)) {
    return value.map(normalizeFirestoreValue);
  }

  // Object
  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = normalizeFirestoreValue(v);
    }
    return out;
  }

  return value;
}

// ─── Thai → English lookup maps ──────────────────────────────────────────────
const GENDER_MAP      = { 'ชาย': 'male', 'หญิง': 'female' };
const PORTION_MAP     = { 'บุฟเฟ่': 'buffet', 'มากกว่าปกติ': 'large', 'ปกติ': 'normal', 'น้อยกว่าปกติ': 'small' };
const MEAL_TYPE_MAP   = { 'เช้า': 'breakfast', 'ว่าง': 'snack', 'เที่ยง': 'lunch', 'เย็น': 'dinner' };
const VEG_LEVEL_MAP   = { '0/4 จาน': '0/4 plate', '1/4 จาน': '1/4 plate', '2/4 จาน': '2/4 plate', '3/4 จาน': '3/4 plate', '4/4 จาน': '4/4 plate' };
const UPF_TYPE_MAP    = { 'ขนมหวาน': 'sweets', 'เครื่องดื่ม': 'beverage', 'ของทอด': 'fried_food', 'อาหารกึ่งสำเร็จรูป': 'instant_food', 'อาหารหมัก/ดอง': 'fermented_food' };
const UPF_PORTION_MAP = { 'ปกติ': 'normal', 'เล็ก': 'small', 'ใหญ่': 'large' };
const UPF_TASTE_MAP   = { 'หวาน': 'sweet', 'เค็ม': 'salty', 'มัน': 'fatty', 'เปรี้ยว': 'sour', 'เผ็ด': 'spicy' };
const WAKE_FEEL_MAP   = { 'สดชื่น': 'refreshed', 'เฉยๆ': 'neutral', 'ยังง่วงอยู่': 'sleepy', 'ลุกไม่ไหว': 'exhausted' };
const WALK_MAP        = { 'น้อยกว่า 1km': 'less_than_1km', '2-4km': '2-4km', '5-8km': '5-8km', 'มากกว่า 10km': 'more_than_10km' };

// Trim + map lookup; returns undefined (dropped by clean) when not found.
function standardizeValue(input, map) {
  if (input == null) return undefined;
  return map[input.trim()] ?? undefined;
}

// Convert Firestore ISO timestamp string → "HH:mm" local time.
function formatTimeForAI(isoString) {
  if (!isoString) return undefined;
  try {
    const d = new Date(isoString);
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    return `${hh}:${mm}`;
  } catch (_) {
    return undefined;
  }
}

// Recursively strip null, undefined, empty objects, and empty arrays
// so the AI payload contains only meaningful data.
function clean(data) {
  if (Array.isArray(data)) {
    const arr = data.map(clean).filter(v => v !== null && v !== undefined);
    return arr.length ? arr : undefined;
  }
  if (data !== null && typeof data === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(data)) {
      const c = clean(v);
      if (c !== null && c !== undefined) out[k] = c;
    }
    return Object.keys(out).length ? out : undefined;
  }
  return data ?? undefined;
}

function buildAIUserPayload(user, reportType) {

  // ---------- USER PROFILE ----------
  const payload = {
    report_type:    reportType,   // 'initial' | 'weekly'
    age:            user.Age,
    gender:         standardizeValue(user.Gender, GENDER_MAP),
    height_cm:      user.height_cm,
    weight_kg:      user.weight_kg,
    conditions:     user.conditions,
    chronic_issues: user.chronic_issues,
  };

  // ---------- BIOMARKERS ----------
  const latest = Array.isArray(user.health_history) && user.health_history.length > 0
    ? user.health_history[user.health_history.length - 1]
    : null;

  if (latest) {
    const biomarkers = {};
    ['HDL', 'fbs', 'LDL', 'Cholesterol', 'Triglycerides', 'systolic', 'diastolic', 'Heartrate']
      .forEach(f => { const n = Number(latest[f]); if (n > 0) biomarkers[f] = n; });
    if (Object.keys(biomarkers).length) payload.biomarkers = biomarkers;
  }

  // ---------- DAILY LOGS ----------
  payload.daily_logs = (user.daily_logs || []).map(log => {
    const mind      = log.mind      || {};
    const sleep     = log.sleep     || {};
    const risk      = log.risk      || {};
    const nutrition = log.nutrition || {};
    const exercise  = (log.exercise || [])[0] || {};

    const rawSleep   = sleep.sleep_duration_hours;
    const sleepHours = rawSleep != null
      ? parseFloat(Number(rawSleep).toFixed(1))
      : undefined;

    return {
      log_date_key: log.log_date_key,

      mind: {
        mood_score:          mind.mood_score,
        stress_score:        mind.stress_score,
        mindfulness_minutes: mind.mindfulness_minutes,
        gratitude_done:      mind.gratitude_done,
        social_interaction:  mind.social_interaction,
      },

      sleep: {
        bedtime:              formatTimeForAI(sleep.bedtime),
        wake_time:            formatTimeForAI(sleep.wake_time),
        sleep_duration_hours: sleepHours,
        sleep_latency_min:    sleep.sleep_latency,
        waso_min:             sleep.waso,
        caffeine_after_2pm:   sleep.caffeine_after_2pm,
        wakeup_feeling:       standardizeValue(sleep.wakeup_feeling, WAKE_FEEL_MAP),
      },

      exercise: {
        walking_distance:  standardizeValue(exercise.activity_level, WALK_MAP),
        mvpa_minutes:      exercise.mvpa_minutes,
        steps:             exercise.estimated_steps,
        prolonged_sitting: exercise.prolonged_sitting,
        strength_training: exercise.resistance_today,
      },

      nutrition: {
        water_liters: nutrition.water_liters,
        meals: (nutrition.meals || []).map(m => ({
          meal_type:       standardizeValue(m.meal_type, MEAL_TYPE_MAP),
          food:            m.base_text,
          portion:         standardizeValue(m.portion, PORTION_MAP),
          vegetable_level: standardizeValue(m.veg_level, VEG_LEVEL_MAP),
        })),
        upf: (nutrition.upf_logs || []).map(u => ({
          item:     u.upf_item_name,
          category: standardizeValue(u.category_tag, UPF_TYPE_MAP),
          portion:  standardizeValue(u.portion_size, UPF_PORTION_MAP),
          reason:   u.reason_tag,
          tastes:   (u.taste_tags || [])
                      .map(t => standardizeValue(t, UPF_TASTE_MAP))
                      .filter(Boolean),
        })),
      },

      risk: {
        alcohol_drinks:   risk.alcohol_drinks,
        cigarettes_count: risk.cigarettes_count,
      },
    };
  });

  return clean(payload);
}

async function generateWeeklyReport(payload) {
  try {
    const response = await axios.post('https://asia-southeast1-prowell-health-span.cloudfunctions.net/query_rag', payload, {
      timeout: 300000, // Wait for 5 mins
      headers: { 'Content-Type': 'application/json' }
    });
    return response.data;
  } catch (error) {
    logger.error("AI API Error", error);
    throw error;
  }
}

async function sendWeeklyReportNotification(userId, reportType) {
  const userRef = admin.firestore().collection("users").doc(userId);
  const tokensSnap = await userRef.collection("fcm_tokens").get();

  logger.info(`Send push to User: ${userId} `);

  if (tokensSnap.empty) {
    logger.info(`No FCM tokens for user ${userId}`);
    return;
  }

  const tokens = [];
  tokensSnap.forEach(doc => tokens.push(doc.id));

   const titleMap = {
    weekly: "Your weekly health report is ready",
    initial: "Your first health report is ready",
  };

  const message = {
    tokens,
    notification: {
      title: titleMap[reportType] ?? "Your health report is ready",
      body: "Tap to see your insights and progress",
    },
    data: {
      type: reportType,
      userId,
    },
    android: {
      priority: "high",
    },
    apns: {
      payload: {
        aps: {
          sound: "default",
        },
      },
    },
  };

  const response = await admin.messaging().sendEachForMulticast(message);

  logger.info(`Push sent to user ${userId}`, {
    success: response.successCount,
    failure: response.failureCount,
  });

  response.responses.forEach((res, idx) => {
    if (!res.success) {
      logger.error("FCM send error", {
        token: tokens[idx],
        code: res.error?.code,
        message: res.error?.message,
      });
    }
  });

  const batch = admin.firestore().batch();
  response.responses.forEach((res, idx) => {
    if (!res.success) {
      const errCode = res.error?.code;
      if (
        errCode === "messaging/registration-token-not-registered" ||
        errCode === "messaging/invalid-registration-token"
      ) {
        const badToken = tokens[idx];
        batch.delete(userRef.collection("fcm_tokens").doc(badToken));
        logger.warn(`Removed invalid FCM token`, { badToken });
      }
    }
  });

  await batch.commit();
}