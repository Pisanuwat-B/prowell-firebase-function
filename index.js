const REGION = "asia-southeast1";
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { getFunctions } = require("firebase-admin/functions");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { onRequest } = require("firebase-functions/v2/https");
const { onDocumentUpdated } = require("firebase-functions/v2/firestore");
const { Timestamp } = require("firebase-admin/firestore");

// v1 compat used specifically for auth.user().onDelete —
// v2 auth triggers require Identity Platform; v1 works with standard Firebase Auth.
const functionsV1 = require("firebase-functions/v1");

const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const axios = require('axios');

admin.initializeApp();

// Shared helper: groups order items by product ref path, sums quantities,
// and batch-increments sold_qty. Used by both the HTTP endpoint and the
// Firestore trigger below.
async function applyIncrementSoldQty(items) {
  if (!items || items.length === 0) return 0;
  const db = admin.firestore();
  const qtyByPath = {};
  for (const item of items) {
    if (item.product) {
      const path = item.product.path; // DocumentReference → string path
      qtyByPath[path] = (qtyByPath[path] ?? 0) + (item.quantity ?? 1);
    }
  }
  const batch = db.batch();
  for (const [path, qty] of Object.entries(qtyByPath)) {
    batch.update(db.doc(path), {
      sold_qty: admin.firestore.FieldValue.increment(qty),
    });
  }
  await batch.commit();
  return Object.keys(qtyByPath).length;
}

// Admin-utility HTTP endpoint: manually increment sold_qty for a given order.
// Requires a valid Firebase ID token. Kept for admin/debugging purposes.
exports.incrementSoldQty = onRequest({ region: REGION }, async (req, res) => {
  const authHeader = req.headers.authorization ?? '';
  if (!authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    await admin.auth().verifyIdToken(authHeader.split('Bearer ')[1]);
  } catch (err) {
    return res.status(401).json({ error: 'Invalid token' });
  }

  const { orderId } = req.body ?? {};
  if (!orderId) {
    return res.status(400).json({ error: 'orderId is required' });
  }

  try {
    const db = admin.firestore();
    const orderSnap = await db.collection('orders').doc(orderId).get();
    if (!orderSnap.exists) {
      return res.status(404).json({ error: 'Order not found' });
    }
    const updated = await applyIncrementSoldQty(orderSnap.data().items ?? []);
    logger.info(`incrementSoldQty: updated ${updated} products for order ${orderId}`);
    return res.status(200).json({ success: true, updated });
  } catch (err) {
    logger.error('incrementSoldQty failed', { orderId, err });
    return res.status(500).json({ error: err.message });
  }
});

// Firestore trigger: fires when an order document is updated.
// Increments sold_qty only on the transition TO 'ชำระเงินสำเร็จ' (paid),
// ensuring inventory is decremented only after confirmed payment.
exports.onOrderPaid = onDocumentUpdated(
  { document: "orders/{orderId}", region: REGION },
  async (event) => {
    const before = event.data.before.data();
    const after  = event.data.after.data();

    // Idempotency guard: only act on the exact transition → 'ชำระเงินสำเร็จ'.
    // If status was already 'ชำระเงินสำเร็จ' before, this is a re-write/retry —
    // skip to avoid double-counting.
    if (after.status !== 'ชำระเงินสำเร็จ' || before.status === 'ชำระเงินสำเร็จ') {
      return null;
    }

    const orderId = event.params.orderId;
    try {
      const updated = await applyIncrementSoldQty(after.items ?? []);
      logger.info(`onOrderPaid: incremented sold_qty for ${updated} products`, { orderId });
    } catch (err) {
      logger.error('onOrderPaid: failed to increment sold_qty', { orderId, err });
      // Not re-thrown: Cloud Functions would retry on unhandled rejection,
      // which would double-increment. Log and exit cleanly instead.
    }
    return null;
  }
);

// HTTP endpoint: collects all admin FCM tokens and sends a push notification
// for a paid order. Called by the main app immediately after payment succeeds.
// Requires a valid Firebase ID token in the Authorization header.
exports.notifyAdminsOrderPaid = onRequest({ region: REGION }, async (req, res) => {
  const authHeader = req.headers.authorization ?? '';
  if (!authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    await admin.auth().verifyIdToken(authHeader.split('Bearer ')[1]);
  } catch {
    return res.status(401).json({ error: 'Invalid token' });
  }

  const { orderId } = req.body ?? {};
  if (!orderId) return res.status(400).json({ error: 'orderId is required' });

  try {
    const db = admin.firestore();

    const orderSnap = await db.collection('orders').doc(orderId).get();
    if (!orderSnap.exists) return res.status(404).json({ error: 'Order not found' });
    const order = orderSnap.data();

    const adminsSnap = await db.collection('users').where('role', '==', 'admin').get();
    if (adminsSnap.empty) {
      logger.info('notifyAdminsOrderPaid: no admin users found');
      return res.status(200).json({ success: true, sent: 0 });
    }

    // Build a flat list of { adminRef, token } so we can delete stale tokens
    // from the correct subcollection after sending.
    const tokenEntries = [];
    await Promise.all(adminsSnap.docs.map(async (adminDoc) => {
      const tokensSnap = await adminDoc.ref.collection('admin_fcm_tokens').get();
      tokensSnap.forEach(t => {
        const token = t.data().token;
        if (token) tokenEntries.push({ adminRef: adminDoc.ref, token });
      });
    }));

    if (tokenEntries.length === 0) {
      logger.info('notifyAdminsOrderPaid: no admin_fcm_tokens found');
      return res.status(200).json({ success: true, sent: 0 });
    }

    const tokens = tokenEntries.map(e => e.token);
    const shortId = orderId.slice(-6).toUpperCase();
    const amount = (order.total_amount ?? 0).toLocaleString('th-TH');

    const fcmMessage = {
      tokens,
      notification: {
        title: `ออเดอร์ #${shortId} ชำระเงินแล้ว`,
        body: `ยอดรวม ฿${amount} — รอการจัดส่ง`,
      },
      data: { orderId, type: 'order_paid' },
      android: { priority: 'high' },
      apns: { payload: { aps: { sound: 'default' } } },
    };

    const result = await admin.messaging().sendEachForMulticast(fcmMessage);

    // Clean up any tokens that FCM rejected so we don't keep retrying them.
    const batch = db.batch();
    result.responses.forEach((r, idx) => {
      if (!r.success) {
        const code = r.error?.code;
        if (
          code === 'messaging/registration-token-not-registered' ||
          code === 'messaging/invalid-registration-token'
        ) {
          const { adminRef, token } = tokenEntries[idx];
          batch.delete(adminRef.collection('admin_fcm_tokens').doc(token));
          logger.warn('notifyAdminsOrderPaid: removed stale token', { token });
        } else {
          logger.error('notifyAdminsOrderPaid: FCM error', { code: r.error?.code, msg: r.error?.message });
        }
      }
    });
    await batch.commit();

    logger.info('notifyAdminsOrderPaid done', { orderId, success: result.successCount, failure: result.failureCount });
    return res.status(200).json({ success: true, sent: result.successCount });
  } catch (err) {
    logger.error('notifyAdminsOrderPaid failed', { orderId, err });
    return res.status(500).json({ error: err.message });
  }
});

// Firestore trigger: fires when an order transitions to 'จัดส่งสำเร็จ' (shipped).
// Sends an FCM push notification to the customer with the carrier + tracking number.
exports.onOrderShipped = onDocumentUpdated(
  { document: "orders/{orderId}", region: REGION },
  async (event) => {
    const before = event.data.before.data();
    const after  = event.data.after.data();

    // Idempotency guard: only act on the exact transition → 'จัดส่งสำเร็จ'.
    if (after.status !== 'จัดส่งสำเร็จ' || before.status === 'จัดส่งสำเร็จ') {
      return null;
    }

    const orderId   = event.params.orderId;
    const userRef   = after.user_ref; // Firestore DocumentReference

    if (!userRef) {
      logger.warn('onOrderShipped: order has no user_ref', { orderId });
      return null;
    }

    const db = admin.firestore();
    const tokensSnap = await db.doc(userRef.path).collection('fcm_tokens').get();

    if (tokensSnap.empty) {
      logger.info('onOrderShipped: no FCM tokens for user', { orderId });
      return null;
    }

    const tokens         = tokensSnap.docs.map(doc => doc.id);
    const trackingNumber = after.tracking_number  ?? '';
    const carrier        = after.shipping_carrier ?? '';
    const shortId        = orderId.slice(-6).toUpperCase();

    const bodyParts = [];
    if (carrier)        bodyParts.push(carrier);
    if (trackingNumber) bodyParts.push(`เลขพัสดุ: ${trackingNumber}`);
    const body = bodyParts.length > 0
      ? bodyParts.join(' · ')
      : 'ติดตามสถานะพัสดุได้แล้ว';

    const fcmMessage = {
      tokens,
      notification: {
        title: `คำสั่งซื้อ #${shortId} ถูกจัดส่งแล้ว 🚚`,
        body,
      },
      data: { type: 'order_shipped', orderId },
      android: { priority: 'high' },
      apns:    { payload: { aps: { sound: 'default' } } },
    };

    try {
      const result = await admin.messaging().sendEachForMulticast(fcmMessage);

      // Clean up stale tokens so we don't keep retrying them.
      const batch = db.batch();
      result.responses.forEach((r, idx) => {
        if (!r.success) {
          const code = r.error?.code;
          if (
            code === 'messaging/registration-token-not-registered' ||
            code === 'messaging/invalid-registration-token'
          ) {
            batch.delete(db.doc(userRef.path).collection('fcm_tokens').doc(tokens[idx]));
            logger.warn('onOrderShipped: removed stale token', { token: tokens[idx] });
          } else {
            logger.error('onOrderShipped: FCM error', { code, msg: r.error?.message });
          }
        }
      });
      await batch.commit();

      logger.info('onOrderShipped: notification sent', {
        orderId,
        success: result.successCount,
        failure: result.failureCount,
      });
    } catch (err) {
      logger.error('onOrderShipped: failed to send FCM', { orderId, err });
    }

    return null;
  }
);

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

function standardizeMood(moodScore) {
  switch (moodScore) {
    case 1: return 'sad';
    case 2: return 'neutral';
    case 3: return 'good';
    case 4: return 'happy';
    case 5: return 'great';
    case 6: return 'angry';
    default: return undefined;
  }
}

function standardizeStress(stressScore) {
  switch (stressScore) {
    case 1: return 'very_relaxed';
    case 2: return 'relaxed';
    case 3: return 'calm';
    case 4: return 'stressed';
    case 5: return 'overwhelmed';
    default: return undefined;
  }
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
  const todayKey = new Date().toISOString().slice(0, 10); // 'yyyy-mm-dd'
  const payload = {
    report_type:    reportType,   // 'initial' | 'weekly'
    today:          todayKey,
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
        mood:                standardizeMood(mind.mood_score),
        stress_label:        standardizeStress(mind.stress_score),
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

