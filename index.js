const REGION = "asia-southeast1";
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { getFunctions } = require("firebase-admin/functions");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { onRequest } = require("firebase-functions/v2/https");
const { Timestamp } = require("firebase-admin/firestore");

const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const axios = require('axios');

admin.initializeApp();

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
    
    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + 7);
    const startTime = new Date().toISOString(); // Record start of the task
    
    logger.info(`[Task Start] User: ${userId} with log count: ${dailyLogs.length} at ${startTime}`);

    if (distinctDays < 3) {
			await userRef.update({
				next_weekly_report_at: Timestamp.fromDate(nextDate),
			});
			logger.info(`User ${userId} has insufficient time coverage`, {
				distinctDays,
			});
			return;
		}

    const userData = normalizeFirestoreValue(userSnap.data());
    userData.daily_logs = normalizeFirestoreValue(dailyLogs);
    userData.health_history = await fetchSubcollection(userRef, "health_history");

    const aiResult = await generateWeeklyReport(userData);

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
      model_version: "mock-v1",
      notification_read: false,
    });

    batch.update(userRef, {
      next_weekly_report_at: Timestamp.fromDate(nextDate),
    });
    
    await batch.commit();

    await sendWeeklyReportNotification(userId);
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

async function sendWeeklyReportNotification(userId) {
  const userRef = admin.firestore().collection("users").doc(userId);
  const tokensSnap = await userRef.collection("fcm_tokens").get();

  logger.info(`Send push to User: ${userId} `);

  if (tokensSnap.empty) {
    logger.info(`No FCM tokens for user ${userId}`);
    return;
  }

  const tokens = [];
  tokensSnap.forEach(doc => tokens.push(doc.id));

  const message = {
    tokens,
    notification: {
      title: "Your weekly health report is ready",
      body: "Tap to see your insights and progress",
    },
    data: {
      type: "weekly_report",
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