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

async function getLast7DailyLogs(userRef) {
  const sevenDaysAgo = Timestamp.fromDate(
    new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
  );

  const snap = await userRef
    .collection('daily_logs')
    .where('log_date', '>=', sevenDaysAgo)
    .get();

  return snap.docs.map(doc => doc.data());
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

    const dailyLogs = await getLast7DailyLogs(userRef);
		const distinctDays = countDistinctLogDays(dailyLogs);

    const now = new Date();
    const reportWeekEnd = new Date(now);
    const reportWeekStart = new Date(now);
    reportWeekStart.setDate(reportWeekStart.getDate() - 7);
    
    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + 7);

    if (distinctDays < 3) {
			await userRef.update({
				next_weekly_report_at: Timestamp.fromDate(nextDate),
			});
			logger.info(`User ${userId} has insufficient time coverage`, {
				distinctDays,
			});
			return;
		}

		logger.info(`Processing report for ${userId} with log count: ${dailyLogs.length}`);

    const aiResult = await generateWeeklyReport({
      user: userSnap.data(),
      dailyLogs,
      report_week_start: reportWeekStart,
      report_week_end: reportWeekEnd,
    });

    const batch = admin.firestore().batch();
    const reportRef = userRef.collection("weekly_reports").doc();
    batch.set(reportRef, {
      ...aiResult,
      created_at: Timestamp.fromDate(now),
      report_week_start: Timestamp.fromDate(reportWeekStart),
      report_week_end: Timestamp.fromDate(reportWeekEnd),
      model_version: "mock-v1",
    });

    batch.update(userRef, {
      next_weekly_report_at: Timestamp.fromDate(nextDate),
    });
    
    await batch.commit();
  }
);

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