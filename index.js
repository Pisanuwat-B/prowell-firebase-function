const { onSchedule } = require("firebase-functions/v2/scheduler");
const { getFunctions } = require("firebase-admin/functions");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const { onRequest } = require("firebase-functions/v2/https");
const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const axios = require('axios');

// MANUAL TRIGGER for testing
exports.manualDailyScan = onRequest(async (req, res) => {
  logger.info("Manual trigger started");
  
  // We simply call the same logic as your scheduler
  // You might want to wrap your boss logic in a named function to reuse it perfectly
  try {
    const db = admin.firestore();
    const now = admin.firestore.Timestamp.now();
    
    const usersSnap = await db.collection('users')
      .where('consent.consent_ai_processing', '==', true)
      .where('consent.consent_product_recommendation', '==', true)
      .where('next_weekly_report_at', '<=', now)
      .limit(100) 
      .get();

    logger.info(`Test found ${usersSnap.size} users`);
    
    // Log exactly which IDs were picked up
    usersSnap.docs.forEach(doc => logger.info(`Picked up user: ${doc.id}`));

    const queue = getFunctions().taskQueue("processWeeklyReport");
    const promises = usersSnap.docs.map(doc => queue.enqueue({ userId: doc.id }));
    await Promise.all(promises);

    res.status(200).send(`Enqueued ${usersSnap.size} users. Check logs for worker progress.`);
  } catch (err) {
    logger.error("Manual trigger failed", err);
    res.status(500).send(err.message);
  }
});

exports.dailyUserScan = onSchedule({
  schedule: "*/10 3-6 * * *", 
  timeZone: "Asia/Bangkok",
}, async (event) => {
  const db = admin.firestore();
  const now = admin.firestore.Timestamp.now();
  
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

  const queue = getFunctions().taskQueue("processWeeklyReport");
  
  const promises = usersSnap.docs.map(doc => {
    return queue.enqueue({ userId: doc.id });
  });

  await Promise.all(promises);
  logger.info(`Enqueued ${usersSnap.size} tasks with full consent.`);
});

exports.processWeeklyReport = onTaskDispatched(
  {
    timeoutSeconds: 300, // Give it 5 minutes per user to be safe
    memory: "1GiB",
    retryConfig: { maxAttempts: 2 },
    rateLimits: { maxConcurrentDispatches: 10 } // Only 10 calls at once
  },
  async (request) => {
    const userId = request.data.userId; // Data passed from the Dispatcher
    const userRef = admin.firestore().collection('users').doc(userId);
    const userSnap = await userRef.get();
    
    if (!userSnap.exists) return;

    const dailyLogs = await getLast7DailyLogs(userRef);
    
    const nextDate = new Date();
    nextDate.setDate(nextDate.getDate() + 7);

    if (dailyLogs.length < 3) {
      await userRef.update({ 
        next_weekly_report_at: admin.firestore.Timestamp.fromDate(nextDate) 
      });
      return;
    }

		logger.info(`Processing report for ${userId}`, {
			logCount: dailyLogs.length,
			payloadSample: dailyLogs[0] // Check if fields like log_date are correct
		});

    const aiResult = await generateWeeklyReport({ user: userSnap.data(), dailyLogs });

    const batch = admin.firestore().batch();
    batch.set(userRef.collection('weekly_reports').doc(), { ...aiResult });
    batch.update(userRef, { 
      next_weekly_report_at: admin.firestore.Timestamp.fromDate(nextDate) 
    });
    
    await batch.commit();
  }
);

async function getLast7DailyLogs(userRef) {
  const snap = await userRef
    .collection('daily_logs')
    .orderBy('log_date', 'desc')
    .limit(7)
    .get();

  return snap.docs.map(doc => doc.data());
}

async function generateWeeklyReport(payload) {
  // try {
  //   const response = await axios.post('AI_TEAM_ENDPOINT', payload, {
  //     timeout: 180000, // Wait for 3 minutes (180,000ms)
  //     headers: { 'Authorization': `Bearer ${process.env.AI_API_KEY}` }
  //   });
  //   return response.data;
  // } catch (error) {
  //   logger.error("AI API Error", error);
  //   throw error;
  // }

  return {
    summary: 'สุขภาพโดยรวมอยู่ในเกณฑ์ปานกลาง',
    risks: ['การนอนหลับไม่สม่ำเสมอ'],
    recommendations: ['ควรนอนให้ครบ 7–8 ชั่วโมง', 'เพิ่มกิจกรรมการเดินในแต่ละวัน'],
    generated_at: new Date(),
    source: 'Mockup data',
  };
}