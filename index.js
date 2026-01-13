const { onSchedule } = require("firebase-functions/v2/scheduler");
const { getFunctions } = require("firebase-admin/functions");
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
const admin = require("firebase-admin");
const logger = require("firebase-functions/logger");
const axios = require('axios');

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