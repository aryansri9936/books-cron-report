const cron = require('node-cron');
const redis = require('redis');
const nodemailer = require('nodemailer');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const User = require('./models/User'); // Assuming user model exists

// Redis client setup
const redisClient = redis.createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

// Email transporter setup
const transporter = nodemailer.createTransporter({
  host: process.env.SMTP_HOST || 'smtp.gmail.com',
  port: process.env.SMTP_PORT || 587,
  secure: false,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS
  }
});

// Connect to Redis
redisClient.connect().catch(console.error);

redisClient.on('error', (err) => {
  console.error('Redis Client Error:', err);
});

redisClient.on('connect', () => {
  console.log('Redis client connected for report cron job');
});

/**
 * Generate PDF report for user's bulk insertion status
 * @param {Object} statusData - Status data from Redis
 * @param {string} userId - User ID
 * @returns {Promise<Buffer>} PDF buffer
 */
const generatePDFReport = async (statusData, userId) => {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument();
      const chunks = [];

      // Collect PDF data
      doc.on('data', chunk => chunks.push(chunk));
      doc.on('end', () => resolve(Buffer.concat(chunks)));

      // PDF Header
      doc.fontSize(20).text('Books Bulk Insertion Report', 100, 50);
      doc.fontSize(12).text(`Generated on: ${new Date().toLocaleString()}`, 100, 80);
      doc.text(`Report ID: ${Date.now()}`, 100, 95);

      // User Information
      doc.fontSize(16).text('User Details', 100, 130);
      doc.fontSize(12)
         .text(`User ID: ${userId}`, 100, 155)
         .text(`Process Timestamp: ${statusData.timestamp}`, 100, 170);

      // Summary Section
      doc.fontSize(16).text('Summary', 100, 200);
      doc.fontSize(12)
         .text(`Total Books Processed: ${statusData.totalBooks}`, 100, 225)
         .text(`Successful Insertions: ${statusData.successCount}`, 100, 240)
         .text(`Failed Insertions: ${statusData.failureCount}`, 100, 255)
         .text(`Success Rate: ${((statusData.successCount / statusData.totalBooks) * 100).toFixed(2)}%`, 100, 270);

      // Status indicators
      if (statusData.successCount > 0) {
        doc.fillColor('green').text('✓', 80, 240).fillColor('black');
      }
      if (statusData.failureCount > 0) {
        doc.fillColor('red').text('✗', 80, 255).fillColor('black');
      }

      // Failures Section (if any)
      if (statusData.failures && statusData.failures.length > 0) {
        doc.fontSize(16).text('Failed Items Details', 100, 300);
        let yPosition = 325;
        
        statusData.failures.forEach((failure, index) => {
          if (yPosition > 700) { // Start new page if needed
            doc.addPage();
            yPosition = 50;
          }
          
          doc.fontSize(12)
             .text(`${index + 1}. Title: ${failure.title}`, 100, yPosition)
             .text(`   Index: ${failure.index}`, 100, yPosition + 15)
             .text(`   Error: ${failure.error}`, 100, yPosition + 30);
          
          yPosition += 50;
        });
      }

      // Footer
      const pageCount = doc.bufferedPageRange().count;
      for (let i = 0; i < pageCount; i++) {
        doc.switchToPage(i);
        doc.fontSize(10)
           .text(`Page ${i + 1} of ${pageCount}`, 450, 750);
      }

      doc.end();
    } catch (error) {
      reject(error);
    }
  });
};

/**
 * Get user email by user ID
 * @param {string} userId - User ID
 * @returns {Promise<string>} User email
 */
const getUserEmail = async (userId) => {
  try {
    // Try to get from User model first
    const user = await User.findById(userId);
    if (user && user.email) {
      return user.email;
    }
    
    // Fallback: try to get from Redis cache
    const cachedEmail = await redisClient.get(`user_email:${userId}`);
    if (cachedEmail) {
      return cachedEmail;
    }
    
    // Default fallback - this should be replaced with actual user lookup
    console.warn(`No email found for user ${userId}, using fallback`);
    return process.env.DEFAULT_EMAIL || 'admin@example.com';
  } catch (error) {
    console.error(`Error getting email for user ${userId}:`, error);
    throw error;
  }
};

/**
 * Send email with PDF report attachment
 * @param {string} userEmail - User's email address
 * @param {Buffer} pdfBuffer - PDF report buffer
 * @param {Object} statusData - Status data for email content
 * @param {string} userId - User ID
 */
const sendReportEmail = async (userEmail, pdfBuffer, statusData, userId) => {
  try {
    const mailOptions = {
      from: process.env.EMAIL_FROM || 'noreply@booksapi.com',
      to: userEmail,
      subject: 'Books Bulk Insertion Report',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
          <h2>Books Bulk Insertion Report</h2>
          <p>Dear User,</p>
          <p>Your bulk book insertion process has been completed. Here's a summary:</p>
          
          <div style="background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0;">
            <h3>Summary</h3>
            <ul style="list-style-type: none; padding: 0;">
              <li><strong>User ID:</strong> ${userId}</li>
              <li><strong>Total Books:</strong> ${statusData.totalBooks}</li>
              <li><strong>Successful:</strong> <span style="color: green;">${statusData.successCount}</span></li>
              <li><strong>Failed:</strong> <span style="color: red;">${statusData.failureCount}</span></li>
              <li><strong>Success Rate:</strong> ${((statusData.successCount / statusData.totalBooks) * 100).toFixed(2)}%</li>
              <li><strong>Process Time:</strong> ${statusData.timestamp}</li>
            </ul>
          </div>
          
          ${statusData.failureCount > 0 ? 
            '<p style="color: red;"><strong>Note:</strong> Some books failed to insert. Please check the attached PDF report for detailed error information.</p>' : 
            '<p style="color: green;"><strong>Success!</strong> All books were inserted successfully.</p>'
          }
          
          <p>Please find the detailed report attached as a PDF file.</p>
          
          <p>Best regards,<br>
          Books API Team</p>
          
          <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
          <p style="font-size: 12px; color: #666;">
            This is an automated message. Please do not reply to this email.
          </p>
        </div>
      `,
      attachments: [
        {
          filename: `books-report-${userId}-${Date.now()}.pdf`,
          content: pdfBuffer,
          contentType: 'application/pdf'
        }
      ]
    };

    const info = await transporter.sendMail(mailOptions);
    console.log(`[${new Date().toISOString()}] Email sent successfully to ${userEmail}:`, info.messageId);
    return info;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error sending email to ${userEmail}:`, error);
    throw error;
  }
};

/**
 * Process status reports for all users
 */
const processStatusReports = async () => {
  try {
    console.log(`[${new Date().toISOString()}] Starting status report processing...`);
    
    // Get all bulk status keys from Redis
    const statusKeys = await redisClient.keys('bulk_status:*');
    
    if (statusKeys.length === 0) {
      console.log(`[${new Date().toISOString()}] No status reports to process`);
      return;
    }

    console.log(`[${new Date().toISOString()}] Found ${statusKeys.length} status reports to process`);

    let processedCount = 0;
    let errorCount = 0;

    for (const key of statusKeys) {
      const userId = key.split(':')[1];
      console.log(`[${new Date().toISOString()}] Processing report for user: ${userId}`);

      try {
        // Get status data from Redis
        const statusData = await redisClient.get(key);
        
        if (!statusData) {
          console.log(`[${new Date().toISOString()}] No data found for key: ${key}`);
          continue;
        }

        const parsedStatus = JSON.parse(statusData);
        
        // Validate status data
        if (!parsedStatus.userId || parsedStatus.totalBooks === undefined) {
          console.log(`[${new Date().toISOString()}] Invalid status data for key: ${key}`);
          await redisClient.del(key);
          continue;
        }

        // Get user email
        const userEmail = await getUserEmail(userId);
        
        // Generate PDF report
        console.log(`[${new Date().toISOString()}] Generating PDF report for user: ${userId}`);
        const pdfBuffer = await generatePDFReport(parsedStatus, userId);

        // Send email with report
        console.log(`[${new Date().toISOString()}] Sending email report to: ${userEmail}`);
        await sendReportEmail(userEmail, pdfBuffer, parsedStatus, userId);

        // Delete the status record from Redis after successful email
        await redisClient.del(key);
        console.log(`[${new Date().toISOString()}] Status record deleted: ${key}`);

        processedCount++;
        console.log(`[${new Date().toISOString()}] Successfully processed report for user: ${userId}`);

      } catch (error) {
        errorCount++;
        console.error(`[${new Date().toISOString()}] Error processing report for user ${userId}:`, error);

        // Store error information for monitoring
        const errorKey = `report_error:${userId}:${Date.now()}`;
        const errorData = {
          userId: userId,
          originalKey: key,
          error: error.message,
          timestamp: new Date().toISOString(),
          retryCount: 0
        };
        
        await redisClient.setEx(errorKey, 86400, JSON.stringify(errorData)); // Expire after 24 hours
        console.log(`[${new Date().toISOString()}] Error logged to: ${errorKey}`);
      }
    }

    console.log(`[${new Date().toISOString()}] Status report processing completed:`);
    console.log(`  - Processed: ${processedCount}`);
    console.log(`  - Errors: ${errorCount}`);

  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error in status report processing:`, error);
  }
};

/**
 * Initialize the report cron job
 * Runs every 5 minutes
 */
const startReportCron = () => {
  // Schedule: every 5 minutes
  cron.schedule('*/5 * * * *', async () => {
    console.log(`\n${'='.repeat(70)}`);
    console.log(`[${new Date().toISOString()}] Report cron job triggered`);
    console.log('='.repeat(70));
    
    await processStatusReports();
    
    console.log('='.repeat(70));
    console.log(`[${new Date().toISOString()}] Report cron job completed`);
    console.log(`${'='.repeat(70)}\n`);
  });

  console.log('Report cron job initialized - running every 5 minutes');
};

/**
 * Test email configuration
 */
const testEmailConfig = async () => {
  try {
    await transporter.verify();
    console.log('Email configuration verified successfully');
  } catch (error) {
    console.error('Email configuration error:', error);
  }
};

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down report cron job...');
  await redisClient.quit();
  transporter.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down report cron job...');
  await redisClient.quit();
  transporter.close();
  process.exit(0);
});

module.exports = { 
  startReportCron, 
  processStatusReports, 
  generatePDFReport, 
  sendReportEmail,
  testEmailConfig 
};
