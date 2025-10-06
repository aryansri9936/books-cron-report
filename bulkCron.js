const cron = require('node-cron');
const redis = require('redis');
const Book = require('./models/books');

// Redis client setup
const redisClient = redis.createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

// Connect to Redis
redisClient.connect().catch(console.error);

redisClient.on('error', (err) => {
  console.error('Redis Client Error:', err);
});

redisClient.on('connect', () => {
  console.log('Redis client connected for bulk cron job');
});

/**
 * Process bulk book insertions from Redis
 * Tracks insertion status per user
 */
const processBulkBooks = async () => {
  try {
    console.log(`[${new Date().toISOString()}] Starting bulk book processing...`);

    // Get all keys matching the bulk book pattern
    const keys = await redisClient.keys('bulk_books:*');
    
    if (keys.length === 0) {
      console.log(`[${new Date().toISOString()}] No bulk books to process`);
      return;
    }

    console.log(`[${new Date().toISOString()}] Found ${keys.length} bulk book entries to process`);

    for (const key of keys) {
      const userId = key.split(':')[1];
      console.log(`[${new Date().toISOString()}] Processing bulk books for user: ${userId}`);

      try {
        // Get the book array from Redis
        const booksData = await redisClient.get(key);
        
        if (!booksData) {
          console.log(`[${new Date().toISOString()}] No data found for key: ${key}`);
          continue;
        }

        const books = JSON.parse(booksData);
        
        if (!Array.isArray(books) || books.length === 0) {
          console.log(`[${new Date().toISOString()}] Invalid or empty book array for user: ${userId}`);
          await redisClient.del(key);
          continue;
        }

        console.log(`[${new Date().toISOString()}] Processing ${books.length} books for user: ${userId}`);

        // Initialize status tracking
        const status = {
          userId: userId,
          totalBooks: books.length,
          successCount: 0,
          failureCount: 0,
          timestamp: new Date().toISOString(),
          failures: []
        };

        // Process each book
        for (let i = 0; i < books.length; i++) {
          const book = books[i];
          
          try {
            // Validate book data
            if (!book.title || !book.author) {
              throw new Error('Missing required fields: title and author');
            }

            // Create book document
            const newBook = new Book({
              title: book.title,
              author: book.author,
              isbn: book.isbn || null,
              publishedDate: book.publishedDate || null,
              genre: book.genre || null,
              description: book.description || null,
              userId: userId
            });

            // Save to database
            await newBook.save();
            status.successCount++;
            
            console.log(`[${new Date().toISOString()}] Successfully inserted book ${i + 1}/${books.length}: ${book.title}`);
          } catch (error) {
            status.failureCount++;
            status.failures.push({
              index: i,
              title: book.title || 'Unknown',
              error: error.message
            });
            
            console.error(`[${new Date().toISOString()}] Failed to insert book ${i + 1}/${books.length}: ${error.message}`);
          }
        }

        // Store status in Redis
        const statusKey = `bulk_status:${userId}:${Date.now()}`;
        await redisClient.setEx(statusKey, 86400, JSON.stringify(status)); // Expire after 24 hours
        
        console.log(`[${new Date().toISOString()}] Bulk insertion complete for user ${userId}:`);
        console.log(`  - Success: ${status.successCount}`);
        console.log(`  - Failures: ${status.failureCount}`);
        console.log(`  - Status saved to: ${statusKey}`);

        // Remove the processed bulk books from Redis
        await redisClient.del(key);
        console.log(`[${new Date().toISOString()}] Removed processed key: ${key}`);

      } catch (error) {
        console.error(`[${new Date().toISOString()}] Error processing bulk books for user ${userId}:`, error);
        
        // Store error status
        const errorStatus = {
          userId: userId,
          error: error.message,
          timestamp: new Date().toISOString(),
          status: 'failed'
        };
        
        const errorKey = `bulk_error:${userId}:${Date.now()}`;
        await redisClient.setEx(errorKey, 86400, JSON.stringify(errorStatus));
        
        console.log(`[${new Date().toISOString()}] Error status saved to: ${errorKey}`);
      }
    }

    console.log(`[${new Date().toISOString()}] Bulk book processing completed`);
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error in bulk processing cron job:`, error);
  }
};

/**
 * Initialize the cron job
 * Runs every 2 minutes
 */
const startBulkCron = () => {
  // Schedule: every 2 minutes
  cron.schedule('*/2 * * * *', async () => {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`[${new Date().toISOString()}] Bulk cron job triggered`);
    console.log('='.repeat(60));
    
    await processBulkBooks();
    
    console.log('='.repeat(60));
    console.log(`[${new Date().toISOString()}] Bulk cron job completed`);
    console.log(`${'='.repeat(60)}\n`);
  });

  console.log('Bulk book cron job initialized - running every 2 minutes');
};

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\nShutting down bulk cron job...');
  await redisClient.quit();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\nShutting down bulk cron job...');
  await redisClient.quit();
  process.exit(0);
});

module.exports = { startBulkCron, processBulkBooks };
