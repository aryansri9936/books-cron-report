const express = require('express');
const router = express.Router();
const Book = require('../models/Book');
const redis = require('../config/redis');
const authMiddleware = require('../middleware/auth');

// Helper function to generate user-specific cache keys
const getUserCacheKey = (userId, key) => `user:${userId}:${key}`;

// Helper function to invalidate user-specific cache
const invalidateUserCache = async (userId) => {
  const cacheKey = getUserCacheKey(userId, 'books');
  await redis.del(cacheKey);
};

// CREATE - Add a new book
router.post('/', authMiddleware, async (req, res) => {
  try {
    const { title, author, isbn, publishedYear, genre } = req.body;
    const userId = req.user.id;

    // Validate required fields
    if (!title || !author || !isbn) {
      return res.status(400).json({ error: 'Title, author, and ISBN are required' });
    }

    // Create new book
    const book = new Book({
      title,
      author,
      isbn,
      publishedYear,
      genre,
      userId
    });

    await book.save();

    // Invalidate user's cache
    await invalidateUserCache(userId);

    res.status(201).json({ message: 'Book created successfully', book });
  } catch (error) {
    if (error.code === 11000) {
      return res.status(400).json({ error: 'Book with this ISBN already exists' });
    }
    res.status(500).json({ error: 'Error creating book', details: error.message });
  }
});

// READ - Get all books (with Redis caching per user)
router.get('/', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const cacheKey = getUserCacheKey(userId, 'books');

    // Try to get from cache
    const cachedBooks = await redis.get(cacheKey);
    
    if (cachedBooks) {
      return res.json({ 
        source: 'cache', 
        books: JSON.parse(cachedBooks) 
      });
    }

    // If not in cache, fetch from database
    const books = await Book.find({ userId }).sort({ createdAt: -1 });

    // Store in cache with 1 hour expiration
    await redis.setex(cacheKey, 3600, JSON.stringify(books));

    res.json({ 
      source: 'database', 
      books 
    });
  } catch (error) {
    res.status(500).json({ error: 'Error fetching books', details: error.message });
  }
});

// READ - Get a single book by ID
router.get('/:id', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const book = await Book.findOne({ _id: req.params.id, userId });

    if (!book) {
      return res.status(404).json({ error: 'Book not found' });
    }

    res.json({ book });
  } catch (error) {
    res.status(500).json({ error: 'Error fetching book', details: error.message });
  }
});

// UPDATE - Update a book
router.put('/:id', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const { title, author, isbn, publishedYear, genre } = req.body;

    const book = await Book.findOneAndUpdate(
      { _id: req.params.id, userId },
      { title, author, isbn, publishedYear, genre },
      { new: true, runValidators: true }
    );

    if (!book) {
      return res.status(404).json({ error: 'Book not found' });
    }

    // Invalidate user's cache
    await invalidateUserCache(userId);

    res.json({ message: 'Book updated successfully', book });
  } catch (error) {
    if (error.code === 11000) {
      return res.status(400).json({ error: 'Book with this ISBN already exists' });
    }
    res.status(500).json({ error: 'Error updating book', details: error.message });
  }
});

// DELETE - Delete a book
router.delete('/:id', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const book = await Book.findOneAndDelete({ _id: req.params.id, userId });

    if (!book) {
      return res.status(404).json({ error: 'Book not found' });
    }

    // Invalidate user's cache
    await invalidateUserCache(userId);

    res.json({ message: 'Book deleted successfully', book });
  } catch (error) {
    res.status(500).json({ error: 'Error deleting book', details: error.message });
  }
});

// BULK INSERT - Add multiple books at once
router.post('/bulk', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    const { books } = req.body;

    if (!books || !Array.isArray(books) || books.length === 0) {
      return res.status(400).json({ error: 'Books array is required and must not be empty' });
    }

    // Validate each book has required fields
    for (const book of books) {
      if (!book.title || !book.author || !book.isbn) {
        return res.status(400).json({ 
          error: 'Each book must have title, author, and ISBN' 
        });
      }
    }

    // Add userId to each book
    const booksWithUserId = books.map(book => ({
      ...book,
      userId
    }));

    // Insert all books
    const insertedBooks = await Book.insertMany(booksWithUserId, { ordered: false });

    // Invalidate user's cache
    await invalidateUserCache(userId);

    res.status(201).json({ 
      message: `${insertedBooks.length} books inserted successfully`, 
      books: insertedBooks 
    });
  } catch (error) {
    if (error.code === 11000) {
      // Some books may have been inserted before duplicate error
      await invalidateUserCache(req.user.id);
      return res.status(400).json({ 
        error: 'Some books have duplicate ISBNs', 
        details: error.message 
      });
    }
    res.status(500).json({ error: 'Error bulk inserting books', details: error.message });
  }
});

// CACHE INVALIDATION - Manual cache invalidation endpoint
router.delete('/cache/invalidate', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id;
    await invalidateUserCache(userId);

    res.json({ message: 'Cache invalidated successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Error invalidating cache', details: error.message });
  }
});

module.exports = router;
