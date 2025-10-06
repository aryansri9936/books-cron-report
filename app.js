require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { connectDB } = require('./db');
const authRoutes = require('./routes/auth');
const bookRoutes = require('./routes/books');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Connect to MongoDB
connectDB();

// Routes
app.use('/api/auth', authRoutes);
app.use('/api/books', bookRoutes);

// Health check
app.get('/', (req, res) => {
  res.json({ message: 'Books API with Redis & Cron Jobs', status: 'running' });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Error:', err.message);
  res.status(500).json({ error: err.message || 'Internal Server Error' });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
