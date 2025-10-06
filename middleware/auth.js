const jwt = require('jsonwebtoken');

/**
 * Authentication Middleware
 * Validates JWT token from Authorization header
 * Attaches user information to request object
 * Returns 401 for missing or invalid tokens
 */
const authMiddleware = async (req, res, next) => {
  try {
    // Extract token from Authorization header
    const authHeader = req.headers.authorization;

    // Check if Authorization header exists
    if (!authHeader) {
      return res.status(401).json({ 
        error: 'Access denied',
        message: 'No authorization token provided' 
      });
    }

    // Check if token follows Bearer scheme
    if (!authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ 
        error: 'Access denied',
        message: 'Invalid authorization format. Use Bearer token' 
      });
    }

    // Extract token from Bearer scheme
    const token = authHeader.substring(7);

    // Check if token exists after Bearer
    if (!token) {
      return res.status(401).json({ 
        error: 'Access denied',
        message: 'No token provided' 
      });
    }

    // Verify JWT token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // Attach user information to request object
    req.user = {
      id: decoded.id || decoded.userId,
      email: decoded.email,
      username: decoded.username
    };

    // Proceed to next middleware/route handler
    next();

  } catch (error) {
    // Handle JWT verification errors
    if (error.name === 'JsonWebTokenError') {
      return res.status(401).json({ 
        error: 'Access denied',
        message: 'Invalid token' 
      });
    }

    if (error.name === 'TokenExpiredError') {
      return res.status(401).json({ 
        error: 'Access denied',
        message: 'Token has expired' 
      });
    }

    // Handle other errors
    return res.status(401).json({ 
      error: 'Access denied',
      message: 'Token validation failed' 
    });
  }
};

module.exports = authMiddleware;
