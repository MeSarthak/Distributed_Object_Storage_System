package auth

import (
	"errors"
	"net/http"
	"strings"

	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	ContextUserID   = "user_id"
	ContextUserEmail = "user_email"
	ContextUserRole = "user_role"
	ContextClaims   = "claims"
)

// AuthMiddleware validates the JWT token from the Authorization header and attaches
// user claims to the Gin request context.
func AuthMiddleware(authService *AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, types.StandardResponse{
				Success:   false,
				Message:   "Authorization header is required",
				ErrorCode: types.ErrCodeAuthUnauthorized,
			})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, types.StandardResponse{
				Success:   false,
				Message:   "Authorization header must use Bearer <token> format",
				ErrorCode: types.ErrCodeAuthUnauthorized,
			})
			return
		}

		tokenString := strings.TrimSpace(parts[1])
		claims, err := authService.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, types.StandardResponse{
				Success:   false,
				Message:   "Invalid or expired token: " + err.Error(),
				ErrorCode: types.ErrCodeAuthUnauthorized,
			})
			return
		}

		// Inject identity into context
		c.Set(ContextUserID, claims.UserID)
		c.Set(ContextUserEmail, claims.Email)
		c.Set(ContextUserRole, claims.Role)
		c.Set(ContextClaims, claims)

		c.Next()
	}
}

// RequireRole ensures that the authenticated user possesses one of the authorized roles.
func RequireRole(allowedRoles ...types.UserRole) gin.HandlerFunc {
	roleMap := make(map[types.UserRole]struct{}, len(allowedRoles))
	for _, r := range allowedRoles {
		roleMap[r] = struct{}{}
	}

	return func(c *gin.Context) {
		roleVal, exists := c.Get(ContextUserRole)
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, types.StandardResponse{
				Success:   false,
				Message:   "User identity not found in context",
				ErrorCode: types.ErrCodeAuthUnauthorized,
			})
			return
		}

		role, ok := roleVal.(types.UserRole)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, types.StandardResponse{
				Success:   false,
				Message:   "Invalid user role in context",
				ErrorCode: types.ErrCodeAuthForbidden,
			})
			return
		}

		if _, allowed := roleMap[role]; !allowed {
			c.AbortWithStatusJSON(http.StatusForbidden, types.StandardResponse{
				Success:   false,
				Message:   "Access denied: insufficient permissions for role " + string(role),
				ErrorCode: types.ErrCodeAuthForbidden,
			})
			return
		}

		c.Next()
	}
}

// GetUserID retrieves the authenticated user's UUID from the Gin context.
func GetUserID(c *gin.Context) (uuid.UUID, error) {
	val, exists := c.Get(ContextUserID)
	if !exists {
		return uuid.Nil, errors.New("user_id not found in context")
	}
	id, ok := val.(uuid.UUID)
	if !ok {
		return uuid.Nil, errors.New("user_id in context is not a valid UUID")
	}
	return id, nil
}

// GetUserRole retrieves the authenticated user's role from the Gin context.
func GetUserRole(c *gin.Context) (types.UserRole, error) {
	val, exists := c.Get(ContextUserRole)
	if !exists {
		return "", errors.New("user_role not found in context")
	}
	role, ok := val.(types.UserRole)
	if !ok {
		return "", errors.New("user_role in context is not a valid UserRole")
	}
	return role, nil
}

// GetClaims retrieves the full JWT claims from the Gin context.
func GetClaims(c *gin.Context) (*types.JWTClaims, error) {
	val, exists := c.Get(ContextClaims)
	if !exists {
		return nil, errors.New("claims not found in context")
	}
	claims, ok := val.(*types.JWTClaims)
	if !ok {
		return nil, errors.New("claims in context is not of type *JWTClaims")
	}
	return claims, nil
}

