package auth

import (
	"fmt"
	"net/http"

	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
)

// AuthHandler provides the HTTP endpoints for user registration, authentication,
// and token validation.
type AuthHandler struct {
	authService *AuthService
	logRepo     *database.LogRepository
}

// NewAuthHandler constructs an AuthHandler.
func NewAuthHandler(authService *AuthService, logRepo *database.LogRepository) *AuthHandler {
	return &AuthHandler{
		authService: authService,
		logRepo:     logRepo,
	}
}

// RegisterRoutes registers auth endpoints with the given Gin router.
func (h *AuthHandler) RegisterRoutes(router *gin.Engine, authMiddleware gin.HandlerFunc) {
	authGroup := router.Group("/api/auth")
	{
		authGroup.POST("/register", h.Register)
		authGroup.POST("/login", h.Login)
		authGroup.GET("/validate", authMiddleware, h.Validate)
	}
}

// Register handles POST /api/auth/register.
func (h *AuthHandler) Register(c *gin.Context) {
	var req types.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Invalid request payload: " + err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	res, err := h.authService.Register(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	if h.logRepo != nil {
		_ = h.logRepo.InsertSystemLog(
			"USER_REGISTER",
			fmt.Sprintf("User registered: %s (%s) with role %s", res.User.Name, res.User.Email, res.User.Role),
			types.SeverityInfo,
			map[string]string{
				"user_id": res.User.UserID.String(),
				"email":   res.User.Email,
				"role":    string(res.User.Role),
			},
		)
	}

	c.JSON(http.StatusCreated, types.StandardResponse{
		Success: true,
		Message: "User registered successfully",
		Data:    res,
	})
}

// Login handles POST /api/auth/login.
func (h *AuthHandler) Login(c *gin.Context) {
	var req types.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "Invalid request payload: " + err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	res, err := h.authService.Login(req)
	if err != nil {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   err.Error(),
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}

	if h.logRepo != nil {
		_ = h.logRepo.InsertSystemLog(
			"USER_LOGIN",
			fmt.Sprintf("User logged in: %s (%s)", res.User.Name, res.User.Email),
			types.SeverityInfo,
			map[string]string{
				"user_id": res.User.UserID.String(),
				"email":   res.User.Email,
			},
		)
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Login successful",
		Data:    res,
	})
}

// Validate handles GET /api/auth/validate.
// Protected by AuthMiddleware. Returns current authenticated user claims.
func (h *AuthHandler) Validate(c *gin.Context) {
	claims, err := GetClaims(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "Unauthorized",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}

	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Token is valid",
		Data: gin.H{
			"user_id": claims.UserID,
			"email":   claims.Email,
			"role":    claims.Role,
		},
	})
}

