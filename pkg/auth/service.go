package auth

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

// UserStore defines the data access contract required by AuthService.
// database.UserRepository satisfies this interface.
type UserStore interface {
	CreateUser(user *types.User) error
	GetUserByEmail(email string) (*types.User, error)
	GetUserByID(userID uuid.UUID) (*types.User, error)
	EmailExists(email string) (bool, error)
}

// AuthService handles registration, password hashing, credential verification,
// and JWT token generation & verification.
type AuthService struct {
	userStore UserStore
	cfg       config.AuthConfig
}

// NewAuthService constructs an AuthService.
func NewAuthService(userStore UserStore, cfg config.AuthConfig) *AuthService {
	if cfg.BcryptCost == 0 {
		cfg.BcryptCost = 12
	}
	if cfg.ExpirationHours == 0 {
		cfg.ExpirationHours = 24
	}
	return &AuthService{
		userStore: userStore,
		cfg:       cfg,
	}
}

// Register registers a new user with bcrypt password hashing and returns an AuthResponse.
func (s *AuthService) Register(req types.RegisterRequest) (*types.AuthResponse, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, errors.New("name is required")
	}

	email := strings.ToLower(strings.TrimSpace(req.Email))
	if email == "" {
		return nil, errors.New("email is required")
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return nil, fmt.Errorf("invalid email format: %w", err)
	}

	if len(req.Password) < 6 {
		return nil, errors.New("password must be at least 6 characters")
	}

	// Validate or default role
	role := req.Role
	if role == "" {
		role = types.RoleUser
	} else if role != types.RoleUser && role != types.RoleAdmin {
		return nil, fmt.Errorf("invalid role %s: must be USER or ADMIN", role)
	}

	// Check if email already registered
	exists, err := s.userStore.EmailExists(email)
	if err != nil {
		return nil, fmt.Errorf("check email: %w", err)
	}
	if exists {
		return nil, errors.New("email is already registered")
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	user := &types.User{
		UserID:       uuid.New(),
		Name:         name,
		Email:        email,
		PasswordHash: string(hashedPassword),
		Role:         role,
		CreatedAt:    time.Now().UTC(),
	}

	if err := s.userStore.CreateUser(user); err != nil {
		return nil, fmt.Errorf("save user: %w", err)
	}

	// Generate JWT
	token, err := s.generateToken(user)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	// Return safe user copy without password hash
	safeUser := *user
	safeUser.PasswordHash = ""

	return &types.AuthResponse{
		Token: token,
		User:  safeUser,
	}, nil
}

// Login authenticates a user by email and password, returning an AuthResponse.
func (s *AuthService) Login(req types.LoginRequest) (*types.AuthResponse, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	if email == "" || req.Password == "" {
		return nil, errors.New("email and password are required")
	}

	user, err := s.userStore.GetUserByEmail(email)
	if err != nil {
		// Do not reveal whether email exists for security
		return nil, errors.New("invalid email or password")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid email or password")
	}

	token, err := s.generateToken(user)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	safeUser := *user
	safeUser.PasswordHash = ""

	return &types.AuthResponse{
		Token: token,
		User:  safeUser,
	}, nil
}

// ValidateToken parses and validates a JWT token string.
func (s *AuthService) ValidateToken(tokenString string) (*types.JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &types.JWTClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(s.cfg.JWTSecret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*types.JWTClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return claims, nil
}

// generateToken creates a signed HMAC-SHA256 JWT for the user.
func (s *AuthService) generateToken(user *types.User) (string, error) {
	now := time.Now().UTC()
	claims := types.JWTClaims{
		UserID: user.UserID,
		Email:  user.Email,
		Role:   user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(s.cfg.ExpirationHours) * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Subject:   user.UserID.String(),
			Issuer:    "distributed-storage-coordinator",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.cfg.JWTSecret))
}

