package database

import (
	"database/sql"
	"fmt"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// UserRepository provides typed database query methods for the users table.
type UserRepository struct {
	db *DB
}

// NewUserRepository constructs a UserRepository.
func NewUserRepository(db *DB) *UserRepository {
	return &UserRepository{db: db}
}

// CreateUser inserts a new user record into the users table.
func (r *UserRepository) CreateUser(user *types.User) error {
	query := `
		INSERT INTO users (user_id, name, email, password_hash, role, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	if user.UserID == uuid.Nil {
		user.UserID = uuid.New()
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now().UTC()
	}

	_, err := r.db.Exec(query,
		user.UserID,
		user.Name,
		user.Email,
		user.PasswordHash,
		string(user.Role),
		user.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create user %s (%s): %w", user.Name, user.Email, err)
	}
	return nil
}

// GetUserByEmail retrieves a user by their unique email address.
func (r *UserRepository) GetUserByEmail(email string) (*types.User, error) {
	query := `
		SELECT user_id, name, email, password_hash, role, created_at
		FROM users
		WHERE email = $1
	`
	row := r.db.QueryRow(query, email)
	u, err := scanUser(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user with email %s not found: %w", email, err)
	}
	if err != nil {
		return nil, fmt.Errorf("get user by email %s: %w", email, err)
	}
	return u, nil
}

// GetUserByID retrieves a user by their unique user UUID.
func (r *UserRepository) GetUserByID(userID uuid.UUID) (*types.User, error) {
	query := `
		SELECT user_id, name, email, password_hash, role, created_at
		FROM users
		WHERE user_id = $1
	`
	row := r.db.QueryRow(query, userID)
	u, err := scanUser(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user %s not found: %w", userID, err)
	}
	if err != nil {
		return nil, fmt.Errorf("get user by id %s: %w", userID, err)
	}
	return u, nil
}

// EmailExists checks whether a user already exists with the given email.
func (r *UserRepository) EmailExists(email string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)`
	var exists bool
	err := r.db.QueryRow(query, email).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check email exists %s: %w", email, err)
	}
	return exists, nil
}

// scanUser scans a single *sql.Row into a types.User struct.
func scanUser(row *sql.Row) (*types.User, error) {
	var u types.User
	var roleStr string
	var createdAt time.Time

	err := row.Scan(
		&u.UserID,
		&u.Name,
		&u.Email,
		&u.PasswordHash,
		&roleStr,
		&createdAt,
	)
	if err != nil {
		return nil, err
	}
	u.Role = types.UserRole(roleStr)
	u.CreatedAt = createdAt
	return &u, nil
}

