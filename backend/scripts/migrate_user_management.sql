-- User Management schema extension for CDC pipeline application
-- Run this against your PostgreSQL database to add roles, user_roles, invitations
-- and extend users table (is_external, nullable hashed_password).

-- 1) Make users.hashed_password nullable (for PENDING/invited users)
ALTER TABLE users ALTER COLUMN hashed_password DROP NOT NULL;

-- 2) Add is_external to users if not exists
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'users' AND column_name = 'is_external'
  ) THEN
    ALTER TABLE users ADD COLUMN is_external BOOLEAN NOT NULL DEFAULT false;
  END IF;
END $$;

-- 3) Create roles table
CREATE TABLE IF NOT EXISTS roles (
  id VARCHAR(36) PRIMARY KEY,
  name VARCHAR(100) UNIQUE NOT NULL,
  description TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_roles_name ON roles(name);

-- 4) Create user_roles table
CREATE TABLE IF NOT EXISTS user_roles (
  id VARCHAR(36) PRIMARY KEY,
  user_id VARCHAR(36) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role_id VARCHAR(36) NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
  workspace_id VARCHAR(36),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_roles(role_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_user_workspace ON user_roles(user_id, workspace_id);

-- 5) Create invitation status type and invitations table
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'invitation_status_enum') THEN
    CREATE TYPE invitation_status_enum AS ENUM ('PENDING', 'ACCEPTED', 'EXPIRED');
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS invitations (
  id VARCHAR(36) PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  invited_by VARCHAR(36) REFERENCES users(id) ON DELETE SET NULL,
  token VARCHAR(255) UNIQUE NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
  role_name VARCHAR(50),
  workspace_id VARCHAR(36),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  accepted_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_invitations_email ON invitations(email);
CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations(token);
CREATE INDEX IF NOT EXISTS idx_invitations_status ON invitations(status);
CREATE INDEX IF NOT EXISTS idx_invitations_expires_at ON invitations(expires_at);
