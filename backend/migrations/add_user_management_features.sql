-- Migration script to add user management columns and tables
-- Run this script on your PostgreSQL database

-- Add new columns to users table (if they don't exist)
DO $$ 
BEGIN
    -- Add tenant_id column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' AND column_name = 'tenant_id') THEN
        ALTER TABLE users ADD COLUMN tenant_id VARCHAR(36);
    END IF;
    
    -- Add status column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' AND column_name = 'status') THEN
        ALTER TABLE users ADD COLUMN status VARCHAR(20);
    END IF;
    
    -- Add last_login column
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'users' AND column_name = 'last_login') THEN
        ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
    END IF;
END $$;

-- Create index on tenant_id
CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users(tenant_id);

-- Set default values for existing users
UPDATE users 
SET status = 'active' 
WHERE status IS NULL;

-- Create user_sessions table
CREATE TABLE IF NOT EXISTS user_sessions (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    refresh_token_hash TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);

-- Update audit_logs table to support both old and new structure
DO $$
BEGIN
    -- Add new columns if they don't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'tenant_id') THEN
        ALTER TABLE audit_logs ADD COLUMN tenant_id VARCHAR(36);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'resource_type') THEN
        ALTER TABLE audit_logs ADD COLUMN resource_type VARCHAR(50);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'resource_id') THEN
        ALTER TABLE audit_logs ADD COLUMN resource_id VARCHAR(36);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'old_value') THEN
        ALTER TABLE audit_logs ADD COLUMN old_value JSONB;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'new_value') THEN
        ALTER TABLE audit_logs ADD COLUMN new_value JSONB;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'ip_address') THEN
        ALTER TABLE audit_logs ADD COLUMN ip_address VARCHAR(45);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'user_agent') THEN
        ALTER TABLE audit_logs ADD COLUMN user_agent TEXT;
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'audit_logs' AND column_name = 'created_at') THEN
        ALTER TABLE audit_logs ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    END IF;
END $$;

-- Create indexes for audit_logs new columns
CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_id ON audit_logs(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_type ON audit_logs(resource_type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_id ON audit_logs(resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at);

-- Migrate existing audit_logs data to new structure (if needed)
-- Only run if old columns exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'audit_logs' AND column_name = 'entity_type') THEN
        UPDATE audit_logs 
        SET 
            resource_type = entity_type,
            resource_id = entity_id,
            old_value = old_values,
            new_value = new_values,
            created_at = timestamp
        WHERE (resource_type IS NULL OR resource_id IS NULL) 
          AND (entity_type IS NOT NULL OR entity_id IS NOT NULL);
    END IF;
END $$;

-- Verify the changes
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns
WHERE table_name = 'users'
ORDER BY ordinal_position;

