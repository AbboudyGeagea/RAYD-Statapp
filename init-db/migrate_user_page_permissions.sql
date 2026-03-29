-- Migration: Add user_page_permissions table
CREATE TABLE IF NOT EXISTS public.user_page_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    page_key VARCHAR(50) NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (user_id, page_key)
);
