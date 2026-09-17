CREATE TABLE keygen (
    id UUID PRIMARY KEY,
    pub_key TEXT NOT NULL,
    ca_cert TEXT NOT NULL,
    ca_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE node_plugins (
    uuid UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    plugin_config JSONB,
    view_position INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
