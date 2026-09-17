-- name: GetKeygenCredentials :one
SELECT pub_key, ca_cert, ca_key
FROM keygen
ORDER BY created_at ASC
LIMIT 1;

-- name: ListNodePlugins :many
SELECT uuid, name, plugin_config, view_position, created_at, updated_at
FROM node_plugins
ORDER BY view_position ASC;

-- name: GetNodePluginByUUID :one
SELECT uuid, name, plugin_config, view_position, created_at, updated_at
FROM node_plugins
WHERE uuid = $1;

-- name: CreateNodePlugin :one
INSERT INTO node_plugins (uuid, name, plugin_config, view_position)
VALUES ($1, $2, $3, $4)
RETURNING uuid, name, plugin_config, view_position, created_at, updated_at;

-- name: DeleteNodePlugin :exec
DELETE FROM node_plugins
WHERE uuid = $1;
