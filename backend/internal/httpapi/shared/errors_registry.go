package shared

import "net/http"

// This file is the single source of truth for named API errors: one place
// that pairs a stable machine-readable Code with a fixed HTTPStatus and a
// canonical Message, instead of each of the call sites across the
// codebase inventing its own free-text message and status code by hand.
//
// These use descriptive SCREAMING_SNAKE_CASE strings — idiomatic for a
// Go registry meant to be read directly (shared.ErrNodeNotFound.Code == "NODE_NOT_FOUND"),
// and self-documenting in logs/API responses.
//
// Usage at a call site:
//
//	shared.SendAPIError(w, shared.ErrNodeNotFound, cfg)                  // no underlying error
//	shared.SendAPIError(w, shared.ErrNodeNotFound.WithCause(err), cfg)   // wraps a DB/driver error
//
// This registry is additive: nothing here changes SendError's existing
// behavior, and no existing call site is required to migrate at once.
// Migrate call sites incrementally, one package at a time, verifying each
// batch — see the patch history for internal/httpapi/users as the first
// migrated package.

// Adding a new code: keep names in this format <Domain><Condition>, group
// by domain with a comment header, and never reuse or renumber an existing
// Code string once it has shipped — clients/logs may already depend on it.

// --- Generic / cross-domain -------------------------------------------------

var (
	ErrNotFound            = &APIError{StatusCode: http.StatusNotFound, Code: "NOT_FOUND", Message: "Not found"}
	ErrInternalServerError = &APIError{StatusCode: http.StatusInternalServerError, Code: "INTERNAL_SERVER_ERROR", Message: "Internal server error"}
	ErrUnauthorized        = &APIError{StatusCode: http.StatusUnauthorized, Code: "UNAUTHORIZED", Message: "Unauthorized"}
	ErrForbidden           = &APIError{StatusCode: http.StatusForbidden, Code: "FORBIDDEN", Message: "Forbidden"}
	ErrForbiddenRoleError  = &APIError{StatusCode: http.StatusForbidden, Code: "FORBIDDEN_ROLE_ERROR", Message: "Forbidden role error"}
	ErrInvalidUUID         = &APIError{StatusCode: http.StatusBadRequest, Code: "INVALID_UUID", Message: "Invalid UUID"}
	ErrInvalidJSON         = &APIError{StatusCode: http.StatusBadRequest, Code: "INVALID_JSON", Message: "Invalid JSON"}
)

// --- Users -------------------------------------------------------------

var (
	ErrUserNotFound                   = &APIError{StatusCode: http.StatusNotFound, Code: "USER_NOT_FOUND", Message: "User not found"}
	ErrUsernameAlreadyExists          = &APIError{StatusCode: http.StatusConflict, Code: "USERNAME_ALREADY_EXISTS", Message: "Username already exists"}
	ErrShortUUIDAlreadyExists         = &APIError{StatusCode: http.StatusConflict, Code: "SHORT_UUID_ALREADY_EXISTS", Message: "Short UUID already exists"}
	ErrVlessUUIDAlreadyExists         = &APIError{StatusCode: http.StatusConflict, Code: "VLESS_UUID_ALREADY_EXISTS", Message: "Vless UUID already exists"}
	ErrUserHwidDeviceExists           = &APIError{StatusCode: http.StatusConflict, Code: "USER_HWID_DEVICE_ALREADY_EXISTS", Message: "Hwid device already exists for this user"}
	ErrUserHwidDeviceLimitReached     = &APIError{StatusCode: http.StatusBadRequest, Code: "USER_HWID_DEVICE_LIMIT_REACHED", Message: "Hwid device limit reached for this user"}
	ErrHwidDeviceNotFound             = &APIError{StatusCode: http.StatusNotFound, Code: "HWID_DEVICE_NOT_FOUND", Message: "Hwid device not found"}
	ErrGetAllHwidDevicesFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_HWID_DEVICES_ERROR", Message: "Failed to fetch hwid devices"}
	ErrGetUserHwidDevicesFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_HWID_DEVICES_ERROR", Message: "Failed to fetch user hwid devices"}
	ErrCreateUserHwidDeviceFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_USER_HWID_DEVICE_ERROR", Message: "Failed to save hwid device"}
	ErrDeleteUserHwidDeviceFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_USER_HWID_DEVICE_ERROR", Message: "Failed to delete user hwid device"}
	ErrGetHwidStatsFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_HWID_STATS_ERROR", Message: "Failed to fetch hwid statistics"}
	ErrUserWriteFailed                = &APIError{StatusCode: http.StatusInternalServerError, Code: "USER_WRITE_FAILED", Message: "Failed to write user"}
	ErrGetAllUsersFailed              = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_USERS_ERROR", Message: "Get all users error"}
	ErrGetUserByError                 = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_BY_ERROR", Message: "Get user by error"}
	ErrGetUserByIDFailed              = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_BY_ID_ERROR", Message: "Get user by ID error"}
	ErrCreateUserFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_USER_ERROR", Message: "Failed to create user"}
	ErrUpdateUserFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_USER_ERROR", Message: "Failed to update user"}
	ErrDeleteUserFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_USER_ERROR", Message: "Failed to delete user"}
	ErrFetchUserTagsFailed            = &APIError{StatusCode: http.StatusInternalServerError, Code: "FETCH_USER_TAGS_ERROR", Message: "Failed to fetch user tags"}
	ErrFetchUserHistoryFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_SUBSCRIPTION_REQUEST_HISTORY_ERROR", Message: "Failed to fetch user subscription request history"}
	ErrFetchUserAccessibleNodesFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_ACCESSIBLE_NODES_ERROR", Message: "Failed to fetch user accessible nodes"}
	ErrFetchUpdatedUserFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "FETCH_UPDATED_USER_ERROR", Message: "Failed to fetch updated user"}
	ErrBulkDeleteUsersFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_DELETE_USERS_ERROR", Message: "Failed to delete users"}
	ErrBulkRevokeSubscriptionFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_REVOKE_USERS_SUBSCRIPTION_ERROR", Message: "Failed to revoke users subscription"}
	ErrBulkResetTrafficFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_RESET_USERS_TRAFFIC_ERROR", Message: "Failed to reset users traffic"}
	ErrBulkDeleteByStatusFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_DELETE_BY_STATUS_ERROR", Message: "Failed to delete users by status"}
	ErrBulkResetAllTrafficFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_RESET_ALL_USERS_TRAFFIC_ERROR", Message: "Failed to reset all users traffic"}
	ErrBulkExtendExpirationFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "BULK_EXTEND_USERS_EXPIRATION_DATE_ERROR", Message: "Failed to extend users expiration date"}
	ErrResolveUserFailed              = &APIError{StatusCode: http.StatusInternalServerError, Code: "RESOLVE_USER_ERROR", Message: "Failed to resolve user"}
)

// --- Nodes ---------------------------------------------------------------

var (
	ErrNodeNotFound             = &APIError{StatusCode: http.StatusNotFound, Code: "NODE_NOT_FOUND", Message: "Node not found"}
	ErrNodeNameAlreadyExists    = &APIError{StatusCode: http.StatusBadRequest, Code: "NODE_NAME_ALREADY_EXISTS", Message: "Node with this name already exists"}
	ErrNodeAddressAlreadyExists = &APIError{StatusCode: http.StatusBadRequest, Code: "NODE_ADDRESS_ALREADY_EXISTS", Message: "Node with this address already exists"}

	// Mirrors upstream's per-operation error codes (CREATE_NODE_ERROR,
	// UPDATE_NODE_ERROR, ...) rather than one generic 500 — the code alone
	// tells you which operation failed in logs/monitoring, no need to also
	// inspect the free-text message.
	ErrCreateNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_NODE_ERROR", Message: "Create node error"}
	ErrUpdateNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_NODE_ERROR", Message: "Update node error"}
	ErrDeleteNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_NODE_ERROR", Message: "Delete node error"}
	ErrEnableNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "ENABLE_NODE_ERROR", Message: "Enable node error"}
	ErrDisableNodeFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "DISABLE_NODE_ERROR", Message: "Disable node error"}
	ErrRestartNodeFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "RESTART_NODE_ERROR", Message: "Restart node error"}
	ErrReorderNodesFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_NODES_ERROR", Message: "Reorder nodes error"}
	ErrGetAllNodesFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_NODES_ERROR", Message: "Get all nodes error"}
	ErrGetOneNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ONE_NODE_ERROR", Message: "Get one node error"}

	// Exodus-specific node operations with no direct upstream equivalent —
	// still given their own code rather than a generic 500, for the same
	// per-operation-diagnosability reason.
	ErrFetchUpdatedNodeFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "FETCH_UPDATED_NODE_ERROR", Message: "Failed to fetch updated node"}
	ErrFetchNodeTagsFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "FETCH_NODE_TAGS_ERROR", Message: "Failed to fetch node tags"}
	ErrResetNodeTrafficFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "RESET_NODE_TRAFFIC_ERROR", Message: "Failed to reset node traffic"}
	ErrModifyNodesProfileFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "MODIFY_NODES_PROFILE_ERROR", Message: "Failed to modify nodes profile"}
	ErrNodeBulkActionFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "NODE_BULK_ACTION_ERROR", Message: "Failed to perform bulk actions"}
	ErrBuildNodeResponseFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "BUILD_NODE_RESPONSE_ERROR", Message: "Failed to build node response"}
	ErrNodesListCannotBeEmpty   = &APIError{StatusCode: http.StatusBadRequest, Code: "NODES_LIST_CANNOT_BE_EMPTY", Message: "Nodes list cannot be empty"}

	// Matches upstream's CONFIG_PROFILE_INBOUND_NOT_FOUND_IN_SPECIFIED_PROFILE (A124).
	ErrConfigProfileInboundNotFoundInProfile = &APIError{StatusCode: http.StatusNotFound, Code: "CONFIG_PROFILE_INBOUND_NOT_FOUND_IN_SPECIFIED_PROFILE", Message: "Config profile inbound not found in specified profile"}
	ErrValidateConfigProfileInboundsFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "VALIDATE_CONFIG_PROFILE_INBOUNDS_ERROR", Message: "Failed to validate config profile inbounds"}
)

// --- Config profiles -------------------------------------------------------

var (
	ErrConfigProfileNotFound          = &APIError{StatusCode: http.StatusNotFound, Code: "CONFIG_PROFILE_NOT_FOUND", Message: "Config profile not found"}
	ErrConfigProfileNameAlreadyExists = &APIError{StatusCode: http.StatusConflict, Code: "CONFIG_PROFILE_NAME_ALREADY_EXISTS", Message: "Config profile name already exists in database. Config profile names must be unique."}
	ErrInboundTagsMustBeUnique        = &APIError{StatusCode: http.StatusConflict, Code: "INBOUND_TAGS_MUST_BE_UNIQUE", Message: "Inbounds with same tag already exists in database. Inbound tags must be unique."}
	ErrSnippetNameAlreadyExists       = &APIError{StatusCode: http.StatusBadRequest, Code: "SNIPPET_NAME_ALREADY_EXISTS", Message: "Snippet name already exists"}
	ErrSnippetNotFound                = &APIError{StatusCode: http.StatusNotFound, Code: "SNIPPET_NOT_FOUND", Message: "Snippet not found"}

	ErrGetConfigProfilesFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_CONFIG_PROFILES_ERROR", Message: "Get config profiles error"}
	ErrGetConfigProfileByUUIDFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_CONFIG_PROFILE_BY_UUID_ERROR", Message: "Get config profile by uuid error"}
	ErrGetComputedConfigProfileFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_COMPUTED_CONFIG_PROFILE_BY_UUID_ERROR", Message: "Get computed config profile by uuid error"}
	ErrCreateConfigProfileFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_CONFIG_PROFILE_ERROR", Message: "Create config profile error"}
	ErrUpdateConfigProfileFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_CONFIG_PROFILE_ERROR", Message: "Update config profile error"}
	ErrDeleteConfigProfileFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_CONFIG_PROFILE_ERROR", Message: "Failed to delete config profile"}
	ErrBuildConfigProfileResponseFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "BUILD_CONFIG_PROFILE_RESPONSE_ERROR", Message: "Failed to build config profile response"}
	ErrReorderConfigProfilesFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_CONFIG_PROFILES_ERROR", Message: "Reorder config profiles error"}
	ErrReservedConfigProfileName        = &APIError{StatusCode: http.StatusBadRequest, Code: "RESERVED_CONFIG_PROFILE_NAME", Message: "Config profile name is reserved"}
)

// --- External squads ---------------------------------------------------

var (
	ErrExternalSquadNotFound              = &APIError{StatusCode: http.StatusNotFound, Code: "EXTERNAL_SQUAD_NOT_FOUND", Message: "External squad not found"}
	ErrExternalSquadNameAlreadyExists     = &APIError{StatusCode: http.StatusConflict, Code: "EXTERNAL_SQUAD_NAME_ALREADY_EXISTS", Message: "External squad name already exists"}
	ErrGetExternalSquadsFailed            = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_EXTERNAL_SQUADS_ERROR", Message: "Get external squads error"}
	ErrGetExternalSquadByUUIDFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_EXTERNAL_SQUAD_BY_UUID_ERROR", Message: "Get external squad by uuid error"}
	ErrCreateExternalSquadFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_EXTERNAL_SQUAD_ERROR", Message: "Create external squad error"}
	ErrUpdateExternalSquadFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_EXTERNAL_SQUAD_ERROR", Message: "Update external squad error"}
	ErrDeleteExternalSquadFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_EXTERNAL_SQUAD_ERROR", Message: "Delete external squad error"}
	ErrAddUsersToExternalSquadFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "ADD_USERS_TO_EXTERNAL_SQUAD_ERROR", Message: "Add users to external squad error"}
	ErrRemoveUsersFromExternalSquadFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "REMOVE_USERS_FROM_EXTERNAL_SQUAD_ERROR", Message: "Remove users from external squad error"}
	ErrReorderExternalSquadsFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_EXTERNAL_SQUADS_ERROR", Message: "Failed to reorder external squads"}
)

// --- Internal squads ---------------------------------------------------

var (
	ErrInternalSquadNotFound                 = &APIError{StatusCode: http.StatusNotFound, Code: "INTERNAL_SQUAD_NOT_FOUND", Message: "Internal squad not found"}
	ErrInternalSquadNameAlreadyExists        = &APIError{StatusCode: http.StatusConflict, Code: "INTERNAL_SQUAD_NAME_ALREADY_EXISTS", Message: "Internal squad name already exists"}
	ErrReservedInternalSquadName             = &APIError{StatusCode: http.StatusBadRequest, Code: "RESERVED_INTERNAL_SQUAD_NAME", Message: "Internal squad name is reserved"}
	ErrGetInternalSquadsFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INTERNAL_SQUADS_ERROR", Message: "Get internal squads error"}
	ErrGetInternalSquadByUUIDFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INTERNAL_SQUAD_BY_UUID_ERROR", Message: "Get internal squad by uuid error"}
	ErrCreateInternalSquadFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_INTERNAL_SQUAD_ERROR", Message: "Create internal squad error"}
	ErrUpdateInternalSquadFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_INTERNAL_SQUAD_ERROR", Message: "Update internal squad error"}
	ErrDeleteInternalSquadFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_INTERNAL_SQUAD_ERROR", Message: "Delete internal squad error"}
	ErrAddUsersToInternalSquadFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "ADD_USERS_TO_INTERNAL_SQUAD_ERROR", Message: "Add users to internal squad error"}
	ErrRemoveUsersFromInternalSquadFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "REMOVE_USERS_FROM_INTERNAL_SQUAD_ERROR", Message: "Remove users from internal squad error"}
	ErrGetInternalSquadAccessibleNodesFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INTERNAL_SQUAD_ACCESSIBLE_NODES_ERROR", Message: "Get internal squad accessible nodes error"}
	ErrReorderInternalSquadsFailed           = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_INTERNAL_SQUADS_ERROR", Message: "Failed to reorder internal squads"}
)

// --- Hosts -------------------------------------------------------------

var (
	ErrHostNotFound            = &APIError{StatusCode: http.StatusNotFound, Code: "HOST_NOT_FOUND", Message: "Host not found"}
	ErrHostRemarkAlreadyExists = &APIError{StatusCode: http.StatusBadRequest, Code: "HOST_REMARK_ALREADY_EXISTS", Message: "Host remark already exists"}
	ErrGetAllHostsFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_HOSTS_ERROR", Message: "Get all hosts error"}
	ErrGetOneHostFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ONE_HOST_ERROR", Message: "Get one host error"}
	ErrCreateHostFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_HOST_ERROR", Message: "Create host error"}
	ErrUpdateHostFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_HOST_ERROR", Message: "Update host error"}
	ErrDeleteHostFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_HOST_ERROR", Message: "Delete host error"}
	ErrGetAllHostTagsFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_HOST_TAGS_ERROR", Message: "Get all host tags error"}
)

// --- SRS Lists ---------------------------------------------------------

var (
	ErrSrsListNotFound          = &APIError{StatusCode: http.StatusNotFound, Code: "SRS_LIST_NOT_FOUND", Message: "SRS list not found"}
	ErrSrsListNameAlreadyExists = &APIError{StatusCode: http.StatusConflict, Code: "SRS_LIST_NAME_ALREADY_EXISTS", Message: "SRS list name already exists"}
	ErrGetSrsListsFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SRS_LISTS_ERROR", Message: "Get SRS lists error"}
	ErrGetSrsListByUUIDFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SRS_LIST_BY_UUID_ERROR", Message: "Get SRS list by uuid error"}
	ErrCreateSrsListFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_SRS_LIST_ERROR", Message: "Create SRS list error"}
	ErrUpdateSrsListFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_SRS_LIST_ERROR", Message: "Update SRS list error"}
	ErrDeleteSrsListFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_SRS_LIST_ERROR", Message: "Delete SRS list error"}
	ErrReorderSrsListsFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_SRS_LISTS_ERROR", Message: "Failed to reorder SRS lists"}
	ErrCreateSRSListFailed      = ErrCreateSrsListFailed
	ErrUpdateSRSListFailed      = ErrUpdateSrsListFailed
	ErrDeleteSRSListFailed      = ErrDeleteSrsListFailed
	ErrReorderSRSListsFailed    = ErrReorderSrsListsFailed
	ErrGetAllSRSListsFailed     = ErrGetSrsListsFailed
)

// --- Subscription Page Configs -----------------------------------------

var (
	ErrSubpageConfigNotFound              = &APIError{StatusCode: http.StatusNotFound, Code: "SUBSCRIPTION_PAGE_CONFIG_NOT_FOUND", Message: "Subscription page config not found"}
	ErrSubpageConfigNameAlreadyExists     = &APIError{StatusCode: http.StatusConflict, Code: "SUBSCRIPTION_PAGE_CONFIG_NAME_ALREADY_EXISTS", Message: "Subscription page config name already exists"}
	ErrReservedSubpageConfigCantBeDeleted = &APIError{StatusCode: http.StatusBadRequest, Code: "RESERVED_SUBPAGE_CONFIG_CANT_BE_DELETED", Message: "Reserved subscription page config cannot be deleted"}
	ErrGetAllSubpageConfigsFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_SUBSCRIPTION_PAGE_CONFIGS_ERROR", Message: "Get all subscription page configs error"}
	ErrGetSubpageConfigByUUIDFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SUBSCRIPTION_PAGE_CONFIG_BY_UUID_ERROR", Message: "Get subscription page config by uuid error"}
	ErrCreateSubpageConfigFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_SUBSCRIPTION_PAGE_CONFIG_ERROR", Message: "Create subscription page config error"}
	ErrUpdateSubpageConfigFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_SUBSCRIPTION_PAGE_CONFIG_ERROR", Message: "Update subscription page config error"}
	ErrDeleteSubpageConfigFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_SUBSCRIPTION_PAGE_CONFIG_ERROR", Message: "Delete subscription page config error"}
	ErrReorderSubpageConfigsFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_SUBSCRIPTION_PAGE_CONFIGS_ERROR", Message: "Reorder subscription page configs error"}
	ErrCloneSubpageConfigFailed           = &APIError{StatusCode: http.StatusInternalServerError, Code: "CLONE_SUBSCRIPTION_PAGE_CONFIG_ERROR", Message: "Clone subscription page config error"}
)

// --- Subscription Templates --------------------------------------------

var (
	ErrSubTemplateNotFound                                 = &APIError{StatusCode: http.StatusNotFound, Code: "SUBSCRIPTION_TEMPLATE_NOT_FOUND", Message: "Subscription template not found"}
	ErrSubTemplateNameAlreadyExists                        = &APIError{StatusCode: http.StatusConflict, Code: "TEMPLATE_NAME_ALREADY_EXISTS_FOR_THIS_TYPE", Message: "Template name already exists for this type"}
	ErrSubTemplateNameAlreadyExistsForThisType             = &APIError{StatusCode: http.StatusConflict, Code: "TEMPLATE_NAME_ALREADY_EXISTS_FOR_THIS_TYPE", Message: "Template name already exists for this type"}
	ErrReservedSubTemplateName                             = &APIError{StatusCode: http.StatusBadRequest, Code: "RESERVED_TEMPLATE_NAME", Message: "Template name is reserved"}
	ErrReservedSubTemplateCannotBeDeleted                  = &APIError{StatusCode: http.StatusBadRequest, Code: "RESERVED_TEMPLATE_CANNOT_BE_DELETED", Message: "Reserved template cannot be deleted"}
	ErrSubTemplateTypeNotAllowed                           = &APIError{StatusCode: http.StatusBadRequest, Code: "TEMPLATE_TYPE_NOT_ALLOWED", Message: "Template type not allowed"}
	ErrSubTemplateJsonNotAllowedForYaml                    = &APIError{StatusCode: http.StatusBadRequest, Code: "TEMPLATE_JSON_NOT_ALLOWED_FOR_YAML_TEMPLATE", Message: "Template JSON is not allowed for YAML template"}
	ErrSubTemplateYamlNotAllowedForJson                    = &APIError{StatusCode: http.StatusBadRequest, Code: "TEMPLATE_YAML_NOT_ALLOWED_FOR_JSON_TEMPLATE", Message: "Template YAML is not allowed for JSON template"}
	ErrSubTemplateJsonAndYamlCannotBeUpdatedSimultaneously = &APIError{StatusCode: http.StatusBadRequest, Code: "TEMPLATE_JSON_AND_YAML_CANNOT_BE_UPDATED_SIMULTANEOUSLY", Message: "templateJson and encodedTemplateYaml cannot be updated simultaneously"}
	ErrGetAllSubTemplatesFailed                            = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_SUBSCRIPTION_TEMPLATES_ERROR", Message: "Get all subscription templates error"}
	ErrGetSubTemplateByUUIDFailed                          = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SUBSCRIPTION_TEMPLATE_BY_UUID_ERROR", Message: "Get subscription template by uuid error"}
	ErrCreateSubTemplateFailed                             = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_SUBSCRIPTION_TEMPLATE_ERROR", Message: "Create subscription template error"}
	ErrUpdateSubTemplateFailed                             = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_SUBSCRIPTION_TEMPLATE_ERROR", Message: "Update subscription template error"}
	ErrDeleteSubTemplateFailed                             = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_SUBSCRIPTION_TEMPLATE_ERROR", Message: "Delete subscription template error"}
)

// --- Subscription Connections (Nodes) ----------------------------------

var (
	// Backward-compatibility aliases
	ErrSubConnectionNotFound      = ErrNodeNotFound
	ErrGetAllSubConnectionsFailed = ErrGetAllNodesFailed
	ErrDeleteSubConnectionFailed  = ErrDeleteNodeFailed
)

// --- Panel Settings & Keygen -------------------------------------------

var (
	ErrGetPanelSettingsFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_PANEL_SETTINGS_ERROR", Message: "Get panel settings error"}
	ErrUpdatePanelSettingsFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_PANEL_SETTINGS_ERROR", Message: "Update panel settings error"}
	ErrGetPublicKeyFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_PUBLIC_KEY_ERROR", Message: "Get public key error"}
)

// --- Node Plugins & Shared Lists ---------------------------------------

var (
	ErrNodePluginNotFound       = &APIError{StatusCode: http.StatusNotFound, Code: "NODE_PLUGIN_NOT_FOUND", Message: "Node plugin not found"}
	ErrGetNodePluginsFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODE_PLUGINS_ERROR", Message: "Failed to load node plugins"}
	ErrCreateNodePluginFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_NODE_PLUGIN_ERROR", Message: "Failed to create node plugin"}
	ErrUpdateNodePluginFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_NODE_PLUGIN_ERROR", Message: "Failed to update node plugin"}
	ErrDeleteNodePluginFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_NODE_PLUGIN_ERROR", Message: "Failed to delete node plugin"}
	ErrReorderNodePluginsFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_NODE_PLUGINS_ERROR", Message: "Failed to reorder node plugins"}
	ErrCloneNodePluginFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "CLONE_NODE_PLUGIN_ERROR", Message: "Failed to clone node plugin"}
	ErrSyncNodePluginFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "SYNC_NODE_PLUGIN_ERROR", Message: "Failed to sync node plugin"}
	ErrExecuteNodePluginFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "EXECUTE_NODE_PLUGIN_ERROR", Message: "Failed to execute node plugin"}
	ErrSharedListNotFound       = &APIError{StatusCode: http.StatusNotFound, Code: "SHARED_LIST_NOT_FOUND", Message: "Shared list not found"}
	ErrGetAllSharedListsFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_ALL_SHARED_LISTS_ERROR", Message: "Failed to fetch shared lists"}
	ErrCreateSharedListFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_SHARED_LIST_ERROR", Message: "Failed to create shared list"}
	ErrUpdateSharedListFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_SHARED_LIST_ERROR", Message: "Failed to update shared list"}
	ErrDeleteSharedListFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_SHARED_LIST_ERROR", Message: "Failed to delete shared list"}
)

// --- Node Integrations -------------------------------------------------

var (
	ErrNodeIntegrationNotFound        = &APIError{StatusCode: http.StatusNotFound, Code: "NODE_INTEGRATION_NOT_FOUND", Message: "Node integration not found"}
	ErrGetNodeIntegrationsFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODE_INTEGRATIONS_ERROR", Message: "Failed to fetch node integrations"}
	ErrGetNodeIntegrationByUUIDFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODE_INTEGRATION_BY_UUID_ERROR", Message: "Failed to fetch node integration"}
	ErrCreateNodeIntegrationFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_NODE_INTEGRATION_ERROR", Message: "Failed to create node integration"}
	ErrUpdateNodeIntegrationFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_NODE_INTEGRATION_ERROR", Message: "Failed to update node integration"}
	ErrDeleteNodeIntegrationFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_NODE_INTEGRATION_ERROR", Message: "Failed to delete node integration"}
)

// --- Infra Billing -----------------------------------------------------

var (
	ErrInfraProviderNotFound                 = &APIError{StatusCode: http.StatusNotFound, Code: "INFRA_PROVIDER_NOT_FOUND", Message: "Infra provider not found"}
	ErrCreateInfraProviderFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_INFRA_PROVIDER_ERROR", Message: "Create infra provider error"}
	ErrUpdateInfraProviderFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_INFRA_PROVIDER_ERROR", Message: "Update infra provider error"}
	ErrDeleteInfraProviderFailed             = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_INFRA_PROVIDER_ERROR", Message: "Delete infra provider error"}
	ErrGetInfraProvidersFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INFRA_PROVIDERS_ERROR", Message: "Get infra providers error"}
	ErrGetInfraProviderByUUIDFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INFRA_PROVIDER_BY_UUID_ERROR", Message: "Get infra provider by uuid error"}
	ErrCreateInfraBillingNodeFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_INFRA_BILLING_NODE_ERROR", Message: "Create infra billing node error"}
	ErrUpdateInfraBillingNodeFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_INFRA_BILLING_NODE_ERROR", Message: "Update infra billing node error"}
	ErrDeleteInfraBillingNodeFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_INFRA_BILLING_NODE_BY_UUID_ERROR", Message: "Delete infra billing node by UUID error"}
	ErrGetBillingNodesFailed                 = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_BILLING_NODES_ERROR", Message: "Get billing nodes error"}
	ErrCreateInfraBillingHistoryRecordFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_INFRA_BILLING_HISTORY_RECORD_ERROR", Message: "Create infra billing history record error"}
	ErrGetInfraBillingHistoryRecordsFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_INFRA_BILLING_HISTORY_RECORDS_ERROR", Message: "Get infra billing history records error"}
	ErrDeleteInfraBillingHistoryRecordFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_INFRA_BILLING_HISTORY_RECORD_BY_UUID_ERROR", Message: "Delete infra billing history record by UUID error"}
)

// --- System & Bandwidth Stats ------------------------------------------

var (
	ErrGetSystemStatsFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SYSTEM_STATS_ERROR", Message: "Failed to read system statistics"}
	ErrGetSystemRecapFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SYSTEM_RECAP_ERROR", Message: "Failed to read system recap"}
	ErrGetBandwidthStatsFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_BANDWIDTH_STATS_ERROR", Message: "Failed to read bandwidth statistics"}
	ErrGetNodesRealtimeUsageFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODES_REALTIME_USAGE_ERROR", Message: "Failed to fetch nodes realtime usage"}
	ErrGetNodesSparklineFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODES_SPARKLINE_ERROR", Message: "Failed to fetch nodes sparkline"}
	ErrGetNodesUsageFailed         = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODES_USAGE_BY_RANGE_ERROR", Message: "Failed to fetch nodes usage"}
	ErrGetTopNodesFailed           = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_TOP_NODES_ERROR", Message: "Failed to fetch top nodes"}
	ErrGetNodeUsersSparklineFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_NODE_USERS_SPARKLINE_ERROR", Message: "Failed to fetch node users sparkline"}
	ErrGetTopUsersFailed           = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_TOP_USERS_ERROR", Message: "Failed to fetch top users"}
	ErrGetUserSparklineFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_SPARKLINE_ERROR", Message: "Failed to fetch user sparkline"}
	ErrGetUserNodesSeriesFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_NODES_SERIES_ERROR", Message: "Failed to fetch user nodes series"}
	ErrGetUserTopNodesFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_TOP_NODES_ERROR", Message: "Failed to fetch user top nodes"}
	ErrGetUserStatsFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_USER_STATS_ERROR", Message: "Failed to fetch user statistics"}
	ErrGenerateX25519KeypairFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GENERATE_X25519_KEYPAIR_ERROR", Message: "Failed to generate x25519 keypair"}
	ErrEncryptHappCryptoLinkFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "ENCRYPT_HAPP_CRYPTO_LINK_ERROR", Message: "Failed to encrypt Happ crypto link"}
)

// --- Subscription Delivery & Public -----------------------------------

var (
	ErrSubscriptionSettingsNotFound        = &APIError{StatusCode: http.StatusNotFound, Code: "SUBSCRIPTION_SETTINGS_NOT_FOUND", Message: "Subscription settings not found"}
	ErrGetSubscriptionSettingsFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SUBSCRIPTION_SETTINGS_ERROR", Message: "Failed to load subscription settings"}
	ErrUpdateSubscriptionSettingsFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_SUBSCRIPTION_SETTINGS_ERROR", Message: "Failed to update subscription settings"}
	ErrRenderSubscriptionFailed            = &APIError{StatusCode: http.StatusInternalServerError, Code: "RENDER_SUBSCRIPTION_ERROR", Message: "Failed to render subscription"}
	ErrNoActiveHostsAvailable              = &APIError{StatusCode: http.StatusNotFound, Code: "NO_ACTIVE_HOSTS_AVAILABLE", Message: "No active hosts available"}
	ErrUserIsDisabledOrExpired             = &APIError{StatusCode: http.StatusForbidden, Code: "USER_IS_DISABLED_OR_EXPIRED", Message: "User is disabled or expired"}
	ErrCheckHwidDeviceLimitFailed          = &APIError{StatusCode: http.StatusInternalServerError, Code: "CHECK_HWID_DEVICE_LIMIT_ERROR", Message: "Failed to check hwid device limit"}
	ErrGenerateShadowsocksLinkFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GENERATE_SHADOWSOCKS_LINK_ERROR", Message: "Failed to generate shadowsocks link"}
	ErrGetSubpageConfigFailed              = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SUBPAGE_CONFIG_ERROR", Message: "Failed to get subpage config"}
	ErrReorderSubscriptionTemplatesFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "REORDER_SUBSCRIPTION_TEMPLATES_ERROR", Message: "Failed to reorder subscription templates"}
	ErrGetSubscriptionRequestHistoryFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_SUBSCRIPTION_REQUEST_HISTORY_ERROR", Message: "Failed to fetch subscription request history"}
)

// --- Keygen / Keys -----------------------------------------------------

var (
	ErrGetKeygenDataFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_KEYGEN_DATA_ERROR", Message: "Failed to fetch keygen data"}
	ErrGenerateNodeCertFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GENERATE_NODE_CERTIFICATE_ERROR", Message: "Failed to generate node certificate"}
	ErrEncodeSecretPayloadFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "ENCODE_SECRET_PAYLOAD_ERROR", Message: "Failed to encode secret payload"}
	ErrGenerateGRPCTokenFailed   = &APIError{StatusCode: http.StatusInternalServerError, Code: "GENERATE_GRPC_AUTH_TOKEN_ERROR", Message: "Failed to generate grpc auth token"}
)

// --- Passkeys ----------------------------------------------------------

var (
	ErrPasskeyNotFound          = &APIError{StatusCode: http.StatusNotFound, Code: "PASSKEY_NOT_FOUND", Message: "Passkey not found"}
	ErrGetPasskeysFailed        = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_PASSKEYS_ERROR", Message: "Failed to fetch passkeys"}
	ErrUpdatePasskeyFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_PASSKEY_ERROR", Message: "Failed to update passkey"}
	ErrDeletePasskeyFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_PASSKEY_ERROR", Message: "Failed to delete passkey"}
	ErrPasskeysNotEnabled       = &APIError{StatusCode: http.StatusForbidden, Code: "PASSKEYS_NOT_ENABLED", Message: "Passkeys not enabled"}
	ErrPasskeysNotConfigured    = &APIError{StatusCode: http.StatusForbidden, Code: "PASSKEYS_NOT_CONFIGURED", Message: "Passkeys not configured"}
	ErrPasskeyChallengeNotFound = &APIError{StatusCode: http.StatusForbidden, Code: "PASSKEY_CHALLENGE_NOT_FOUND", Message: "Challenge not found or expired"}
	ErrPasskeySetupFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "PASSKEY_SETUP_ERROR", Message: "Passkey setup error"}
)

// --- Metadata ----------------------------------------------------------

var (
	ErrGetMetadataFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_METADATA_ERROR", Message: "Failed to fetch metadata"}
	ErrUpdateMetadataFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "UPDATE_METADATA_ERROR", Message: "Failed to update metadata"}
)

// --- Auth & API Tokens -------------------------------------------------

var (
	ErrAdminNotFound             = &APIError{StatusCode: http.StatusNotFound, Code: "ADMIN_NOT_FOUND", Message: "Admin not found"}
	ErrAdminAlreadyExists        = &APIError{StatusCode: http.StatusConflict, Code: "ADMIN_ALREADY_EXISTS", Message: "Admin account already exists"}
	ErrRegistrationNotAllowed    = &APIError{StatusCode: http.StatusForbidden, Code: "REGISTRATION_NOT_ALLOWED", Message: "Registration is not allowed"}
	ErrLoginNotAllowed           = &APIError{StatusCode: http.StatusForbidden, Code: "LOGIN_NOT_ALLOWED", Message: "Login is not allowed"}
	ErrInvalidCredentials        = &APIError{StatusCode: http.StatusBadRequest, Code: "INVALID_CREDENTIALS", Message: "Invalid credentials"}
	ErrCreateAdminAccountFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_ADMIN_ACCOUNT_ERROR", Message: "Failed to create admin account"}
	ErrCheckAdminStatusFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "CHECK_ADMIN_STATUS_ERROR", Message: "Failed to check admin status"}
	ErrGetAuthBootstrapFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_AUTH_BOOTSTRAP_ERROR", Message: "Failed to get auth bootstrap"}
	ErrGetAuthStatusFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "GET_AUTH_STATUS_ERROR", Message: "Failed to get auth status"}
	ErrValidateCredentialsFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "VALIDATE_CREDENTIALS_ERROR", Message: "Failed to validate credentials"}
	ErrCreateAuthTokenFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_AUTH_TOKEN_ERROR", Message: "Failed to create auth token"}
	ErrLoadCurrentSessionFailed  = &APIError{StatusCode: http.StatusInternalServerError, Code: "LOAD_CURRENT_SESSION_ERROR", Message: "Failed to load current session"}
	ErrLoginFailed               = &APIError{StatusCode: http.StatusInternalServerError, Code: "LOGIN_ERROR", Message: "Login error"}
	ErrForbiddenRole             = &APIError{StatusCode: http.StatusForbidden, Code: "FORBIDDEN_ROLE_ERROR", Message: "Forbidden role error"}
	ErrRequestedTokenNotFound    = &APIError{StatusCode: http.StatusNotFound, Code: "REQUESTED_TOKEN_NOT_FOUND", Message: "Requested token not found"}
	ErrApiTokenNotFound          = &APIError{StatusCode: http.StatusNotFound, Code: "API_TOKEN_NOT_FOUND", Message: "API token not found"}
	ErrAPITokenNotFound          = &APIError{StatusCode: http.StatusNotFound, Code: "API_TOKEN_NOT_FOUND", Message: "API token not found"}
	ErrInvalidApiTokenScope      = &APIError{StatusCode: http.StatusBadRequest, Code: "INVALID_API_TOKEN_SCOPE", Message: "One or more provided API token scopes are invalid"}
	ErrCreateApiTokenFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "CREATE_API_TOKEN_ERROR", Message: "Create API token error"}
	ErrDeleteApiTokenFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "DELETE_API_TOKEN_ERROR", Message: "Delete API token error"}
	ErrFindAllApiTokensFailed    = &APIError{StatusCode: http.StatusInternalServerError, Code: "FIND_ALL_API_TOKENS_ERROR", Message: "Find all API tokens error"}
	ErrIssueOttTokenFailed       = &APIError{StatusCode: http.StatusInternalServerError, Code: "ISSUE_OTT_TOKEN_ERROR", Message: "Failed to issue OTT token"}
	ErrOAuth2ProviderNotFound    = &APIError{StatusCode: http.StatusNotFound, Code: "OAUTH2_PROVIDER_NOT_FOUND", Message: "OAuth2 provider not found"}
	ErrOAuth2AuthorizeFailed     = &APIError{StatusCode: http.StatusInternalServerError, Code: "OAUTH2_AUTHORIZE_ERROR", Message: "OAuth2 authorize error"}
	ErrOAuth2CallbackFailed      = &APIError{StatusCode: http.StatusInternalServerError, Code: "OAUTH2_CALLBACK_ERROR", Message: "OAuth2 callback error"}
	ErrOAuth2ProviderDisabled    = &APIError{StatusCode: http.StatusForbidden, Code: "OAUTH2_PROVIDER_DISABLED", Message: "OAuth2 provider is disabled"}
	ErrOAuth2StateMismatch       = &APIError{StatusCode: http.StatusForbidden, Code: "OAUTH2_STATE_MISMATCH", Message: "OAuth2 state mismatch"}
	ErrOAuth2PrincipalNotAllowed = &APIError{StatusCode: http.StatusForbidden, Code: "OAUTH2_PRINCIPAL_NOT_ALLOWED", Message: "OAuth2 principal is not allowed"}
	ErrOAuth2SessionCreateFailed = &APIError{StatusCode: http.StatusInternalServerError, Code: "OAUTH2_SESSION_CREATE_FAILED", Message: "Failed to create OAuth2 session"}
)
