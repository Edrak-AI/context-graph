export const TokenScopes = Object.freeze({
  SEND_MAIL: 'mail:send',
  FETCH_CONFIG: 'fetch:config',
  PASSWORD_RESET: 'password:reset',
  USER_LOOKUP: 'user:lookup',
  TOKEN_REFRESH: 'token:refresh',
  STORAGE_TOKEN: 'storage:token',
  CONVERSATION_CREATE: 'conversation:create',
  VALIDATE_EMAIL: 'email:validate',
  ORG_EMAIL_VERIFY: 'org:email:verify',
  EMAIL_VERIFIED: 'email:verified',
  // Edrak identity bridge: service-to-service user provisioning (users `/internal/provision`).
  USER_PROVISION: 'user:provision',
  // Edrak identity bridge: one CGraph org per Edrak tenant (org `/internal/provision`).
  ORG_PROVISION: 'org:provision',
} as const);

// Create a type for the TokenScopes keys
export type TokenScopes = (typeof TokenScopes)[keyof typeof TokenScopes];
