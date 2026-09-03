// Optional Sentry error reporting for the Node gateway (Edrak fork). Must be imported before any
// other module so the SDK can patch http/express. Inert unless SENTRY_DSN is set.
import * as Sentry from '@sentry/node';

const dsn = (process.env.SENTRY_DSN || '').trim();
if (dsn) {
  Sentry.init({
    dsn,
    environment: process.env.SENTRY_ENVIRONMENT || process.env.NODE_ENV || 'production',
    release: process.env.SENTRY_RELEASE || undefined,
    serverName: 'nodejs_gateway',
    tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE || '0'),
    sendDefaultPii: false,
  });
}
