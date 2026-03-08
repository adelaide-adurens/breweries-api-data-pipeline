# Email Setup for Pipeline Alerts

This guide explains how to configure email alerts so you receive notifications when DAG or task failures occur (e.g. data quality issues, timeouts, infrastructure problems).

## Overview

Airflow sends emails on task failure when:

1. SMTP is configured
2. `email_on_failure` is `True` (set in the DAG)
3. `email` (recipient list) is set via the `alert_email_to` Variable

## Step 1: Choose an SMTP Provider

Common options:

| Provider | SMTP Host | Port | Notes |
|----------|-----------|------|-------|
| Gmail | smtp.gmail.com | 587 | Requires [App Password](https://support.google.com/accounts/answer/185833) |
| SendGrid | smtp.sendgrid.net | 587 | API key as password |
| Outlook | smtp.office365.com | 587 | Use account password or app password |
| Mailgun | smtp.mailgun.org | 587 | Use SMTP credentials from dashboard |

## Step 2: Configure Environment Variables

Copy `.env.example` to `.env` and fill in your SMTP details:

```bash
cp .env.example .env
```

Edit `.env` and set:

```env
# SMTP (required for failure emails)
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@gmail.com
```

**Security**: Never commit `.env` to version control. It is listed in `.gitignore`.

## Step 3: Add Docker Compose SMTP Environment

Ensure your `docker-compose.yml` passes these variables to Airflow services. Add to the `x-airflow-common` environment section:

```yaml
environment:
  # ... existing vars ...
  AIRFLOW__SMTP__SMTP_HOST: ${AIRFLOW__SMTP__SMTP_HOST:-}
  AIRFLOW__SMTP__SMTP_PORT: ${AIRFLOW__SMTP__SMTP_PORT:-587}
  AIRFLOW__SMTP__SMTP_USER: ${AIRFLOW__SMTP__SMTP_USER:-}
  AIRFLOW__SMTP__SMTP_PASSWORD: ${AIRFLOW__SMTP__SMTP_PASSWORD:-}
  AIRFLOW__SMTP__SMTP_MAIL_FROM: ${AIRFLOW__SMTP__SMTP_MAIL_FROM:-}
```

Or use an `env_file` in docker-compose:

```yaml
env_file:
  - .env
```

## Step 4: Set Alert Recipients in Airflow

1. Open Airflow UI (http://localhost:8080)
2. Go to **Admin** → **Variables**
3. Add variable:
   - Key: `alert_email_to`
   - Value: `your_email@example.com` (or comma-separated: `a@x.com,b@y.com`)

## Step 5: Test Email Alerts

**Option A: Trigger a failing task**

1. Temporarily break a task (e.g. invalid path in the DAG)
2. Run the DAG manually
3. Wait for the task to fail
4. Check your inbox

**Option B: Use Airflow's test email**

```bash
docker compose run airflow-webserver airflow config get-value smtp smtp_mail_from
```

If SMTP is configured, a failing task will trigger an email automatically.

## Gmail-Specific: App Password

1. Enable 2-Step Verification on your Google account
2. Go to [Google App Passwords](https://myaccount.google.com/apppasswords)
3. Create an app password for "Mail"
4. Use this 16-character password as `AIRFLOW__SMTP__SMTP_PASSWORD`

## Troubleshooting

- **No emails received**: Check SMTP credentials, firewall, and that `alert_email_to` is set
- **Connection refused**: Ensure port 587 (or 465 for SSL) is not blocked
- **Authentication failed**: For Gmail, use an App Password, not your regular password
