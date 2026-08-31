# Vault Dynamic Credentials Evidence

## Database Secrets Engine Configuration
- Plugin: cassandra-database-plugin (built-in)
- Connection: cassandra-0.cassandra-headless.cassandra.svc.cluster.local:9042
- Role: fraud-detection-role (default_ttl=1h, max_ttl=24h)

## Dynamic Credential Generation Test
Generated credentials:
- username: v_root_fraud_detection_lmgi7dmtvbnibfeenwvi_1788208512
- lease_id: database/creds/fraud-detection-role/8Sezcz7yMqy08b3hOIPsCE0M
- lease_duration: 1h

Verified login to Cassandra with generated credentials - SUCCESS
Query returned data from fraud_detection.transactions_by_user table.

## Credential Rotation Proof
Lease: database/creds/fraud-detection-role/FJwkMd0bnqzi09khnK1iPTVW

Before renewal:
- expire_time: 2026-08-31T21:35:41Z
- ttl: 58m17s (approaching expiry)

After renewal (vault lease renew):
- lease_duration: 1h (reset to fresh full hour)
- No service restart required
- No new credentials generated - same lease extended

## Kubernetes Auth Integration
- Policy: fraud-detection-policy (read access to database/creds/fraud-detection-role)
- Role: fraud-detection-streams (bound to default SA in kafka namespace)
