-- Schema with enums, nulls, extra properties disallowed
SELECT jsonschema_is_valid('{
  "type": "object",
  "properties": {
    "status": { "type": "string", "enum": ["active", "inactive", "pending"] },
    "email": { "type": ["string", "null"], "format": "email" }
  },
  "required": ["status"],
  "additionalProperties": false
}'::json);

-- Valid enum and null email
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "status": { "type": "string", "enum": ["active", "inactive", "pending"] },
      "email": { "type": ["string", "null"], "format": "email" }
    },
    "required": ["status"],
    "additionalProperties": false
  }'::json,
  '{"status": "active", "email": null}'::json
);

-- Invalid enum value
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "status": { "type": "string", "enum": ["active", "inactive", "pending"] },
      "email": { "type": ["string", "null"], "format": "email" }
    },
    "required": ["status"],
    "additionalProperties": false
  }'::json,
  '{"status": "disabled", "email": null}'::json
);

-- Invalid email format (assuming format is validated)
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "status": { "type": "string", "enum": ["active", "inactive", "pending"] },
      "email": { "type": ["string", "null"], "format": "email" }
    },
    "required": ["status"],
    "additionalProperties": false
  }'::json,
  '{"status": "active", "email": "not-an-email"}'::json
);

-- Extra property not allowed
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "status": { "type": "string", "enum": ["active", "inactive", "pending"] },
      "email": { "type": ["string", "null"], "format": "email" }
    },
    "required": ["status"],
    "additionalProperties": false
  }'::json,
  '{"status": "active", "extra": "should not be here"}'::json
);
