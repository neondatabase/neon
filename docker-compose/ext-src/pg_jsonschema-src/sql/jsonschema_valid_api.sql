-- Define schema
SELECT jsonschema_is_valid('{
  "type": "object",
  "properties": {
    "username": { "type": "string" },
    "age": { "type": "integer" }
  },
  "required": ["username"]
}'::json);

-- Valid instance
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "username": { "type": "string" },
      "age": { "type": "integer" }
    },
    "required": ["username"]
  }'::json,
  '{"username": "alice", "age": 25}'::json
);

-- Invalid instance: missing required "username"
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "username": { "type": "string" },
      "age": { "type": "integer" }
    },
    "required": ["username"]
  }'::json,
  '{"age": 25}'::json
);

-- Invalid instance: wrong type for "age"
SELECT jsonschema_validation_errors(
  '{
    "type": "object",
    "properties": {
      "username": { "type": "string" },
      "age": { "type": "integer" }
    },
    "required": ["username"]
  }'::json,
  '{"username": "bob", "age": "twenty"}'::json
);
