-- The hadron_endpoint_safekeeper junction table stores the many-to-many relationship between
-- endpoints and safekeepers. This is essentially the timeline-to-safekeeper assignment table.

CREATE TABLE hadron_endpoint_safekeeper (
  endpoint_id UUID NOT NULL,
  sk_node_id BIGINT NOT NULL,
  PRIMARY KEY (endpoint_id, sk_node_id),
  FOREIGN KEY (endpoint_id) REFERENCES hadron_endpoints (endpoint_id) ON DELETE CASCADE,
  FOREIGN KEY (sk_node_id) REFERENCES hadron_safekeepers (sk_node_id) ON DELETE CASCADE
);